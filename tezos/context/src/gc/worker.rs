// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    collections::HashSet,
    iter::FromIterator,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crossbeam_channel::{Receiver, RecvError};
use static_assertions::assert_eq_size;

use crate::{
    chunks::{ChunkedVec, SharedChunk, SharedIndexMapView},
    kv_store::{index_map::IndexMap, HashId},
    serialize::in_memory::iter_hash_ids,
    ObjectHash,
};

use tezos_spsc::Producer;

use super::SortedMap;

pub(crate) const PRESERVE_CYCLE_COUNT: usize = 2;

/// Used for statistics
///
/// Number of items in `GCThread::pending`.
pub(crate) static GC_PENDING_HASHIDS: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct GCThread {
    // pub(crate) cycles: Cycles,
    pub(crate) free_ids: Producer<HashId>,
    pub(crate) recv: Receiver<Command>,
    pub(crate) pending: Vec<HashId>,
    pub(crate) debug: bool,

    pub(crate) objects_view: SharedIndexMapView<HashId, Option<Box<[u8]>>>,
    pub(crate) hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>>,

    pub(crate) global_counter: IndexMap<HashId, Option<u8>>,
    pub(crate) counter: u8,
}

assert_eq_size!([u8; 16], Option<Box<[u8]>>);
assert_eq_size!([u8; 16], Option<Arc<[u8]>>);

pub(crate) enum Command {
    MarkNewIds {
        new_ids: ChunkedVec<HashId>,
    },
    MarkReused {
        new_ids: ChunkedVec<HashId>,
        commit_hash_id: HashId,
    },
    NewChunks {
        objects_chunks: Option<Vec<SharedChunk<Option<Box<[u8]>>>>>,
        hashes_chunks: Option<Vec<SharedChunk<Option<Box<ObjectHash>>>>>,
    },
    Close,
}

impl GCThread {
    pub(crate) fn run(mut self) {
        // Enable debug logs when `TEZEDGE_GC_DEBUG` is present
        self.debug = std::env::var("TEZEDGE_GC_DEBUG").is_ok();

        loop {
            let msg = self.recv.recv();

            self.debug(&msg);

            match msg {
                Ok(Command::NewChunks {
                    objects_chunks,
                    hashes_chunks,
                }) => {
                    self.add_chunks(objects_chunks, hashes_chunks);
                }
                Ok(Command::MarkNewIds { new_ids }) => {
                    self.mark_new_ids(new_ids);
                }
                Ok(Command::MarkReused {
                    // reused,
                    // values_in_block,
                    new_ids,
                    commit_hash_id,
                }) => self.mark_reused(new_ids, commit_hash_id),
                // }) => self.mark_reused(reused, values_in_block, new_ids, commit_hash_id),
                Ok(Command::Close) => {
                    elog!("GC received Command::Close");
                    break;
                }
                Err(e) => {
                    elog!("GC channel is closed {:?}", e);
                    break;
                }
            }
        }
        elog!("GC exited");
    }

    fn add_chunks(
        &mut self,
        objects_chunks: Option<Vec<SharedChunk<Option<Box<[u8]>>>>>,
        hashes_chunks: Option<Vec<SharedChunk<Option<Box<ObjectHash>>>>>,
    ) {
        if let Some(objects_chunks) = objects_chunks {
            self.objects_view.append_chunks(objects_chunks);
        };
        if let Some(hashes_chunks) = hashes_chunks {
            self.hashes_view.append_chunks(hashes_chunks);
        };
    }

    fn debug(&self, msg: &Result<Command, RecvError>) {
        if !self.debug {
            return;
        }

        let msg = match msg {
            Ok(Command::MarkReused {
                new_ids,
                commit_hash_id,
            }) => {
                format!(
                    "REUSED NEW_IDS={:?} COMMIT_HASH_ID={:?}",
                    new_ids.len(),
                    commit_hash_id
                )
            }
            Ok(Command::NewChunks {
                objects_chunks,
                hashes_chunks,
            }) => {
                format!(
                    "NEW_CHUNKS OBJECTS={:?} HASHES={:?}",
                    objects_chunks.as_ref().map(|c| c.len()).unwrap_or(0),
                    hashes_chunks.as_ref().map(|c| c.len()).unwrap_or(0),
                )
            }
            Ok(Command::MarkNewIds { new_ids }) => format!("NEW_IDS {:?}", new_ids.len()),
            Ok(Command::Close { .. }) => "CLOSE".to_owned(),
            Err(_) => "ERR".to_owned(),
        };

        log!(
            // "GC_DEBUG NMSG={:?} MSG={:?} PENDING={:?} GLOBAL_LEN={:?} GLOBAL_CAP={:?}",
            "GC_DEBUG NMSG={:?} MSG={:?} PENDING={:?} PENDING_CAP={:?} OBJECT_LIST={:?} HASH_LIST={:?} COUNTER_LIST={:?}",
            self.recv.len(),
            msg,
            self.pending.len(),
            self.pending.capacity(),
            // self.values_map.len(),
            self.objects_view.nchunks(),
            self.hashes_view.nchunks(),
            self.global_counter.entries.list_of_chunks.len(),
            // self.global.len(),
            // self.global.capacity(),
        );
    }

    /// Notify the main thread that the ids are free to reused
    fn send_unused(&mut self, unused: Vec<HashId>) {
        let unused_length = unused.len();
        let navailable = self.free_ids.available();

        let (to_send, pending) = if navailable < unused_length {
            unused.split_at(navailable)
        } else {
            (&unused[..], &[][..])
        };

        if let Err(e) = self.free_ids.push_slice(to_send) {
            elog!("GC: Fail to send free ids {:?}", e);
            self.pending.extend_from_slice(&unused);
            GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
            return;
        }

        if !pending.is_empty() {
            self.pending.extend_from_slice(pending);
            GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
        }
    }

    fn traverse_mark_impl(
        &mut self,
        hash_id: HashId,
        counter: u8,
        traversed: &mut usize,
        depth: usize,
        max_depth: &mut usize,
        objets_total_bytes: &mut usize,
    ) {
        *traversed += 1;
        *max_depth = depth.max(*max_depth);

        let mut hash_ids: [Option<HashId>; 256] = [None; 256];

        self.objects_view
            .with(hash_id, |object_bytes| {
                let object_bytes = match object_bytes {
                    Some(Some(object_bytes)) => object_bytes,
                    _ => {
                        let chunk = self.objects_view.chunk_index_of(hash_id).unwrap();
                        panic!("Missing Object {:?} chunk={:?}", hash_id, chunk);
                    }
                };

                *objets_total_bytes += object_bytes.len();

                for (index, hash_id) in iter_hash_ids(object_bytes).enumerate() {
                    hash_ids[index] = Some(hash_id);
                }
            })
            .unwrap();

        {
            let value_counter = self
                .global_counter
                .get_mut(hash_id)
                .unwrap()
                .unwrap()
                .as_mut()
                .unwrap();
            *value_counter = counter;
        }

        for hash_id in hash_ids {
            let hash_id = match hash_id {
                Some(hash_id) => hash_id,
                None => break,
            };
            self.traverse_mark_impl(
                hash_id,
                counter,
                traversed,
                depth + 1,
                max_depth,
                objets_total_bytes,
            );
        }
    }

    fn traverse_mark(
        &mut self,
        hash_id: HashId,
        counter: u8,
        traversed: &mut usize,
        max_depth: &mut usize,
        objets_total_bytes: &mut usize,
    ) {
        self.traverse_mark_impl(
            hash_id,
            counter,
            traversed,
            1,
            max_depth,
            objets_total_bytes,
        );
    }

    fn take_unused(&mut self) -> Vec<HashId> {
        let unused_at = self.counter.wrapping_sub(3);

        let mut unused = Vec::with_capacity(2048);

        for (hash_id, hash_id_counter) in self.global_counter.iter_with_keys() {
            if !matches!(hash_id_counter, Some(counter) if *counter == unused_at) {
                continue;
            }

            unused.push(hash_id);
        }

        let set = HashSet::<&HashId>::from_iter(unused.iter());
        assert_eq!(set.len(), unused.len());

        for hash_id in &unused {
            let (_, obj_dealloc) = self.objects_view.clear(*hash_id).unwrap();
            let hash_dealloc = self.hashes_view.clear(*hash_id).unwrap();

            // let chunk = self.hashes_view.chunk_index_of(*hash_id);

            // assert_eq!(obj_dealloc, hash_dealloc);

            // if obj_dealloc {
            //     println!("DEALLOCATED CHUNK({:?})", chunk);
            // }

            self.global_counter.insert_at(*hash_id, None).unwrap();
        }

        // println!("CLEARED {:?}", unused);

        unused
    }

    fn mark_new_ids(&mut self, new_ids: ChunkedVec<HashId>) {
        for hash_id in new_ids.iter() {
            self.global_counter
                .insert_at(*hash_id, Some(self.counter))
                .unwrap();
        }
    }

    fn mark_reused(&mut self, new_ids: ChunkedVec<HashId>, commit_hash_id: HashId) {
        let now = std::time::Instant::now();

        self.mark_new_ids(new_ids);

        if !self.recv.is_empty() {
            // println!("DONT MARK");
            // self.send_unused(hashid_without_value);
            return;
        }

        let mut traversed = 0;
        let mut max_depth = 0;
        let mut objets_total_bytes = 0;
        self.traverse_mark(
            commit_hash_id,
            self.counter,
            &mut traversed,
            &mut max_depth,
            &mut objets_total_bytes,
        );

        let unused = self.take_unused();

        // let mut sent = hashid_without_value.len();
        let sent = unused.len();

        // self.send_unused(hashid_without_value);
        self.send_unused(unused);

        let (alive, dead) = self.objects_view.alive_dead();
        let (h_alive, h_dead) = self.hashes_view.alive_dead();

        log!(
            "MARK_REUSED SENT={:?} TRAVERSED={:?} MAX_DEPTH={:?} OBJECT_TOTAL_BYTES={:?} TIME={:?} OBJ_LIST_ALIVE={:?} OBJ_LIST_DEAD={:?} HASH_LIST_ALIVE={:?} HASH_LIVE_DEAD={:?}",
            sent,
            traversed,
            max_depth,
            objets_total_bytes,
            now.elapsed(),
            alive,
            dead,
            h_alive,
            h_dead,
        );

        self.counter = self.counter.wrapping_add(1);
    }
}
