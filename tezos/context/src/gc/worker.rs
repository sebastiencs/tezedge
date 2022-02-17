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
    kv_store::{in_memory::debug_jemalloc, index_map::IndexMap, HashId},
    serialize::in_memory::iter_hash_ids,
    ObjectHash,
};

use tezos_spsc::Producer;

pub(crate) const PRESERVE_CYCLE_COUNT: usize = 2;
const PENDING_CHUNK_CAPACITY: usize = 100_000;
const UNUSED_CHUNK_CAPACITY: usize = 20_000;
pub const NEW_IDS_CHUNK_CAPACITY: usize = 20_000;

/// Used for statistics
///
/// Number of items in `GCThread::pending`.
pub(crate) static GC_PENDING_HASHIDS: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct GCThread {
    free_ids: Producer<HashId>,
    recv: Receiver<Command>,
    pending: ChunkedVec<HashId, PENDING_CHUNK_CAPACITY>,
    debug: bool,

    objects_view: SharedIndexMapView<HashId, Option<Box<[u8]>>>,
    hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>>,

    global_counter: IndexMap<HashId, Option<u8>, 1_000_000>,
    counter: u8,
}

assert_eq_size!([u8; 16], Option<Box<[u8]>>);
assert_eq_size!([u8; 16], Option<Arc<[u8]>>);

pub(crate) enum Command {
    MarkNewIds {
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
    },
    MarkReused {
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
        commit_hash_id: HashId,
    },
    NewChunks {
        objects_chunks: Option<Vec<SharedChunk<Option<Box<[u8]>>>>>,
        hashes_chunks: Option<Vec<SharedChunk<Option<Box<ObjectHash>>>>>,
    },
    Close,
}

impl GCThread {
    pub fn new(
        recv: Receiver<Command>,
        producer: Producer<HashId>,
        objects_view: SharedIndexMapView<HashId, Option<Box<[u8]>>>,
        hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>>,
    ) -> Self {
        Self {
            recv,
            free_ids: producer,
            pending: ChunkedVec::default(),
            debug: false,
            global_counter: IndexMap::default(),
            counter: 0,
            objects_view,
            hashes_view,
        }
    }
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
                    self.send_unused(None);
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
    /// Returns the number of `HashId` sent
    fn send_unused(
        &mut self,
        unused: impl Into<Option<ChunkedVec<HashId, { UNUSED_CHUNK_CAPACITY }>>>,
    ) -> usize {
        let unused: Option<ChunkedVec<HashId, { UNUSED_CHUNK_CAPACITY }>> = unused.into();
        let unused_is_none = unused.is_none();

        let sent = self.send_unused_impl(unused);

        if unused_is_none && sent > 0 {
            log!("GC_DEBUG SENT={:?}", sent);
        }

        GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
        sent
    }

    fn send_unused_impl(
        &mut self,
        unused: Option<ChunkedVec<HashId, { UNUSED_CHUNK_CAPACITY }>>,
    ) -> usize {
        if let Some(unused) = unused {
            self.pending.append_chunks(unused);
        };

        let mut sent = 0;

        loop {
            if self.pending.is_empty() {
                return sent;
            }

            let navailable = self.free_ids.available();

            if navailable < PENDING_CHUNK_CAPACITY {
                // We send chunk by chunk
                return sent;
            }

            let chunk = match self.pending.pop_first_chunk() {
                Some(chunk) => chunk,
                None => return sent,
            };

            if let Err(e) = self.free_ids.push_slice(&chunk) {
                elog!("GC: Fail to send free ids {:?}", e);
                self.pending.extend_from_slice(&chunk);
                return sent;
            } else {
                sent += chunk.len()
            }
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

    fn take_unused(&mut self) -> ChunkedVec<HashId, { UNUSED_CHUNK_CAPACITY }> {
        let unused_at = self.counter.wrapping_sub(3);

        let mut unused = ChunkedVec::default();

        for (hash_id, hash_id_counter) in self.global_counter.iter_with_keys() {
            if !matches!(hash_id_counter, Some(counter) if *counter == unused_at) {
                continue;
            }

            unused.push(hash_id);
        }

        let set = HashSet::<&HashId>::from_iter(unused.iter());
        assert_eq!(set.len(), unused.len());

        for hash_id in unused.iter() {
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

    fn mark_new_ids(&mut self, new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>) {
        for hash_id in new_ids.iter() {
            self.global_counter
                .insert_at(*hash_id, Some(self.counter))
                .unwrap();
        }
    }

    fn mark_reused(
        &mut self,
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
        commit_hash_id: HashId,
    ) {
        let now = std::time::Instant::now();

        self.mark_new_ids(new_ids);

        if !self.recv.is_empty() {
            // println!("DONT MARK");
            // self.send_unused(hashid_without_value);
            self.send_unused(None);
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
        let found_unused = unused.len();

        // self.send_unused(hashid_without_value);
        let nsent = self.send_unused(unused);

        let (alive, dead) = self.objects_view.alive_dead();
        let (h_alive, h_dead) = self.hashes_view.alive_dead();

        log!(
            "MARK_REUSED FOUND_UNUSED={:?} SENT={:?} TRAVERSED={:?} MAX_DEPTH={:?} OBJECT_TOTAL_BYTES={:?} TIME={:?} OBJ_LIST_ALIVE={:?} OBJ_LIST_DEAD={:?} HASH_LIST_ALIVE={:?} HASH_LIVE_DEAD={:?}",
            found_unused,
            nsent,
            traversed,
            max_depth,
            objets_total_bytes,
            now.elapsed(),
            alive,
            dead,
            h_alive,
            h_dead,
        );
        debug_jemalloc();

        self.counter = self.counter.wrapping_add(1);
    }
}
