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
    gc::{
        jemalloc::debug_jemalloc,
        stats::{CollectorStatistics, CommitStatistics, OnMessageStatistics},
    },
    kv_store::{index_map::IndexMap, HashId},
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

type ChunkIndex = u32;

pub(crate) struct GCThread {
    free_ids: Producer<HashId>,
    recv: Receiver<Command>,
    pending: ChunkedVec<HashId, PENDING_CHUNK_CAPACITY>,
    debug: bool,

    objects_view: SharedIndexMapView<HashId, Option<Box<[u8]>>>,
    hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>>,

    global_counter: IndexMap<HashId, Option<u8>, 1_000_000>,
    counter: u8,

    nalives_per_chunk: IndexMap<ChunkIndex, u32, 1_000_000>,
    new_ids_in_commit: usize,
}

assert_eq_size!([u8; 16], Option<Box<[u8]>>);
assert_eq_size!([u8; 16], Option<Arc<[u8]>>);

pub enum Command {
    MarkNewIds {
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
    },
    Commit {
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
            nalives_per_chunk: IndexMap::default(),
            new_ids_in_commit: 0,
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
                    self.mark_new_ids(new_ids);
                    self.send_unused_on_empty_channel();
                }
                Ok(Command::Commit {
                    new_ids,
                    commit_hash_id,
                }) => self.handle_commit(new_ids, commit_hash_id),
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

    fn debug(&self, command: &Result<Command, RecvError>) {
        if !self.debug {
            return;
        }

        let stats = OnMessageStatistics {
            command,
            pending_command: self.recv.len(),
            pending_hash_ids_length: self.pending.len(),
            pending_hash_ids_capacity: self.pending.capacity(),
            objects_nchunks: self.objects_view.nchunks(),
            hashes_nchunks: self.hashes_view.nchunks(),
            counter_nchunks: self.global_counter.entries.list_of_chunks.len(),
        };

        log!("{:?}", stats);
    }

    /// This method send some `HashId` without checking if they are
    /// in a dead chunk or not.
    /// We have to do this when the channel `Self::free_ids` is empty
    /// or the main thread will allocate new chunks
    fn send_unused_on_empty_channel(&mut self) {
        if self.free_ids.len() >= 10_000 {
            // `Self::free_ids` is not empty, don't send anything
            return;
        }

        let pending = match self.pending.pop_first_chunk() {
            Some(pending) if !pending.is_empty() => pending,
            _ => return,
        };

        let send_at = pending.len().saturating_sub(10_000);

        log!(
            "GCThread::free_ids is empty, sending {:?} HashId",
            pending.len() - send_at
        );

        if let Err(e) = self.free_ids.push_slice(&pending[send_at..]) {
            elog!("GC: Fail to send free ids {:?}", e);
            self.pending.extend_from_slice(&pending);
        } else {
            self.pending.extend_from_slice(&pending[..send_at]);
        }
    }

    /// Notify the main thread that the ids are free to reused
    ///
    /// This prioritize HashId that are not in dead chunks, to avoid
    /// reallocating them.
    ///
    /// Returns the number of `HashId` sent
    fn send_unused(&mut self) -> usize {
        let sent = self.send_unused_impl();

        if sent > 0 {
            log!("GC_DEBUG SENT={:?}", sent);
        }

        GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
        sent
    }

    fn is_in_dead_chunk(&self, hash_id: HashId) -> bool {
        let chunk_index = self.hashes_view.chunk_index_of(hash_id).unwrap() as u32;
        self.nalives_per_chunk
            .get(chunk_index)
            .unwrap()
            .map(|n| *n == 0)
            .unwrap_or(true)
    }

    fn send_unused_impl(&mut self) -> usize {
        if self.free_ids.len() > 50_000 {
            return 0;
        }

        let navailable = self.free_ids.available();
        let npending = self.pending.len();

        if npending == 0 || navailable == 0 {
            return 0;
        }

        let nto_send = npending.min(navailable).min(50_000);
        let mut to_send = Vec::<HashId>::with_capacity(nto_send);
        let mut nchunk_pending = self.pending.nchunks();

        'outer: while let Some(chunk) = self.pending.pop_first_chunk() {
            for hash_id in chunk {
                if nchunk_pending > 0 && self.is_in_dead_chunk(hash_id) {
                    self.pending.push(hash_id);
                } else {
                    to_send.push(hash_id);
                }

                if nchunk_pending == 0 && to_send.len() > 20_000 {
                    // `nchunk_pending` is zero, it means we already sent all `HashId` in
                    // a non-dead chunk
                    break 'outer;
                }

                if to_send.len() == nto_send {
                    break 'outer;
                }
            }
            nchunk_pending = nchunk_pending.saturating_sub(1);
        }

        if let Err(e) = self.free_ids.push_slice(&to_send) {
            elog!("GC: Fail to send free ids {:?}", e);
            self.pending.extend_from_slice(&to_send);
            0
        } else {
            to_send.len()
        }
    }

    fn traverse_and_mark_tree_impl(
        &mut self,
        hash_id: HashId,
        counter: u8,
        nobjects: &mut usize,
        depth: usize,
        max_depth: &mut usize,
        objets_total_bytes: &mut usize,
    ) {
        *nobjects += 1;
        *max_depth = depth.max(*max_depth);

        let mut hash_ids = [None::<HashId>; 256];

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
            self.traverse_and_mark_tree_impl(
                hash_id,
                counter,
                nobjects,
                depth + 1,
                max_depth,
                objets_total_bytes,
            );
        }
    }

    fn traverse_and_mark_tree(
        &mut self,
        hash_id: HashId,
        counter: u8,
        nobjects: &mut usize,
        max_depth: &mut usize,
        objets_total_bytes: &mut usize,
    ) {
        self.traverse_and_mark_tree_impl(
            hash_id,
            counter,
            nobjects,
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

        for hash_id in unused.iter().copied() {
            let (_, is_obj_dealloc) = self.objects_view.clear(hash_id).unwrap();
            let (_, is_hash_dealloc) = self.hashes_view.clear(hash_id).unwrap();

            self.global_counter.insert_at(hash_id, None).unwrap();

            let chunk_index = self.hashes_view.chunk_index_of(hash_id).unwrap() as u32;
            let nalive = self.nalives_per_chunk.entry(chunk_index).unwrap();
            *nalive = nalive.checked_sub(1).unwrap();
        }

        unused
    }

    fn mark_new_ids(&mut self, new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>) {
        self.new_ids_in_commit += new_ids.len();

        for hash_id in new_ids.iter().copied() {
            self.global_counter
                .insert_at(hash_id, Some(self.counter))
                .unwrap();

            let chunk_index = self.hashes_view.chunk_index_of(hash_id).unwrap() as u32;
            let nalive = self.nalives_per_chunk.entry(chunk_index).unwrap();
            *nalive += 1;
        }
    }

    fn handle_commit(
        &mut self,
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
        commit_hash_id: HashId,
    ) {
        let now = std::time::Instant::now();

        self.mark_new_ids(new_ids);

        if self.debug {
            let stats = CommitStatistics {
                new_hash_id: self.new_ids_in_commit,
            };
            log!("{:?}", stats);
        }

        self.new_ids_in_commit = 0;

        if !self.recv.is_empty() {
            // There are still messages in the channel,
            // process them all first, before running the garbage collector
            self.send_unused_on_empty_channel();
            return;
        }

        self.send_unused();

        let mut nobjects = 0;
        let mut max_depth = 0;
        let mut object_total_bytes = 0;
        self.traverse_and_mark_tree(
            commit_hash_id,
            self.counter,
            &mut nobjects,
            &mut max_depth,
            &mut object_total_bytes,
        );

        let unused = self.take_unused();
        let unused_found = unused.len();

        self.pending.append_chunks(unused);
        self.counter = self.counter.wrapping_add(1);

        if self.debug {
            let gc_duration = now.elapsed();

            let now = std::time::Instant::now();
            let (objects_chunks_alive, objects_chunks_dead) = self.objects_view.alive_dead();
            let (hashes_chunks_alive, hashes_chunks_dead) = self.hashes_view.alive_dead();
            println!("Find alive and dead: {:?}", now.elapsed());

            let stats = CollectorStatistics {
                unused_found,
                nobjects,
                max_depth,
                object_total_bytes,
                gc_duration,
                objects_chunks_alive,
                objects_chunks_dead,
                hashes_chunks_alive,
                hashes_chunks_dead,
            };

            log!("{:?}", stats);

            debug_jemalloc();
        }
    }
}
