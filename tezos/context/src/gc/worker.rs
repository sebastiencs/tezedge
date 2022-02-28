// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    collections::BTreeMap,
    num::TryFromIntError,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use crossbeam_channel::{Receiver, RecvError};
use static_assertions::assert_eq_size;
use thiserror::Error;

use crate::{
    chunks::{ChunkedVec, SharedChunk, SharedIndexMapView},
    gc::{
        jemalloc::debug_jemalloc,
        stats::{CollectorStatistics, CommitStatistics, OnMessageStatistics},
    },
    kv_store::{
        in_memory::OBJECTS_CHUNK_CAPACITY, index_map::IndexMap,
        inline_boxed_slice::InlinedBoxedSlice, HashId, HashIdError,
    },
    serialize::in_memory::iter_hash_ids,
    ObjectHash,
};

use tezos_spsc::Producer;

pub(crate) const PRESERVE_CYCLE_COUNT: usize = 2;

const PENDING_CHUNK_CAPACITY: usize = 100_000;
const UNUSED_CHUNK_CAPACITY: usize = 20_000;
pub const NEW_IDS_CHUNK_CAPACITY: usize = 20_000;

const MAX_SEND_TO_MAIN_THREAD: usize = 200_000;
const LIMIT_ON_EMPTY_CHANNEL: usize = 20_000;

const NCOMMITS_BEFORE_COLLECT: usize = 100;

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
    new_ids_in_commit: usize,
    last_gc_timestamp: Option<Instant>,

    objects_view: SharedIndexMapView<HashId, Option<InlinedBoxedSlice>, OBJECTS_CHUNK_CAPACITY>,
    hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>, OBJECTS_CHUNK_CAPACITY>,

    objects_generation: IndexMap<HashId, Option<u8>, 1_000_000>,
    current_generation: u8,

    nalives_per_chunk: IndexMap<ChunkIndex, u32, 1_000_000>,

    commits_since_last_run: usize,
}

assert_eq_size!([u8; 16], Option<Box<[u8]>>);
assert_eq_size!([u8; 8], Option<Box<ObjectHash>>);

#[derive(Debug, Error)]
enum GCError {
    #[error("HashId conversion failed")]
    HashIdFailed,
    #[error("Failed to traverse tree")]
    TraverseTreeError,
    #[error("Alive counter is in an invalid state")]
    InvalidAliveState,
    #[error("Int conversion failed")]
    FromIntFailed {
        #[from]
        e: TryFromIntError,
    },
}

impl From<HashIdError> for GCError {
    fn from(_: HashIdError) -> Self {
        GCError::HashIdFailed
    }
}

pub enum Command {
    MarkNewIds {
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
    },
    Commit {
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
        commit_hash_id: HashId,
        cycle_position: u64,
    },
    NewChunks {
        objects_chunks: Option<Vec<SharedChunk<Option<InlinedBoxedSlice>, OBJECTS_CHUNK_CAPACITY>>>,
        hashes_chunks: Option<Vec<SharedChunk<Option<Box<ObjectHash>>, OBJECTS_CHUNK_CAPACITY>>>,
    },
    Close,
}

impl GCThread {
    pub fn new(
        recv: Receiver<Command>,
        producer: Producer<HashId>,
        objects_view: SharedIndexMapView<HashId, Option<InlinedBoxedSlice>, OBJECTS_CHUNK_CAPACITY>,
        hashes_view: SharedIndexMapView<HashId, Option<Box<ObjectHash>>, OBJECTS_CHUNK_CAPACITY>,
    ) -> Self {
        Self {
            recv,
            free_ids: producer,
            pending: ChunkedVec::default(),
            debug: false,
            objects_generation: IndexMap::default(),
            current_generation: 0,
            objects_view,
            hashes_view,
            nalives_per_chunk: IndexMap::default(),
            new_ids_in_commit: 0,
            last_gc_timestamp: None,
            commits_since_last_run: 0,
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
                    if let Err(e) = self.mark_new_ids(new_ids) {
                        elog!("mark_new_ids failed: {:?}", e);
                    }
                    self.send_unused_on_empty_channel();
                }
                Ok(Command::Commit {
                    new_ids,
                    commit_hash_id,
                    cycle_position,
                }) => {
                    if let Err(e) = self.handle_commit(new_ids, cycle_position, commit_hash_id) {
                        elog!("handle_commit failed: {:?}", e);
                    }
                }
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
        objects_chunks: Option<Vec<SharedChunk<Option<InlinedBoxedSlice>, OBJECTS_CHUNK_CAPACITY>>>,
        hashes_chunks: Option<Vec<SharedChunk<Option<Box<ObjectHash>>, OBJECTS_CHUNK_CAPACITY>>>,
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
            counter_nchunks: self.objects_generation.entries.list_of_chunks.len(),
        };

        log!("{:?}", stats);
    }

    /// This method send some `HashId` without checking if they are
    /// in a dead chunk or not.
    /// We have to do this when the channel `Self::free_ids` is empty
    /// or the main thread will allocate new chunks
    fn send_unused_on_empty_channel(&mut self) {
        if self.free_ids.len() >= LIMIT_ON_EMPTY_CHANNEL {
            // `Self::free_ids` is not empty, don't send anything
            return;
        }

        let pending = match self.pending.pop_first_chunk() {
            Some(pending) if !pending.is_empty() => pending,
            _ => return,
        };

        let send_at = pending.len().saturating_sub(LIMIT_ON_EMPTY_CHANNEL);

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
    fn send_unused(&mut self) -> Result<usize, GCError> {
        let sent = self.send_unused_impl()?;

        GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
        Ok(sent)
    }

    fn is_in_dead_chunk(&self, hash_id: HashId) -> Result<bool, GCError> {
        let chunk_index = self.hashes_view.chunk_index_of(hash_id)? as u32;
        let is_chunk_dead = self
            .nalives_per_chunk
            .get(chunk_index)?
            .map(|n| *n == 0)
            .unwrap_or(true);

        Ok(is_chunk_dead)
    }

    fn send_unused_impl(&mut self) -> Result<usize, GCError> {
        if self.free_ids.len() > MAX_SEND_TO_MAIN_THREAD {
            return Ok(0);
        }

        let navailable = self.free_ids.available();
        let npending = self.pending.len();

        if npending == 0 || navailable == 0 {
            return Ok(0);
        }

        let nto_send = npending.min(navailable).min(MAX_SEND_TO_MAIN_THREAD);
        let mut to_send = Vec::<HashId>::with_capacity(nto_send);
        let mut nchunk_pending = self.pending.nchunks();

        'outer: while let Some(chunk) = self.pending.pop_first_chunk() {
            for hash_id in chunk {
                if nchunk_pending > 0 && self.is_in_dead_chunk(hash_id)? {
                    self.pending.push(hash_id);
                } else {
                    to_send.push(hash_id);
                }

                if nchunk_pending == 0 && to_send.len() > 50_000 {
                    // `nchunk_pending` is zero, it means we already sent all `HashId` in
                    // non-dead chunks
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
            Ok(0)
        } else {
            Ok(to_send.len())
        }
    }

    fn traverse_and_mark_tree(
        &mut self,
        hash_id: HashId,
        counter: u8,
        nobjects: &mut usize,
        depth: usize,
        max_depth: &mut usize,
        objets_total_bytes: &mut usize,
        map_length: &mut BTreeMap<usize, usize>,
    ) -> Result<(), GCError> {
        *nobjects += 1;
        *max_depth = depth.max(*max_depth);

        let mut hash_ids = [None::<HashId>; 256];

        self.objects_view.with(hash_id, |object_bytes| {
            let object_bytes = match object_bytes {
                Some(Some(object_bytes)) => object_bytes,
                _ => {
                    elog!("Missing Object object={:?}", object_bytes);
                    return Err(GCError::TraverseTreeError);
                }
            };

            *objets_total_bytes += object_bytes.len();

            let e = map_length.entry(object_bytes.len()).or_default();
            *e += 1;

            for (index, hash_id) in iter_hash_ids(object_bytes).enumerate() {
                hash_ids[index] = Some(hash_id);
            }

            Ok(())
        })??;

        {
            let value_counter = self.objects_generation.entry(hash_id)?;
            *value_counter = Some(counter);
        }

        for hash_id in hash_ids {
            let hash_id = match hash_id {
                Some(hash_id) => hash_id,
                None => break,
            };
            self.traverse_and_mark_tree(
                hash_id,
                counter,
                nobjects,
                depth + 1,
                max_depth,
                objets_total_bytes,
                map_length,
            )?;
        }

        Ok(())
    }

    fn take_unused(&mut self) -> Result<ChunkedVec<HashId, { UNUSED_CHUNK_CAPACITY }>, GCError> {
        // Mark all objects/hashes at `current_generation - 4` as unused/reusable

        let unused_at = self.current_generation.wrapping_sub(4);

        let mut unused = ChunkedVec::default();

        for (hash_id, generation) in self.objects_generation.iter_with_keys() {
            if !matches!(generation, Some(generation) if *generation == unused_at) {
                continue;
            }

            unused.push(hash_id);
        }

        for hash_id in unused.iter().copied() {
            let (_, _is_obj_dealloc) = self.objects_view.clear(hash_id)?;
            let (_, _is_hash_dealloc) = self.hashes_view.clear(hash_id)?;

            self.objects_generation.insert_at(hash_id, None)?;

            let chunk_index = self.hashes_view.chunk_index_of(hash_id)? as u32;
            let nalive = self.nalives_per_chunk.entry(chunk_index)?;
            *nalive = nalive.checked_sub(1).ok_or(GCError::InvalidAliveState)?;
        }

        Ok(unused)
    }

    fn mark_new_ids(
        &mut self,
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
    ) -> Result<(), GCError> {
        self.new_ids_in_commit += new_ids.len();

        for hash_id in new_ids.iter().copied() {
            self.objects_generation
                .insert_at(hash_id, Some(self.current_generation))?;

            let chunk_index = self.hashes_view.chunk_index_of(hash_id)? as u32;
            let nalive = self.nalives_per_chunk.entry(chunk_index)?;
            *nalive += 1;
        }

        Ok(())
    }

    fn handle_commit(
        &mut self,
        new_ids: ChunkedVec<HashId, NEW_IDS_CHUNK_CAPACITY>,
        cycle_position: u64,
        commit_hash_id: HashId,
    ) -> Result<(), GCError> {
        let now = std::time::Instant::now();

        self.commits_since_last_run += 1;
        self.mark_new_ids(new_ids)?;

        if self.debug {
            let stats = CommitStatistics {
                new_hash_id: self.new_ids_in_commit,
            };
            log!("{:?}", stats);
        }

        self.new_ids_in_commit = 0;
        self.current_generation = (cycle_position & 0xFF) as u8;

        if !self.recv.is_empty() || self.commits_since_last_run < NCOMMITS_BEFORE_COLLECT {
            // There are still messages in the channel,
            // process them all first, before running the garbage collector
            self.send_unused_on_empty_channel();
            return Ok(());
        }
        self.commits_since_last_run = 0;

        self.send_unused()?;

        let mut map_length = BTreeMap::<usize, usize>::default();

        let mut nobjects = 0;
        let mut max_depth = 0;
        let mut object_total_bytes = 0;
        self.traverse_and_mark_tree(
            commit_hash_id,
            self.current_generation,
            &mut nobjects,
            1,
            &mut max_depth,
            &mut object_total_bytes,
            &mut map_length,
        )?;

        // println!(
        //     "MAP_LENGTH LENGTH={:?} MAP={:#?}",
        //     map_length.len(),
        //     map_length
        // );

        let unused = self.take_unused()?;
        let unused_found = unused.len();

        self.pending.append_chunks(unused);

        if self.debug {
            let gc_duration = now.elapsed();
            let delay_since_last_gc = self.last_gc_timestamp.map(|t| t.elapsed());
            self.last_gc_timestamp.replace(std::time::Instant::now());

            let (objects_chunks_alive, objects_chunks_dead) =
                self.objects_view.count_alives_and_deads();
            let (hashes_chunks_alive, hashes_chunks_dead) =
                self.hashes_view.count_alives_and_deads();

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
                delay_since_last_gc,
            };

            log!("{:?}", stats);

            debug_jemalloc();
        }

        Ok(())
    }
}
