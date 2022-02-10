// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crossbeam_channel::{Receiver, RecvError};

use crate::{
    chunks::ChunkedVec,
    kv_store::{index_map::IndexMap, HashId},
    serialize::in_memory::iter_hash_ids,
};

use tezos_spsc::Producer;

// use super::sorted_map::SortedMap;

pub(crate) const PRESERVE_CYCLE_COUNT: usize = 2;

/// Used for statistics
///
/// Number of items in `GCThread::pending`.
pub(crate) static GC_PENDING_HASHIDS: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct GCThread {
    pub(crate) cycles: Cycles,
    pub(crate) free_ids: Producer<HashId>,
    pub(crate) recv: Receiver<Command>,
    pub(crate) pending: Vec<HashId>,
    pub(crate) debug: bool,

    pub(crate) global: IndexMap<HashId, Option<Arc<[u8]>>>,
    pub(crate) global_counter: IndexMap<HashId, Option<u8>>,
    pub(crate) counter: u8,
}

pub(crate) enum Command {
    StartNewCycle {
        values_in_cycle: ChunkedVec<(HashId, Arc<[u8]>)>,
        new_ids: ChunkedVec<HashId>,
    },
    MarkReused {
        values_in_block: ChunkedVec<(HashId, Arc<[u8]>)>,
        new_ids: ChunkedVec<HashId>,
        reused: ChunkedVec<HashId>,
        commit_hash_id: HashId,
    },
    Close,
}

pub(crate) struct Cycles {
    list: VecDeque<HashMap<HashId, Arc<[u8]>>>,
}

impl Default for Cycles {
    fn default() -> Self {
        let mut list = VecDeque::with_capacity(PRESERVE_CYCLE_COUNT);

        for _ in 0..PRESERVE_CYCLE_COUNT {
            list.push_back(Default::default());
        }

        Self { list }
    }
}

impl Cycles {
    fn move_to_last_cycle(&mut self, hash_id: HashId) -> Option<Arc<[u8]>> {
        let mut value = None;

        for store in self.list.iter_mut().take(PRESERVE_CYCLE_COUNT - 1) {
            if let Some(item) = store.remove(&hash_id) {
                value = Some(item);
            };
        }

        let value = value?;

        if let Some(last_cycle) = self.list.back_mut() {
            last_cycle.insert(hash_id, Arc::clone(&value));
        } else {
            elog!("GC: Failed to insert value in Cycles")
        }

        Some(value)
    }

    fn roll(&mut self, new_cycle: HashMap<HashId, Arc<[u8]>>) -> Vec<HashId> {
        let unused = self.list.pop_front().unwrap_or_default();
        self.list.push_back(new_cycle);

        for store in self.list.iter_mut().take(PRESERVE_CYCLE_COUNT - 1) {
            store.shrink_to_fit();
        }

        unused.into_iter().map(|v| v.0).collect()

        // unused.keys_to_vec()
    }
}

impl GCThread {
    pub(crate) fn run(mut self) {
        // Enable debug logs when `TEZEDGE_GC_DEBUG` is present
        self.debug = std::env::var("TEZEDGE_GC_DEBUG").is_ok();

        loop {
            let msg = self.recv.recv();

            self.debug(&msg);

            match msg {
                Ok(Command::StartNewCycle {
                    values_in_cycle,
                    new_ids,
                }) => {
                    // self.start_new_cycle(values_in_cycle, new_ids)
                }
                Ok(Command::MarkReused {
                    reused,
                    values_in_block,
                    new_ids,
                    commit_hash_id,
                }) => self.mark_reused(reused, values_in_block, new_ids, commit_hash_id),
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

    fn debug(&self, msg: &Result<Command, RecvError>) {
        if !self.debug {
            return;
        }

        let msg = match msg {
            Ok(Command::StartNewCycle {
                values_in_cycle,
                new_ids,
            }) => format!(
                "START_NEW_CYCLE VALUES_IN_CYCLE={:?} NEW_IDS={:?}",
                values_in_cycle.len(),
                new_ids.len()
            ),
            Ok(Command::MarkReused { reused, .. }) => format!("REUSED {:?}", reused.len()),
            Ok(Command::Close { .. }) => "CLOSE".to_owned(),
            Err(_) => "ERR".to_owned(),
        };

        log!(
            "CYCLES_LENGTH={:?} NMSG={:?} MSG={:?}",
            self.cycles.list.len(),
            self.recv.len(),
            msg
        );

        let mut total = 0;

        // for (index, c) in self.cycles.list.iter().enumerate() {
        //     log!("CYCLE[{:?}]_LENGTH={:?}", index, c.len());
        //     total += c.len();
        // }
        log!(
            "PENDING={:?} TOTAL_IN_CYCLES={:?}",
            self.pending.len(),
            total
        );
    }

    fn start_new_cycle(
        &mut self,
        mut new_cycle: ChunkedVec<(HashId, Arc<[u8]>)>,
        new_ids: ChunkedVec<HashId>,
    ) {
        GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);

        // // Gather `HashId` created before a commit.
        // // We send them back to the main thread, they can be reused
        // let mut hashid_without_value = Vec::with_capacity(1024);

        // let new_cycle = new_cycle.into_hash_map();

        // for hash_id in new_ids.iter() {
        //     if !new_cycle.contains_key(hash_id) {
        //         hashid_without_value.push(*hash_id);
        //     }
        // }

        // self.send_unused(hashid_without_value);

        // if self.debug {
        //     log!("GC_WORKER: START_NEW_CYCLE NEW_CYCLE={:?}", new_cycle.len(),);
        // }

        // let unused = self.cycles.roll(new_cycle);
        // self.send_unused(unused);
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

    fn send_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let navailable = self.free_ids.available();
        if navailable == 0 {
            return;
        }

        let n_to_send = navailable.min(self.pending.len());
        let start = self.pending.len() - n_to_send;
        let to_send = &self.pending[start..];

        if let Err(e) = self.free_ids.push_slice(to_send) {
            elog!("GC: Fail to send free ids {:?}", e);
            return;
        }

        self.pending.truncate(start);
        GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);
    }

    fn traverse_mark_impl(
        &self,
        global_counter: &mut IndexMap<HashId, Option<u8>>,
        hash_id: HashId,
        counter: u8,
        traversed: &mut usize
    ) {
        *traversed += 1;

        let value = {
            let value = self.global.get(hash_id).unwrap().unwrap().as_ref().unwrap();
            value
        };

        {
            let value_counter = global_counter
                .get_mut(hash_id)
                .unwrap()
                .unwrap()
                .as_mut()
                .unwrap();
            *value_counter = counter;
        }

        for hash_id in iter_hash_ids(&value) {
            self.traverse_mark_impl(global_counter, hash_id, counter, traversed);
        }
    }

    fn traverse_mark(&mut self, hash_id: HashId, counter: u8, traversed: &mut usize) {
        let mut global_counter = std::mem::replace(&mut self.global_counter, IndexMap::empty());
        self.traverse_mark_impl(&mut global_counter, hash_id, counter, traversed);
        self.global_counter = global_counter;
    }

    fn take_unused(&mut self) -> Vec<HashId> {
        let current_counter = self.counter;

        let mut unused = Vec::with_capacity(2048);

        for (hash_id, value) in self.global_counter.iter_with_keys() {
            let value = match value {
                Some(value) => value,
                None => continue,
            };

            if *value == current_counter.wrapping_sub(10) {
                self.global.insert_at(hash_id, None).unwrap();
                unused.push(hash_id);
            }
        }

        for hash_id in &unused {
            self.global_counter.insert_at(*hash_id, None).unwrap();
        }

        unused
    }

    fn mark_reused(
        &mut self,
        mut reused: ChunkedVec<HashId>,
        mut values_in_blocks: ChunkedVec<(HashId, Arc<[u8]>)>,
        new_ids: ChunkedVec<HashId>,
        commit_hash_id: HashId,
    ) {
        let now = std::time::Instant::now();

        while let Some(chunk) = values_in_blocks.pop_first_chunk() {
            for (hash_id, value) in chunk.into_iter() {
                self.global.insert_at(hash_id, Some(value)).unwrap();
                self.global_counter
                    .insert_at(hash_id, Some(self.counter))
                    .unwrap();
            }
        }

        // Gather `HashId` created before a commit.
        // We send them back to the main thread, they can be reused
        let mut hashid_without_value = Vec::with_capacity(1024);

        for hash_id in new_ids.iter() {
            if self
                .global
                .get(*hash_id)
                .map(|v| v.is_none())
                .unwrap_or(true)
            {
                hashid_without_value.push(*hash_id);
            }
        }

        let mut traversed = 0;
        self.traverse_mark(commit_hash_id, self.counter, &mut traversed);

        let unused = self.take_unused();

        let mut sent = hashid_without_value.len();
        sent += unused.len();

        self.send_unused(hashid_without_value);
        self.send_unused(unused);

        log!(
            "MARK_REUSED SENT={:?} TRAVERSED={:?} TIME={:?}",
            sent,
            traversed,
            now.elapsed(),
        );

        self.counter = self.counter.wrapping_add(1);

        // GC_PENDING_HASHIDS.store(self.pending.len(), Ordering::Release);

        // let param = reused.len();
        // let mut none = 0;
        // let mut moved = 0;
        // let mut last_cycle = 0;
        // let mut total = 0;

        // while let Some(hash_id) = reused.pop() {
        //     total += 1;

        //     let value = match self.cycles.move_to_last_cycle(hash_id) {
        //         Some(v) => {
        //             moved += 1;
        //             v
        //         }
        //         None => {
        //             let last = self.cycles.list.back().unwrap();
        //             match last.get(&hash_id).cloned() {
        //                 Some(v) => {
        //                     last_cycle += 1;
        //                     v
        //                 }
        //                 None => {
        //                     none += 1;
        //                     continue;
        //                 }
        //             }
        //         }
        //     };

        //     for hash_id in iter_hash_ids(&value) {
        //         reused.push(hash_id);
        //     }
        // }

        // if self.debug {
        //     log!(
        //         "MARK_REUSED PARAM={:?} TOTAL={:?} MOVED={:?} LAST_CYCLE={:?} NONE_VALUES={:?} {:?}",
        //         param,
        //         total,
        //         moved,
        //         last_cycle,
        //         none,
        //         now.elapsed(),
        //     );
        // }

        // self.send_pending();

        // for store in self.cycles.list.iter_mut() {
        //     store.shrink_to_fit();
        // }
    }
}
