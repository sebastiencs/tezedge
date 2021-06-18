use std::{
    borrow::Cow,
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, VecDeque},
    hash::Hasher,
    sync::Arc,
};

use crossbeam_channel::Sender;
use crypto::hash::ContextHash;

use crate::{
    gc::{
        worker::{Command, Cycles, GCThread, PRESERVE_CYCLE_COUNT},
        GarbageCollectionError, GarbageCollector,
    },
    hash::EntryHash,
    persistent::{DBError, Flushable, KeyValueStoreBackend, Persistable},
};

use tezos_spsc::Consumer;

use super::entries::Entries;
use super::{HashId, VacantEntryHash};

#[derive(Debug)]
pub struct HashValueStore {
    hashes: Entries<HashId, EntryHash>,
    values: Entries<HashId, Option<Arc<[u8]>>>,
    free_ids: Consumer<HashId>,
    new_ids: Vec<HashId>,
}

impl HashValueStore {
    fn new(consumer: Consumer<HashId>) -> Self {
        Self {
            hashes: Entries::new(),
            values: Entries::new(),
            free_ids: consumer,
            new_ids: Vec::with_capacity(1024),
        }
    }

    pub(crate) fn get_vacant_entry_hash(&mut self) -> VacantEntryHash {
        let (hash_id, entry) = if let Some(free_id) = self.free_ids.pop().ok() {
            self.values[free_id] = None;
            (free_id, &mut self.hashes[free_id])
        } else {
            self.hashes.get_vacant_entry()
        };
        self.new_ids.push(hash_id);

        VacantEntryHash {
            entry: Some(entry),
            hash_id,
        }
    }

    pub(crate) fn insert_value_at(&mut self, hash_id: HashId, value: Arc<[u8]>) {
        self.values.insert_at(hash_id, Some(value));
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Option<&EntryHash> {
        self.hashes.get(hash_id)
    }

    pub(crate) fn get_value(&self, hash_id: HashId) -> Option<&[u8]> {
        self.values.get(hash_id)?.as_ref().map(|v| v.as_ref())
    }

    pub(crate) fn contains(&self, hash_id: HashId) -> bool {
        self.values.get(hash_id).unwrap_or(&None).is_some()
    }

    fn take_new_ids(&mut self) -> Vec<HashId> {
        let new_ids = self.new_ids.clone();
        self.new_ids.clear();
        new_ids
    }
}

pub struct InMemory {
    current_cycle: BTreeMap<HashId, Option<Arc<[u8]>>>,
    pub hashes: HashValueStore,
    sender: Sender<Command>,
    pub context_hashes: HashMap<u64, HashId>,
    context_hashes_cycles: VecDeque<Vec<u64>>,
}

impl Default for InMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl GarbageCollector for InMemory {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        self.new_cycle_started();
        Ok(())
    }

    fn block_applied(
        &mut self,
        referenced_older_entries: Vec<HashId>,
    ) -> Result<(), GarbageCollectionError> {
        self.block_applied(referenced_older_entries);
        Ok(())
    }
}

impl Flushable for InMemory {
    fn flush(&self) -> Result<(), failure::Error> {
        Ok(())
    }
}

impl Persistable for InMemory {
    fn is_persistent(&self) -> bool {
        false
    }
}

impl KeyValueStoreBackend for InMemory {
    fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) -> Result<(), DBError> {
        self.write_batch(batch);
        Ok(())
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.contains(hash_id)
    }

    fn put_context_hash(&mut self, hash_id: HashId) -> Result<(), DBError> {
        Ok(self.put_context_hash_impl(hash_id))
    }

    fn get_context_hash(&self, context_hash: &ContextHash) -> Result<Option<HashId>, DBError> {
        Ok(self.get_context_hash_impl(context_hash))
    }

    fn get_hash(&self, hash_id: HashId) -> Result<Option<Cow<EntryHash>>, DBError> {
        Ok(self.get_hash(hash_id).map(|h| Cow::Borrowed(h)))
    }

    fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, DBError> {
        Ok(self.get_value(hash_id).map(|v| Cow::Borrowed(v)))
    }

    fn get_vacant_entry_hash(&mut self) -> Result<VacantEntryHash, DBError> {
        Ok(self.get_vacant_entry_hash())
    }
}

impl InMemory {
    pub fn new() -> Self {
        let (sender, recv) = crossbeam_channel::unbounded();
        let (prod, cons) = tezos_spsc::bounded(2_000_000);

        std::thread::spawn(move || {
            GCThread {
                cycles: Cycles::default(),
                recv,
                free_ids: prod,
                pending: Vec::new(),
            }
            .run()
        });

        let current_cycle = Default::default();
        let hashes = HashValueStore::new(cons);
        let context_hashes = Default::default();

        let mut context_hashes_cycles = VecDeque::with_capacity(PRESERVE_CYCLE_COUNT);
        for _ in 0..PRESERVE_CYCLE_COUNT {
            context_hashes_cycles.push_back(Default::default())
        }

        Self {
            current_cycle,
            hashes,
            sender,
            context_hashes,
            context_hashes_cycles,
        }
    }

    pub(crate) fn get_vacant_entry_hash(&mut self) -> VacantEntryHash {
        self.hashes.get_vacant_entry_hash()
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Option<&EntryHash> {
        self.hashes.get_hash(hash_id)
    }

    pub(crate) fn get_value(&self, hash_id: HashId) -> Option<&[u8]> {
        self.hashes.get_value(hash_id)
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        Ok(self.hashes.contains(hash_id))
    }

    pub fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) {
        for (hash_id, value) in batch {
            self.hashes.insert_value_at(hash_id, Arc::clone(&value));
            self.current_cycle.insert(hash_id, Some(value));
        }
    }

    pub fn new_cycle_started(&mut self) {
        let values_in_cycle = std::mem::take(&mut self.current_cycle);
        let new_ids = self.hashes.take_new_ids();

        self.sender
            .send(Command::StartNewCycle {
                values_in_cycle,
                new_ids,
            })
            .unwrap();

        let unused = self.context_hashes_cycles.pop_front().unwrap();
        for hash in unused {
            self.context_hashes.remove(&hash);
        }
        self.context_hashes_cycles.push_back(Default::default());
    }

    pub fn block_applied(&mut self, reused: Vec<HashId>) {
        self.sender.send(Command::MarkReused { reused }).unwrap()
    }

    pub fn get_context_hash_impl(&self, context_hash: &ContextHash) -> Option<HashId> {
        let mut hasher = DefaultHasher::new();
        hasher.write(context_hash.as_ref());
        let hashed = hasher.finish();

        self.context_hashes.get(&hashed).cloned()
    }

    pub fn put_context_hash_impl(&mut self, commit_hash_id: HashId) {
        let commit_hash = self.hashes.get_hash(commit_hash_id).unwrap();

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash[..]);
        let hashed = hasher.finish();

        self.context_hashes.insert(hashed, commit_hash_id);
        self.context_hashes_cycles.back_mut().unwrap().push(hashed);
    }

    #[cfg(test)]
    pub(crate) fn put_entry_hash(&mut self, entry_hash: EntryHash) -> HashId {
        let vacant = self.get_vacant_entry_hash();
        vacant.write_with(|entry| *entry = entry_hash)
    }
}
