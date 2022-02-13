// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Implementation of an in-memory repository.

use std::{
    borrow::Cow,
    collections::{hash_map::DefaultHasher, VecDeque},
    convert::TryFrom,
    hash::Hasher,
    mem::size_of,
    rc::Rc,
    sync::{atomic::Ordering, Arc, RwLock},
    thread::JoinHandle,
};

#[cfg(test)]
use crate::serialize::persistent::AbsoluteOffset;

use crossbeam_channel::Sender;
use crypto::hash::ContextHash;
use tezos_timing::{RepositoryMemoryUsage, SerializeStats};

use crate::{
    chunks::{ChunkedVec, SharedIndexMap},
    gc::{
        worker::{Command, GCThread, GC_PENDING_HASHIDS, PRESERVE_CYCLE_COUNT},
        GarbageCollectionError, GarbageCollector,
    },
    hash::ObjectHash,
    persistent::{
        DBError, Flushable, KeyValueStoreBackend, Persistable, ReadStatistics, ReloadError,
    },
    working_tree::{
        shape::{DirectoryShapeId, DirectoryShapes, ShapeStrings},
        storage::{DirEntryId, DirectoryOrInodeId, Storage},
        string_interner::{StringId, StringInterner},
        working_tree::{PostCommitData, WorkingTree},
        Commit, Object, ObjectReference,
    },
    ContextKeyValueStore, IndexApi, Map, Persistent, TezedgeIndex,
};
use crate::{persistent::get_commit_hash, serialize::in_memory};

use tezos_spsc::Consumer;

use super::{index_map::IndexMap, persistent::PersistentConfiguration, HashIdError};
use super::{HashId, VacantObjectHash};

#[derive(Debug)]
pub struct HashValueStore {
    hashes: SharedIndexMap<HashId, Option<Box<ObjectHash>>>,
    values: SharedIndexMap<HashId, Option<Box<[u8]>>>,
    free_ids: Option<Consumer<HashId>>,
    new_ids: ChunkedVec<HashId>,
    values_bytes: usize,
}

pub const VALUES_LENGTH: usize = 1_000;
pub const NEW_IDS_LIMIT: usize = 1000;
// pub const VALUES_LENGTH: usize = 100_000;
// pub const NEW_IDS_LIMIT: usize = 20_000;

impl HashValueStore {
    pub(crate) fn new<T>(consumer: T) -> Self
    where
        T: Into<Option<Consumer<HashId>>>,
    {
        Self {
            hashes: SharedIndexMap::with_chunk_capacity(VALUES_LENGTH), // ~320MB
            values: SharedIndexMap::with_chunk_capacity(VALUES_LENGTH), // ~80MB
            free_ids: consumer.into(),
            new_ids: ChunkedVec::with_chunk_capacity(512 * 1024), // ~8KB
            values_bytes: 0,
        } // Total ~400MB
    }

    pub fn get_memory_usage(
        &self,
        strings_total_bytes: usize,
        shapes_total_bytes: usize,
        commit_index_total_bytes: usize,
        nshapes: usize,
    ) -> RepositoryMemoryUsage {
        let values_bytes = self.values_bytes;
        let values_capacity = self.values.capacity();
        let hashes_capacity = self.hashes.capacity();

        let total_bytes = values_bytes
            .saturating_add(values_capacity * size_of::<Option<Box<[u8]>>>())
            .saturating_add(hashes_capacity * size_of::<ObjectHash>())
            .saturating_add(strings_total_bytes)
            .saturating_add(shapes_total_bytes)
            .saturating_add(commit_index_total_bytes);

        RepositoryMemoryUsage {
            values_bytes,
            values_capacity,
            values_length: self.values.len(),
            hashes_capacity,
            hashes_length: self.hashes.len(),
            total_bytes,
            npending_free_ids: self.free_ids.as_ref().map(|c| c.len()).unwrap_or(0),
            gc_npending_free_ids: GC_PENDING_HASHIDS.load(Ordering::Acquire),
            nshapes,
            strings_total_bytes,
            shapes_total_bytes,
            commit_index_total_bytes,
            new_ids_cap: self.new_ids.capacity(),
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self {
            hashes: SharedIndexMap::empty(),
            values: SharedIndexMap::empty(),
            free_ids: self.free_ids.take(),
            new_ids: ChunkedVec::empty(),
            values_bytes: 0,
        }
    }

    pub(crate) fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, HashIdError> {
        let hash_id = match self.get_free_id() {
            Some(free_hash_id) => {
                free_hash_id
            },
            None => {
                let next = self.hashes.len();
                HashId::try_from(next)?
            }
        };

        self.new_ids.push(hash_id);

        Ok(VacantObjectHash {
            entry: None,
            map: Some(&mut self.hashes),
            hash_id,
        })
    }

    fn get_free_id(&mut self) -> Option<HashId> {
        self.free_ids.as_mut()?.pop().ok()
    }

    pub(crate) fn insert_value_at(
        &mut self,
        hash_id: HashId,
        value: Box<[u8]>,
    ) -> Result<(), HashIdError> {
        self.values.insert_at(hash_id, value)
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Result<Option<ObjectHash>, HashIdError> {
        let hash = self
            .hashes
            .with(hash_id, |hash| match hash {
                Some(Some(hash)) => Some(**hash),
                Some(None) => {
                    panic!("AAAAAA {:?} {:?} LEN={:?}", hash_id, hash, self.hashes.len());
                },
                None => return panic!(),
            })
            .unwrap();

        Ok(hash)

        // self.hashes.get(hash_id)
    }

    pub(crate) fn with_value<F, R>(&self, hash_id: HashId, fun: F) -> Result<R, DBError>
    where
        F: FnOnce(Option<&Option<Box<[u8]>>>) -> R,
    {
        Ok(self.values.with(hash_id, fun)?)
    }

    // pub(crate) fn get_value(&self, hash_id: HashId) -> Result<Option<&[u8]>, HashIdError> {
    //     match self.values.get(hash_id)? {
    //         Some(value) => Ok(value.as_ref().map(|v| v.as_ref())),
    //         None => Ok(None),
    //     }
    // }

    pub(crate) fn contains(&self, hash_id: HashId) -> Result<bool, HashIdError> {
        Ok(self.values.with(hash_id, |v| v.is_some())?)
    }

    fn take_new_ids(&mut self) -> ChunkedVec<HashId> {
        std::mem::replace(
            &mut self.new_ids,
            ChunkedVec::with_chunk_capacity(512 * 1024),
        )
    }
}

pub struct InMemory {
    current_cycle: ChunkedVec<(HashId, Arc<[u8]>)>,
    pub hashes: HashValueStore,
    sender: Option<Sender<Command>>,
    pub context_hashes: Map<u64, HashId>,
    context_hashes_cycles: VecDeque<Vec<u64>>,
    thread_handle: Option<JoinHandle<()>>,
    shapes: DirectoryShapes,
    string_interner: StringInterner,
}

impl GarbageCollector for InMemory {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        self.new_cycle_started();
        Ok(())
    }

    fn block_applied(
        &mut self,
        // referenced_older_objects: ChunkedVec<HashId>,
    ) -> Result<(), GarbageCollectionError> {
        panic!()
        // self.block_applied(referenced_older_objects);
        // Ok(())
    }
}

impl Flushable for InMemory {
    fn flush(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Persistable for InMemory {
    fn is_persistent(&self) -> bool {
        false
    }
}

impl KeyValueStoreBackend for InMemory {
    fn reload_database(&mut self) -> Result<(), ReloadError> {
        self.reload_database()
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.contains(hash_id)
    }

    fn put_context_hash(&mut self, object_ref: ObjectReference) -> Result<(), DBError> {
        self.put_context_hash_impl(object_ref.hash_id())
    }

    fn get_context_hash(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<ObjectReference>, DBError> {
        Ok(self.get_context_hash_impl(context_hash).map(Into::into))
    }

    fn get_hash(&self, object_ref: ObjectReference) -> Result<Cow<ObjectHash>, DBError> {
        self.get_hash(object_ref.hash_id()).map(Cow::Owned)
    }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        self.get_vacant_entry_hash()
    }

    fn memory_usage(&self) -> RepositoryMemoryUsage {
        let strings_total_bytes = self.string_interner.memory_usage().total_bytes;
        let shapes_total_bytes = self.shapes.total_bytes();
        let commit_index_total_bytes = self.context_hashes.len()
            * (std::mem::size_of::<HashId>() + std::mem::size_of::<u64>());

        self.hashes.get_memory_usage(
            strings_total_bytes,
            shapes_total_bytes,
            commit_index_total_bytes,
            self.shapes.nshapes(),
        )
    }

    fn get_shape(&self, shape_id: DirectoryShapeId) -> Result<ShapeStrings, DBError> {
        self.shapes
            .get_shape(shape_id)
            .map(ShapeStrings::SliceIds)
            .map_err(Into::into)
    }

    fn make_shape(
        &mut self,
        dir: &[(StringId, DirEntryId)],
    ) -> Result<Option<DirectoryShapeId>, DBError> {
        self.shapes.make_shape(dir).map_err(Into::into)
    }

    fn synchronize_strings_from(&mut self, string_interner: &StringInterner) {
        self.string_interner.extend_from(string_interner);
    }

    fn get_str(&self, string_id: StringId) -> Option<Cow<str>> {
        self.string_interner.get_str(string_id).ok()
    }

    fn get_object(
        &self,
        object_ref: ObjectReference,
        storage: &mut Storage,
        strings: &mut StringInterner,
    ) -> Result<Object, DBError> {
        self.with_value(object_ref.hash_id(), |value| {
            let object_bytes = match value {
                Some(Some(value)) => value,
                _ => {
                    let got_key = self.hashes.values.contains_key(object_ref.hash_id());
                    println!(
                        "OBJECT NOT FOUND HASH_ID={:?} VALUE={:?} GOT_KEY={:?}",
                        object_ref, value, got_key,
                    );
                    println!("VALUES_LEN={:?}", self.hashes.values.len());
                    todo!();
                }
            };
            in_memory::deserialize_object(object_bytes, storage, strings, self).map_err(Into::into)
        })?

        // let object_bytes = self.get_value(object_ref.hash_id())?.unwrap_or(&[]);
        // in_memory::deserialize_object(object_bytes, storage, strings, self).map_err(Into::into)
    }

    fn get_inode(
        &self,
        object_ref: ObjectReference,
        storage: &mut Storage,
        strings: &mut StringInterner,
    ) -> Result<DirectoryOrInodeId, DBError> {
        self.with_value(object_ref.hash_id(), |value| {
            let object_bytes = match value {
                Some(Some(value)) => value,
                _ => todo!(),
            };
            in_memory::deserialize_inode(object_bytes, storage, strings, self).map_err(Into::into)
        })?

        // let object_bytes = self.get_value(object_ref.hash_id())?.unwrap_or(&[]);
        // in_memory::deserialize_inode(object_bytes, storage, strings, self).map_err(Into::into)
    }

    fn get_object_bytes<'a>(
        &self,
        object_ref: ObjectReference,
        buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], DBError> {
        buffer.clear();

        self.with_value(object_ref.hash_id(), |value| {
            if let Some(Some(value)) = value {
                buffer.extend_from_slice(value)
            };
        })
        .unwrap();

        // let slice = self.get_value(object_ref.hash_id())?.unwrap_or(&[]);

        // buffer.clear();
        // buffer.extend_from_slice(slice);

        Ok(buffer)
    }

    fn commit(
        &mut self,
        working_tree: &WorkingTree,
        parent_commit_ref: Option<ObjectReference>,
        author: String,
        message: String,
        date: u64,
    ) -> Result<(ContextHash, Box<SerializeStats>), DBError> {
        self.commit_impl(working_tree, parent_commit_ref, author, message, date, true)
    }

    fn get_hash_id(&self, object_ref: ObjectReference) -> Result<HashId, DBError> {
        object_ref.hash_id_opt().ok_or(DBError::HashIdFailed)
    }

    fn take_strings_on_reload(&mut self) -> Option<StringInterner> {
        // On reload, `Self::string_interner` contains all strings and their hashes
        let string_interner = std::mem::take(&mut self.string_interner);

        // In the repository, we only want strings without their hashes
        self.synchronize_strings_from(&string_interner);

        self.string_interner
            .set_to_serialize_index(string_interner.get_to_serialize_index());

        Some(string_interner)
    }

    fn make_hash_id_ready_for_commit(&mut self, hash_id: HashId) -> Result<HashId, DBError> {
        // Unused HashId are garbage collected
        Ok(hash_id)
    }

    fn get_read_statistics(&self) -> Result<Option<ReadStatistics>, DBError> {
        Ok(None)
    }

    #[cfg(test)]
    fn synchronize_data(
        &mut self,
        batch: &[(HashId, Box<[u8]>)],
        _output: &[u8],
    ) -> Result<Option<AbsoluteOffset>, DBError> {
        let mut vec = ChunkedVec::with_chunk_capacity(batch.len().max(1000));
        for item in batch {
            vec.push(item.clone());
        }
        self.write_batch(vec)?;
        Ok(None)
    }
}

impl InMemory {
    pub fn try_new() -> Result<Self, std::io::Error> {
        // TODO - TE-210: Remove once we hace proper support for history modes.
        let garbage_collector_disabled = std::env::var("DISABLE_INMEM_CONTEXT_GC")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .expect("Provided `DISABLE_INMEM_CONTEXT_GC` value cannot be converted to bool");

        let (sender, thread_handle, hashes) = if garbage_collector_disabled {
            (None, None, HashValueStore::new(None))
        } else {
            let (sender, recv) = crossbeam_channel::unbounded();
            let (prod, cons) = tezos_spsc::bounded(2_000_000);
            let hashes = HashValueStore::new(cons);
            let objects_view = hashes.values.get_view();
            let hashes_view = hashes.hashes.get_view();

            let thread_handle = std::thread::Builder::new()
                .name("ctx-inmem-gc-thread".to_string())
                .spawn(move || {
                    GCThread {
                        // cycles: Cycles::default(),
                        recv,
                        free_ids: prod,
                        pending: Vec::new(),
                        debug: false,
                        // global: IndexMap::with_chunk_capacity(10_000_000),
                        global_counter: IndexMap::with_chunk_capacity(VALUES_LENGTH),
                        counter: 0,
                        objects_view,
                        hashes_view,
                    }
                    .run()
                })?;

            (Some(sender), Some(thread_handle), hashes)
        };

        let current_cycle = ChunkedVec::with_chunk_capacity(512 * 1024);
        let context_hashes = Default::default();

        let mut context_hashes_cycles = VecDeque::with_capacity(PRESERVE_CYCLE_COUNT);
        for _ in 0..PRESERVE_CYCLE_COUNT {
            context_hashes_cycles.push_back(Default::default())
        }

        Ok(Self {
            current_cycle,
            hashes,
            sender,
            context_hashes,
            context_hashes_cycles,
            thread_handle,
            shapes: DirectoryShapes::default(),
            string_interner: StringInterner::default(),
        })
    }

    /// Reload context from disk
    fn reload_database(&mut self) -> Result<(), ReloadError> {
        let (tree, parent_hash, commit) = {
            let mut ondisk = Persistent::try_new(PersistentConfiguration {
                db_path: Some("/tmp/tezedge/context".to_string()),
                // db_path: Some("/home/sebastien/tmp/replay/context".to_string()),
                startup_check: false,
                read_mode: true,
            })?;

            ondisk.reload_database()?;

            let checkout_context_hash: ContextHash = ondisk
                .get_last_context_hash()
                .ok_or(ReloadError::LastCommitNotFound)?;

            let read_repo: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(ondisk));
            let index = TezedgeIndex::new(Arc::clone(&read_repo), None);
            let context = index
                .checkout(&checkout_context_hash)?
                .ok_or(ReloadError::CheckoutFailed)?;

            // Take the commit from repository
            let commit: Commit = index
                .fetch_commit_from_context_hash(&checkout_context_hash)?
                .ok_or(ReloadError::FetchCommitFailed)?;

            // If the commit has a parent, fetch it
            // It is necessary for our new repository to have it.
            let parent_hash: Option<ObjectHash> = match commit.parent_commit_ref {
                Some(parent) => {
                    let repo = read_repo.read()?;
                    Some(repo.get_hash(parent)?.into_owned())
                }
                None => None,
            };

            // Traverse the tree, to store it in the `Storage`
            context.tree.traverse_working_tree(false)?;

            // Forget HashId and offsets, they will be recomputed.
            context.index.storage.borrow_mut().forget_references();

            // Extract the `Storage`, `StringInterner` and `WorkingTree` from
            // the index
            (
                Rc::try_unwrap(context.tree).ok().unwrap(), // Never fail, there is 1 reference alive
                parent_hash,
                commit,
            )
        };

        // Put the parent hash in the new repository (in-memory one)
        let parent_ref: Option<ObjectReference> = match parent_hash {
            Some(parent_hash) => Some(self.put_hash(parent_hash)?.into()),
            None => None,
        };

        // Commit the tree in the in-memory repository
        self.commit_impl(
            &tree,
            parent_ref,
            commit.author,
            commit.message,
            commit.time,
            false,
        )
        .map_err(|error| ReloadError::CommitFailed { error })?;

        self.string_interner = tree
            .index
            .string_interner
            .take()
            .ok_or(ReloadError::StringInternerNotFound)?;

        println!("AFTER_RELOAD MEMORY_USAGE={:#?}", self.memory_usage());

        Ok(())
    }

    fn maybe_send_new_chunks_to_gc(&mut self) {
        let sender = match self.sender.as_ref() {
            Some(sender) => sender,
            None => return,
        };

        let objects_chunks = self.hashes.values.clone_new_chunks();
        let hashes_chunks = self.hashes.hashes.clone_new_chunks();

        if objects_chunks.is_none() && hashes_chunks.is_none() {
            return;
        }

        sender
            .send(Command::NewChunks {
                objects_chunks,
                hashes_chunks,
            })
            .unwrap();
    }

    fn commit_impl(
        &mut self,
        working_tree: &WorkingTree,
        parent_commit_ref: Option<ObjectReference>,
        author: String,
        message: String,
        date: u64,
        mark_as_applied: bool,
    ) -> Result<(ContextHash, Box<SerializeStats>), DBError> {
        self.maybe_send_new_chunks_to_gc();

        let PostCommitData {
            commit_ref,
            batch,
            serialize_stats,
            ..
        } = working_tree
            .prepare_commit(
                date,
                author,
                message,
                parent_commit_ref,
                self,
                Some(in_memory::serialize_object),
                None,
                false,
            )
            .map_err(Box::new)?;

        self.write_batch(batch)?;
        self.maybe_send_new_chunks_to_gc();

        // println!("AFTER_BATCH={:?}", self.hashes.values.len());

        self.put_context_hash(commit_ref)?;
        // if mark_as_applied {
        let commit_hash_id = commit_ref.hash_id();
        self.block_applied(commit_hash_id);
        // }

        let commit_hash = get_commit_hash(commit_ref, self).map_err(Box::new)?;
        Ok((commit_hash, serialize_stats))
    }

    pub fn put_hash(&mut self, hash: ObjectHash) -> Result<HashId, DBError> {
        let hash_id = self
            .get_vacant_object_hash()?
            .write_with(|entry| *entry = hash);
        Ok(hash_id)
    }

    pub(crate) fn get_vacant_entry_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        if self.hashes.new_ids.len() >= NEW_IDS_LIMIT {
            let new_ids = self.hashes.take_new_ids();
            self.sender
                .as_ref()
                .map(|s| s.send(Command::MarkNewIds { new_ids }));
        }

        self.hashes.get_vacant_object_hash().map_err(Into::into)
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Result<ObjectHash, DBError> {
        self.hashes
            .get_hash(hash_id)?
            .ok_or_else(|| DBError::HashNotFound {
                object_ref: hash_id.into(),
            })
    }

    pub(crate) fn with_value<F, R>(&self, hash_id: HashId, fun: F) -> Result<R, DBError>
    where
        F: FnOnce(Option<&Option<Box<[u8]>>>) -> R,
    {
        Ok(self.hashes.values.with(hash_id, fun)?)
    }

    // pub(crate) fn get_value(&self, hash_id: HashId) -> Result<Option<&[u8]>, DBError> {
    //     self.hashes.get_value(hash_id).map_err(Into::into)
    // }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.hashes.contains(hash_id).map_err(Into::into)
    }

    pub fn write_batch(
        &mut self,
        mut batch: ChunkedVec<(HashId, Box<[u8]>)>,
    ) -> Result<(), DBError> {
        while let Some(chunk) = batch.pop_first_chunk() {
            for (hash_id, value) in chunk.into_iter() {
                self.hashes.insert_value_at(hash_id, value)?;
                // self.current_cycle.push((hash_id, value));
            }
        }
        Ok(())
    }

    pub fn new_cycle_started(&mut self) {
        if let Some(sender) = &self.sender {
            // let values_in_cycle = std::mem::replace(
            //     &mut self.current_cycle,
            //     ChunkedVec::with_chunk_capacity(512 * 1024),
            // );
            // let new_ids = self.hashes.take_new_ids();

            // if let Err(e) = sender.try_send(Command::StartNewCycle {
            //     values_in_cycle,
            //     new_ids,
            // }) {
            //     eprintln!("Fail to send Command::StartNewCycle to GC worker: {:?}", e);
            // }

            if let Some(unused) = self.context_hashes_cycles.pop_front() {
                for hash in unused {
                    self.context_hashes.remove(&hash);
                }
            }
            self.context_hashes_cycles.push_back(Default::default());
        }
    }

    pub fn block_applied(&mut self, commit_hash_id: HashId) {
        let sender = match self.sender.as_ref() {
            Some(sender) => sender,
            None => return,
        };

        // let values_in_block = std::mem::replace(
        //     &mut self.current_cycle,
        //     ChunkedVec::with_chunk_capacity(512 * 1024),
        // );
        let new_ids = self.hashes.take_new_ids();

        if let Err(e) = sender.send(Command::MarkReused {
            // reused,
            // values_in_block,
            new_ids,
            commit_hash_id,
        }) {
            eprintln!("Fail to send Command::MarkReused to GC worker: {:?}", e);
        }
    }

    pub fn get_context_hash_impl(&self, context_hash: &ContextHash) -> Option<HashId> {
        let mut hasher = DefaultHasher::new();
        hasher.write(context_hash.as_ref());
        let hashed = hasher.finish();

        self.context_hashes.get(&hashed).cloned()
    }

    pub fn put_context_hash_impl(&mut self, commit_hash_id: HashId) -> Result<(), DBError> {
        let commit_hash = self
            .hashes
            .get_hash(commit_hash_id)?
            .ok_or(DBError::MissingObject {
                object_ref: commit_hash_id.into(),
            })?;

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash[..]);
        let hashed = hasher.finish();

        self.context_hashes.insert(hashed, commit_hash_id);
        if let Some(back) = self.context_hashes_cycles.back_mut() {
            back.push(hashed);
        };

        Ok(())
    }
}

impl Drop for InMemory {
    fn drop(&mut self) {
        let sender = match self.sender.take() {
            Some(sender) => sender,
            None => return,
        };

        if let Err(e) = sender.send(Command::Close) {
            eprintln!("Fail to send Command::Close to GC worker: {:?}", e);
            return;
        }

        let thread_handle = match self.thread_handle.take() {
            Some(thread_handle) => thread_handle,
            None => return,
        };

        if let Err(e) = thread_handle.join() {
            eprintln!("Fail to join  GC worker thread: {:?}", e);
        }
    }
}
