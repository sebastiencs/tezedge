// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{borrow::Borrow, cell::RefCell, collections::HashSet, convert::TryInto, sync::{Arc, RwLock, atomic::AtomicU64}};
use std::{convert::TryFrom, rc::Rc};

use crypto::hash::ContextHash;
use ocaml_interop::BoxRoot;

use crate::{ContextKeyValueStore, StringTreeMap, hash::EntryHash, kv_store::{HashId, in_memory::RepositoryMemoryUsage}, persistent::DBError, working_tree::{Commit, Entry, KeyFragment, Node, Tree, serializer::deserialize, string_interner::{StringId, StringInterner}, tree_storage::{BlobStorageId, NodeId, StorageMemoryUsage, TreeStorage}, working_tree::MerkleError, working_tree_stats::MerkleStoragePerfReport}};
use crate::{working_tree::working_tree::WorkingTree, IndexApi};
use crate::{
    working_tree::working_tree::{FoldDepth, TreeWalker},
    ContextKeyOwned,
};
use crate::{
    ContextError, ContextKey, ContextValue, ProtocolContextApi, ShellContextApi, StringTreeEntry,
    TreeId,
};

// Represents the patch_context function passed from the OCaml side
// It is opaque to rust, we don't care about it's actual type
// because it is not used on Rust, but we need a type to represent it.
pub struct PatchContextFunction {}

#[derive(Clone)]
pub struct TezedgeIndex {
    pub repository: Arc<RwLock<ContextKeyValueStore>>,
    pub patch_context: Rc<Option<BoxRoot<PatchContextFunction>>>,
    // pub strings: Rc<RefCell<StringInterner>>,
    pub storage: Rc<RefCell<TreeStorage>>,

    pub time_clear: Rc<AtomicU64>,
}

// TODO: some of the utility methods here (and in `WorkingTree`) should probably be
// standalone functions defined in a separate module that take the `index`
// as an argument.
// Also revise all errors defined, some may be obsolete now.
impl TezedgeIndex {
    pub fn new(
        repository: Arc<RwLock<ContextKeyValueStore>>,
        patch_context: Option<BoxRoot<PatchContextFunction>>,
    ) -> Self {
        let patch_context = Rc::new(patch_context);
        Self {
            patch_context,
            // strings: Default::default(),
            repository,
            storage: Default::default(),
            time_clear: Rc::new(AtomicU64::new(0)),
        }
    }

    // pub fn get_str(&self, s: &str) -> StringId {
    //     let mut strings = self.strings.borrow_mut();
    //     strings.get_string_id(s)
    // }

    // pub fn get_keys_memory_usage(&self) -> StringMemoryUsage {
    //     let strings = (&*self.strings).borrow();
    //     todo!()
    //     // strings.memory_usage()
    // }

    pub fn find_entry_bytes(&self, hash: HashId) -> Result<Option<Vec<u8>>, DBError> {
        let repo = self.repository.read()?;
        Ok(repo.get_value(hash)?.map(|v| v.to_vec()))
    }

    pub fn find_entry(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Option<Entry>, DBError> {
        match self.find_entry_bytes(hash)? {
            None => Ok(None),
            Some(entry_bytes) => {
                // let mut strings = (&*self.strings).borrow_mut();
                Ok(Some(deserialize(
                    entry_bytes.as_ref(),
                    //&mut tree_storage.strings,
                    tree_storage,
                )?))
            }
        }
    }

    pub fn get_hash(&self, hash_id: HashId) -> Result<Option<EntryHash>, DBError> {
        Ok(self
            .repository
            .read()?
            .get_hash(hash_id)?
            .map(|h| h.into_owned()))
    }

    pub fn get_entry(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Entry, MerkleError> {
        match self.find_entry(hash, tree_storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn find_commit(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Option<Commit>, DBError> {
        match self.find_entry(hash, tree_storage)? {
            Some(Entry::Commit(commit)) => Ok(Some(*commit)),
            Some(Entry::Tree(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Some(Entry::Blob(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
            None => Ok(None),
        }
    }

    pub fn get_commit(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Commit, MerkleError> {
        match self.find_commit(hash, tree_storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn find_tree(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Option<Tree>, DBError> {
        match self.find_entry(hash, tree_storage)? {
            Some(Entry::Tree(tree)) => Ok(Some(tree)),
            Some(Entry::Blob(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Some(Entry::Commit { .. }) => Err(DBError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
            None => Ok(None),
        }
    }

    pub fn get_tree(
        &self,
        hash: HashId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Tree, MerkleError> {
        match self.find_tree(hash, tree_storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn contains(&self, hash: HashId) -> Result<bool, DBError> {
        let db = self.repository.read()?;
        Ok(db.contains(hash)?)
    }

    pub fn get_context_hash_id(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<HashId>, MerkleError> {
        let db = self.repository.read()?;
        Ok(db.get_context_hash(context_hash)?)
    }

    /// Convert key in array form to string form
    pub fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    /// Convert key in string form to array form
    pub fn string_to_key(&self, string: &str) -> ContextKeyOwned {
        string.split('/').map(str::to_string).collect()
    }

    pub fn node_entry(
        &self,
        node_id: NodeId,
        tree_storage: &mut TreeStorage,
    ) -> Result<Entry, MerkleError> {
        let node = tree_storage.get_node(node_id).unwrap();

        if let Some(e) = node.get_entry() {
            return Ok(e);
        };
        let hash = node.get_hash_id()?;
        std::mem::drop(node);

        let entry = self.get_entry(hash, tree_storage)?;

        let node = tree_storage.get_node(node_id).unwrap();
        node.set_entry(&entry)?;

        Ok(entry)
    }

    /// Get context tree under given prefix in string form (for JSON)
    /// depth - None returns full tree
    pub fn _get_context_tree_by_prefix(
        &self,
        context_hash: HashId,
        prefix: &ContextKey,
        depth: Option<usize>,
        tree_storage: &mut TreeStorage,
    ) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null);
        }

        let mut out = StringTreeMap::new();
        let commit = self.get_commit(context_hash, tree_storage)?;

        let root_tree = self.get_tree(commit.root_hash, tree_storage)?;
        let prefixed_tree = self.find_raw_tree(root_tree, prefix, tree_storage)?;
        let delimiter = if prefix.is_empty() { "" } else { "/" };

        let prefixed_tree = tree_storage.get_tree(prefixed_tree).unwrap().to_vec();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.node_entry(*child_node, tree_storage)?;

            let key = tree_storage.get_str(*key);

            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            let rdepth = depth.map(|d| d - 1);
            let key_str = key.to_string();

            std::mem::drop(key);

            //let key_str: &str = key.borrow();
            out.insert(
                key_str,
                self.get_context_recursive(&fullpath, &entry, rdepth, tree_storage)?,
            );
        }

        //stat_updater.update_execution_stats(&mut self.stats);
        Ok(StringTreeEntry::Tree(out))
    }

    /// Go recursively down the tree from Entry, build string tree and return it
    /// (or return hex value if Blob)
    fn get_context_recursive(
        &self,
        path: &str,
        entry: &Entry,
        depth: Option<usize>,
        tree_storage: &mut TreeStorage,
    ) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null);
        }

        match entry {
            Entry::Blob(blob_id) => {
                let blob = tree_storage.get_blob(*blob_id).unwrap();
                Ok(StringTreeEntry::Blob(hex::encode(blob)))
            },
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                let mut new_tree = StringTreeMap::new();

                let tree = tree_storage.get_tree(*tree).unwrap().to_vec();

                for (key, child_node) in tree.iter() {
                    let key = tree_storage.get_str(*key);
                    let fullpath = path.to_owned() + "/" + key;
                    let key_str = key.to_string();
                    std::mem::drop(key);

                    let entry = self.node_entry(*child_node, tree_storage)?;
                    let rdepth = depth.map(|d| d - 1);

                    new_tree.insert(
                        key_str,
                        self.get_context_recursive(&fullpath, &entry, rdepth, tree_storage)?,
                    );
                }
                Ok(StringTreeEntry::Tree(new_tree))
            }
            Entry::Commit(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "Commit".to_string(),
            }),
        }
    }

    /// Find tree by path and return a copy. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    pub fn find_raw_tree(
        &self,
        root: Tree,
        key: &[&str],
        tree_storage: &mut TreeStorage,
    ) -> Result<Tree, MerkleError> {
        let first = match key.first() {
            Some(first) => *first,
            None => {
                // terminate recursion if end of path was reached
                return Ok(root.clone());
            }
        };

        // first get node at key
        let child_node_id = match tree_storage.get_tree_node_id(root, first) {
            Some(hash) => hash,
            None => {
                return Ok(Tree::empty());
            }
        };

        // println!("CHILD_NODE={:?}", child_node_id);

        // get entry (from working tree)
        let child_node = tree_storage.get_node(child_node_id).unwrap();
        if let Some(entry) = child_node.get_entry() {
            match entry {
                Entry::Tree(tree) => {
                    return self.find_raw_tree(tree, &key[1..], tree_storage);
                }
                Entry::Blob(_) => return Ok(Tree::empty()),
                Entry::Commit { .. } => {
                    return Err(MerkleError::FoundUnexpectedStructure {
                        sought: "Tree/Blob".to_string(),
                        found: "commit".to_string(),
                    })
                }
            }
        }

        // println!("BBBBB");

        // get entry by hash (from DB)
        let hash = child_node.get_hash_id();


        // println!("CCCCC {:?}", hash);

        let hash = hash?;

        std::mem::drop(child_node);
        // let hash = child_node.get_hash_id()?;
        let entry = self.get_entry(hash, tree_storage)?;

        let child_node = tree_storage.get_node(child_node_id).unwrap();
        child_node.set_entry(&entry)?;

        match entry {
            Entry::Tree(tree) => self.find_raw_tree(tree, &key[1..], tree_storage),
            Entry::Blob(_) => Ok(Tree::empty()),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    /// Get value from historical context identified by commit hash.
    pub fn get_history(
        &self,
        commit_hash: HashId,
        key: &ContextKey,
    ) -> Result<ContextValue, MerkleError> {
        // let stat_updater = StatUpdater::new(MerkleStorageAction::GetHistory, Some(key));
        let mut tree_storage = (&*self.storage).borrow_mut();

        let commit = self.get_commit(commit_hash, &mut tree_storage)?;
        let tree = self.get_tree(commit.root_hash, &mut tree_storage)?;

        // let tree_slice = tree_storage.get_tree(tree).unwrap();
        // let n = tree_storage.get_node(tree_slice[0].1);
        // let s = tree_storage.get_str(tree_slice[0].0);
        // println!("TREE_ID={:?} TREE={:?} S={:?} NODE={:?}", tree, tree_slice, s, n);

        let blob_id = self.get_from_tree(&tree, key, &mut tree_storage)?;

        let blob = tree_storage.get_blob(blob_id).unwrap();

        // stat_updater.update_execution_stats(&mut self.stats);
        Ok(blob.to_vec())
    }

    fn get_from_tree(
        &self,
        root: &Tree,
        key: &ContextKey,
        tree_storage: &mut TreeStorage,
    ) -> Result<BlobStorageId, MerkleError> {
        // println!("VV");
        let (file, path) = key.split_last().ok_or(MerkleError::KeyEmpty)?;
        // println!("VV1 {:?} {:?} {:?}", root, path, tree_storage.get_tree(*root));

        let node = self.find_raw_tree(*root, &path, tree_storage)?;
        // println!("VV2");

        // get file node from tree
        let node_id =
            tree_storage
                .get_tree_node_id(node, *file)
                // .cloned()
                .ok_or_else(|| MerkleError::ValueNotFound {
                    key: self.key_to_string(key),
                })?;
        // println!("VV3");

        // get blob
        match self.node_entry(node_id, tree_storage)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob {
                key: self.key_to_string(key),
            }),
        }
    }

    /// Construct Vec of all context key-values under given prefix
    pub fn get_context_key_values_by_prefix(
        &self,
        context_hash: HashId,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let mut tree_storage = (&*self.storage).borrow_mut();

        let commit = self.get_commit(context_hash, &mut tree_storage)?;
        let root_tree = self.get_tree(commit.root_hash, &mut tree_storage)?;
        let rv = self._get_context_key_values_by_prefix(&root_tree, prefix, &mut tree_storage);

        rv
    }

    fn _get_context_key_values_by_prefix(
        &self,
        root_tree: &Tree,
        prefix: &ContextKey,
        tree_storage: &mut TreeStorage,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_raw_tree(*root_tree, prefix, tree_storage)?;
        let mut keyvalues: Vec<(ContextKeyOwned, ContextValue)> = Vec::new();
        let delimiter = if prefix.is_empty() { "" } else { "/" };

        let prefixed_tree = tree_storage.get_tree(prefixed_tree).unwrap().to_vec();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.node_entry(*child_node, tree_storage)?;

            let key = tree_storage.get_str(*key);
            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            std::mem::drop(key);

            self.get_key_values_from_tree_recursively(
                &fullpath,
                &entry,
                &mut keyvalues,
                tree_storage,
            )?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }

    // TODO: can we get rid of the recursion?
    fn get_key_values_from_tree_recursively(
        &self,
        path: &str,
        entry: &Entry,
        entries: &mut Vec<(ContextKeyOwned, ContextValue)>,
        tree_storage: &mut TreeStorage,
    ) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob_id) => {
                // push key-value pair
                let blob = tree_storage.get_blob(*blob_id).unwrap();
                entries.push((self.string_to_key(path), blob.to_vec()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                // let tree_storage = (&*self.trees).borrow();
                let tree = tree_storage.get_tree(*tree).unwrap().to_vec();

                tree.iter()
                    .map(|(key, child_node)| {
                        let key = tree_storage.get_str(*key);
                        let fullpath = path.to_owned() + "/" + key;
                        std::mem::drop(key);

                        match self.node_entry(*child_node, tree_storage) {
                            Err(_) => Ok(()),
                            Ok(entry) => self.get_key_values_from_tree_recursively(
                                &fullpath,
                                &entry,
                                entries,
                                tree_storage,
                            ),
                        }
                    })
                    .find_map(|res| match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    })
                    .unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => match self.get_entry(commit.root_hash, tree_storage) {
                Err(err) => Err(err),
                Ok(entry) => {
                    self.get_key_values_from_tree_recursively(path, &entry, entries, tree_storage)
                }
            },
        }
    }
}

impl IndexApi<TezedgeContext> for TezedgeIndex {
    fn exists(&self, context_hash: &ContextHash) -> Result<bool, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => return Ok(false),
            }
        };

        let mut tree_storage = self.storage.borrow_mut();

        if let Some(Entry::Commit(_)) = self.find_entry(hash_id, &mut tree_storage)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn checkout(&self, context_hash: &ContextHash) -> Result<Option<TezedgeContext>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => return Ok(None),
            }
        };

        let mut tree_storage = self.storage.borrow_mut();
        let now = std::time::Instant::now();
        tree_storage.clear();
        let elapsed: u64 = now.elapsed().as_nanos().try_into().unwrap();
        self.time_clear.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);

        if let Some(commit) = self.find_commit(hash_id, &mut tree_storage)? {
            if let Some(tree) = self.find_tree(commit.root_hash, &mut tree_storage)? {
                let tree = WorkingTree::new_with_tree(self.clone(), tree);

                Ok(Some(TezedgeContext::new(
                    self.clone(),
                    Some(hash_id),
                    Some(Rc::new(tree)),
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn block_applied(&self, referenced_older_entries: Vec<HashId>) -> Result<(), ContextError> {
        Ok(self
            .repository
            .write()?
            .block_applied(referenced_older_entries)?)
    }

    fn cycle_started(&mut self) -> Result<(), ContextError> {
        Ok(self.repository.write()?.new_cycle_started()?)
    }

    fn get_key_from_history(
        &self,
        context_hash: &ContextHash,
        key: &ContextKey,
    ) -> Result<Option<ContextValue>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        // println!("CONTEXT_HASH_ID={:?}", hash_id);

        match self.get_history(hash_id, key) {
            Err(MerkleError::ValueNotFound { key: _ }) => Ok(None),
            Err(MerkleError::EntryNotFound { hash_id: _ }) => Ok(None),
            Err(err) => Err(ContextError::MerkleStorageError { error: err }),
            Ok(val) => Ok(Some(val)),
        }
    }

    fn get_key_values_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;
            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        let context = match self.checkout(context_hash)? {
            Some(context) => context,
            None => return Ok(None),
        };

        let repository = self.repository.read()?;
        context
            .tree
            .get_key_values_by_prefix(hash_id, prefix, &*repository)
            .map_err(ContextError::from)
    }

    fn get_context_tree_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
        depth: Option<usize>,
    ) -> Result<StringTreeEntry, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;
            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        let mut tree_storage = self.storage.borrow_mut();

        self._get_context_tree_by_prefix(hash_id, prefix, depth, &mut tree_storage)
            .map_err(ContextError::from)
    }
}

// #[derive(Default)]
// pub struct StringInterner {
//     strings: HashSet<Rc<str>>,
//     mem_usage: StringMemoryUsage,
// }

// impl StringInterner {
//     pub fn get_str(&mut self, s: &str) -> Rc<str> {
//         if s.len() >= 30 {
//             return Rc::from(s);
//         }

//         let mut new_str = false;
//         let string = self
//             .strings
//             .get_or_insert_with(s, |s| {
//                 new_str = true;
//                 Rc::from(s)
//             })
//             .clone();

//         if new_str {
//             self.mem_usage.add_string(s);
//         }

//         string
//     }

//     fn memory_usage(&self) -> StringMemoryUsage {
//         self.mem_usage.clone()
//     }
// }

#[derive(Default, Copy, Clone)]
pub struct StringMemoryUsage {
    nbytes: usize,
    nstrings: usize,
}

impl StringMemoryUsage {
    fn add_string(&mut self, string: &str) {
        self.nbytes = self.nbytes.saturating_add(string.len());
        self.nstrings = self.nstrings.saturating_add(1);
    }
}

// context implementation using merkle-tree-like storage
#[derive(Clone)]
pub struct TezedgeContext {
    pub index: TezedgeIndex,
    pub parent_commit_hash: Option<HashId>,
    pub tree_id: TreeId,
    tree_id_generator: Rc<RefCell<TreeIdGenerator>>,
    pub tree: Rc<WorkingTree>,
}

impl ProtocolContextApi for TezedgeContext {
    fn add(&self, key: &ContextKey, value: &[u8]) -> Result<Self, ContextError> {
        let tree = self.tree.add(key, value)?;

        Ok(self.with_tree(tree))
    }

    fn delete(&self, key_prefix_to_delete: &ContextKey) -> Result<Self, ContextError> {
        let tree = self.tree.delete(key_prefix_to_delete)?;

        Ok(self.with_tree(tree))
    }

    fn copy(
        &self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) -> Result<Option<Self>, ContextError> {
        if let Some(tree) = self.tree.copy(from_key, to_key)? {
            Ok(Some(self.with_tree(tree)))
        } else {
            Ok(None)
        }
    }

    fn find(&self, key: &ContextKey) -> Result<Option<ContextValue>, ContextError> {
        Ok(self.tree.find(key)?)
    }

    fn mem(&self, key: &ContextKey) -> Result<bool, ContextError> {
        Ok(self.tree.mem(key)?)
    }

    fn mem_tree(&self, key: &ContextKey) -> bool {
        self.tree.mem_tree(key)
    }

    fn find_tree(&self, key: &ContextKey) -> Result<Option<WorkingTree>, ContextError> {
        self.tree.find_tree(key).map_err(Into::into)
    }

    fn add_tree(&self, key: &ContextKey, tree: &WorkingTree) -> Result<Self, ContextError> {
        Ok(self.with_tree(self.tree.add_tree(key, tree)?))
    }

    fn empty(&self) -> Self {
        self.with_tree(self.tree.empty())
    }

    fn list(
        &self,
        offset: Option<usize>,
        length: Option<usize>,
        key: &ContextKey,
    ) -> Result<Vec<(String, WorkingTree)>, ContextError> {
        self.tree.list(offset, length, key).map_err(Into::into)
    }

    fn fold_iter(
        &self,
        depth: Option<FoldDepth>,
        key: &ContextKey,
    ) -> Result<TreeWalker, ContextError> {
        Ok(self.tree.fold_iter(depth, key)?)
    }

    fn get_merkle_root(&self) -> Result<EntryHash, ContextError> {
        self.tree.hash().map_err(Into::into)
    }
}

impl ShellContextApi for TezedgeContext {
    fn commit(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError> {
        // Entries to be inserted are obtained from the commit call and written here
        let date: u64 = date.try_into()?;
        let mut repository = self.index.repository.write()?;
        let (commit_hash_id, batch, referenced_older_entries) = self.tree.prepare_commit(
            date,
            author,
            message,
            self.parent_commit_hash,
            &mut *repository,
            true,
        )?;
        // FIXME: only write entries if there are any, empty commits should not produce anything
        repository.write_batch(batch)?;
        repository.put_context_hash(commit_hash_id)?;
        repository.block_applied(referenced_older_entries)?;

        let commit_hash = self.get_commit_hash(commit_hash_id, &*repository)?;
        repository.clear_entries()?;

        std::mem::drop(repository);
        let time = self.index.time_clear.load(std::sync::atomic::Ordering::Relaxed);
        println!("TIME_CLEAR={:?}", std::time::Duration::from_nanos(time.try_into().unwrap()));
        self.get_memory_usage();

        Ok(commit_hash)
    }

    fn hash(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError> {
        let date: u64 = date.try_into()?;
        let mut repository = self.index.repository.write()?;

        let (commit_hash_id, _, _) = self.tree.prepare_commit(
            date,
            author,
            message,
            self.parent_commit_hash,
            &mut *repository,
            false,
        )?;

        let commit_hash = self.get_commit_hash(commit_hash_id, &*repository)?;
        repository.clear_entries()?;
        Ok(commit_hash)
    }

    fn get_last_commit_hash(&self) -> Result<Option<Vec<u8>>, ContextError> {
        let repository = self.index.repository.read()?;

        let value = match self.parent_commit_hash {
            Some(hash_id) => repository.get_value(hash_id)?,
            None => return Ok(None),
        };

        Ok(value.map(|v| v.to_vec()))
    }

    fn get_merkle_stats(&self) -> Result<MerkleStoragePerfReport, ContextError> {
        Ok(MerkleStoragePerfReport::default())
    }

    fn get_memory_usage(&self) -> Result<ContextMemoryUsage, ContextError> {
        let repository = self.index.repository.read()?;
        let storage = (&*self.index.storage).borrow();

        let usage = ContextMemoryUsage {
            repo: repository.memory_usage(),
            storage: storage.memory_usage(),
        };

        println!("usage={:?}", usage);

        Ok(usage)

        // println!("REPO={:?}", repository);
        //println!("STORAGE={:?}", );

        // todo!()

        // Ok(ContextMemoryUsage {
        //     storage: repository.memory_usage(),
        //     strings: strings.memory_usage(),
        // })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ContextMemoryUsage {
    repo: RepositoryMemoryUsage,
    storage: StorageMemoryUsage,
}

/// Generator of Tree IDs which are used to simulate pointers when they are not available.
///
/// During a regular use of the context API, contexts that are still in use are kept
/// alive by pointers to them. This is not available when for example, running the context
/// actions replayer tool. To solve that, we generate a tree id for each versionf of the
/// working tree that is produced while applying a block, so that actions can be associated
/// to the tree to which they are applied.
pub struct TreeIdGenerator(TreeId);

impl TreeIdGenerator {
    fn new() -> Self {
        Self(0)
    }

    fn next(&mut self) -> TreeId {
        self.0 += 1;
        self.0
    }
}

impl TezedgeContext {
    // NOTE: only used to start from scratch, otherwise checkout should be used
    pub fn new(
        index: TezedgeIndex,
        parent_commit_hash: Option<HashId>,
        tree: Option<Rc<WorkingTree>>,
    ) -> Self {
        let tree = if let Some(tree) = tree {
            tree
        } else {
            Rc::new(WorkingTree::new(index.clone()))
        };
        let tree_id_generator = Rc::new(RefCell::new(TreeIdGenerator::new()));
        let tree_id = tree_id_generator.borrow_mut().next();
        Self {
            index,
            parent_commit_hash,
            tree_id,
            tree_id_generator,
            tree,
        }
    }

    /// Produce a new copy of the context, replacing the tree (and if different, with a new tree id)
    pub fn with_tree(&self, tree: WorkingTree) -> Self {
        // TODO: only generate a new id if tree changes? Either that
        // or generate a new one every time for Irmin even if the tree doesn't change
        let tree_id = self.tree_id_generator.borrow_mut().next();
        let tree = Rc::new(tree);
        Self {
            tree,
            tree_id,
            tree_id_generator: Rc::clone(&self.tree_id_generator),
            index: self.index.clone(),
            ..*self
        }
    }

    fn get_commit_hash(
        &self,
        commit_hash_id: HashId,
        repo: &ContextKeyValueStore,
    ) -> Result<ContextHash, ContextError> {
        let commit_hash = match repo.get_hash(commit_hash_id)? {
            Some(hash) => hash,
            None => {
                return Err(MerkleError::EntryNotFound {
                    hash_id: commit_hash_id,
                }
                .into())
            }
        };
        let commit_hash = ContextHash::try_from(&commit_hash[..])?;
        Ok(commit_hash)
    }
}
