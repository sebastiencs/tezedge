// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    borrow::{Borrow, Cow},
    cell::{Cell, RefCell},
    convert::TryInto,
    rc::Rc,
};

use crate::hash::{hash_entry, EntryHash, HashingError};
use crate::ContextValue;
use crate::{kv_store::HashId, ContextKeyValueStore};

use self::{
    tree_storage::{TreeStorage, TreeStorageId},
    working_tree::MerkleError,
};

pub mod map;
pub mod serializer;
pub mod tree_storage;
pub mod working_tree;
pub mod working_tree_stats; // TODO - TE-261 remove or reimplement

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct KeyFragment(pub(crate) Rc<str>);

impl std::ops::Deref for KeyFragment {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for KeyFragment {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<&str> for KeyFragment {
    fn from(value: &str) -> Self {
        KeyFragment(Rc::from(value))
    }
}

impl From<Rc<str>> for KeyFragment {
    fn from(value: Rc<str>) -> Self {
        KeyFragment(value)
    }
}

// Tree must be an ordered structure for consistent hash in hash_tree.
// The entry names *must* be in lexicographical order, as required by the hashing algorithm.
// Currently immutable OrdMap is used to allow cloning trees without too much overhead.
// pub type Tree = Map<KeyFragment, Rc<Node>>;
pub type Tree = TreeStorageId;

use modular_bitfield::prelude::*;

#[derive(BitfieldSpecifier)]
#[bits = 1]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum NodeKind {
    Leaf,
    NonLeaf,
}

#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct NodeBitfield {
    node_kind: NodeKind,
    commited: bool,
    entry_hash_id: B62,
}

impl NodeBitfield {
    pub fn new_with(kind: NodeKind, hash_id: HashId) -> Self {
        Self::new()
            .with_entry_hash_id(hash_id.as_usize().try_into().unwrap())
            .with_node_kind(kind)
            .with_commited(false)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    pub bitfield: Cell<NodeBitfield>,
    pub entry: RefCell<Option<Entry>>,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Commit {
    pub(crate) parent_commit_hash: Option<HashId>,
    pub(crate) root_hash: HashId,
    pub(crate) time: u64,
    pub(crate) author: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Box<Commit>),
}

impl Node {
    pub fn is_commited(&self) -> bool {
        self.bitfield.get().commited()
    }

    pub fn set_commited(&self, commited: bool) {
        let bitfield = self.bitfield.get().with_commited(commited);
        self.bitfield.set(bitfield);
    }

    pub fn node_kind(&self) -> NodeKind {
        self.bitfield.get().node_kind()
    }

    pub fn hash_id(&self) -> Option<HashId> {
        let id = self.bitfield.get().entry_hash_id();
        HashId::new(id.try_into().ok()?)
    }

    pub fn entry_hash<'a>(
        &self,
        store: &'a mut ContextKeyValueStore,
        tree_storage: &TreeStorage<KeyFragment, Rc<Node>>,
    ) -> Result<Cow<'a, EntryHash>, HashingError> {
        let hash_id = self.entry_hash_id(store, tree_storage)?;
        Ok(store
            .get_hash(hash_id)?
            .ok_or_else(|| HashingError::HashIdNotFound { hash_id })?)
    }

    pub fn entry_hash_id(
        &self,
        store: &mut ContextKeyValueStore,
        tree_storage: &TreeStorage<KeyFragment, Rc<Node>>,
    ) -> Result<HashId, HashingError> {
        match self.hash_id() {
            Some(hash_id) => Ok(hash_id),
            None => {
                let hash_id = hash_entry(
                    self.entry
                        .try_borrow()
                        .map_err(|_| HashingError::EntryBorrow)?
                        .as_ref()
                        .ok_or(HashingError::MissingEntry)?,
                    store,
                    tree_storage,
                )?;
                let mut bitfield = self.bitfield.get();
                bitfield.set_entry_hash_id(hash_id.as_usize().try_into().unwrap());
                self.bitfield.set(bitfield);
                //self.entry_hash.set(Some(hash_id));
                Ok(hash_id)
            }
        }
    }

    pub fn get_hash_id(&self) -> Result<HashId, MerkleError> {
        self.hash_id()
            .ok_or(MerkleError::InvalidState("Missing entry hash"))
    }

    pub fn set_entry(&self, entry: &Entry) -> Result<(), MerkleError> {
        self.entry
            .try_borrow_mut()
            .map_err(|_| MerkleError::InvalidState("The Entry is borrowed more than once"))?
            .replace(entry.clone());

        Ok(())
    }
}
