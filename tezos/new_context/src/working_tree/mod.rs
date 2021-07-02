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

use self::{tree_storage::{BlobStorageId, TreeStorage, TreeStorageId}, working_tree::MerkleError};

pub mod map;
pub mod serializer;
pub mod string_interner;
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
// pub type Tree = Map<KeyFragment, Node>;
pub type Tree = TreeStorageId;

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;

#[derive(BitfieldSpecifier)]
#[bits = 1]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum NodeKind {
    Leaf,
    NonLeaf,
}

assert_eq_size!(u8, NodeKind);

impl From<u8> for NodeKind {
    fn from(byte: u8) -> Self {
        match byte {
            0 => Self::Leaf,
            1 => Self::NonLeaf,
            _ => panic!(),
        }
    }
}

impl Into<u8> for NodeKind {
    fn into(self) -> u8 {
        match self {
            NodeKind::Leaf => 0,
            NodeKind::NonLeaf => 1
        }
    }
}

// #[bitfield(bits = 34)]
#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct NodeBitfield {
    _unused: B30,
    node_kind: NodeKind,
    commited: bool,
    entry_hash_id: B32,
}

assert_eq_size!(u64, NodeEntry);
assert_eq_size!(u64, NodeBitfield);

impl NodeBitfield {
    pub fn new_with(kind: NodeKind, hash_id: HashId) -> Self {
        Self::new()
            .with_entry_hash_id(hash_id.as_usize().try_into().unwrap())
            .with_node_kind(kind)
            .with_commited(false)
    }
}

#[derive(BitfieldSpecifier)]
#[bits = 2]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum NodeEntryKind {
    Tree,
    Blob,
    None,
}

#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct NodeEntry {
    entry_kind: NodeEntryKind,
    entry_id: B62,
}

assert_eq_size!(u64, NodeEntry);

impl NodeEntry {
    pub fn new_none() -> Self {
        Self::new().with_entry_kind(NodeEntryKind::None)
    }

    fn get_entry(self) -> Option<Entry> {
        match self.entry_kind() {
            NodeEntryKind::Tree => {
                Some(Entry::Tree(TreeStorageId::from(self)))
            }
            NodeEntryKind::Blob => {
                Some(Entry::Blob(BlobStorageId::from(self)))
            }
            NodeEntryKind::None => {
                None
            }
        }
    }
}

impl Default for NodeEntry {
    fn default() -> Self {
        Self::new().with_entry_kind(NodeEntryKind::None)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    pub(crate) bitfield: Cell<NodeBitfield>,
    pub(crate) entry: Cell<NodeEntry>
}

assert_eq_size!(u128, Node);
assert_eq_size!([u8; 16], Node);

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
    Blob(BlobStorageId),
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

    fn hash_id(&self) -> Option<HashId> {
        let id = self.bitfield.get().entry_hash_id();
        HashId::new(id.try_into().ok()?)
    }

    pub fn entry_hash<'a>(
        &self,
        store: &'a mut ContextKeyValueStore,
        tree_storage: &TreeStorage,
    ) -> Result<Cow<'a, EntryHash>, HashingError> {
        let hash_id = self.entry_hash_id(store, tree_storage)?;
        Ok(store
            .get_hash(hash_id)?
            .ok_or_else(|| HashingError::HashIdNotFound { hash_id })?)
    }

    pub fn entry_hash_id(
        &self,
        store: &mut ContextKeyValueStore,
        tree_storage: &TreeStorage,
    ) -> Result<HashId, HashingError> {
        match self.hash_id() {
            Some(hash_id) => Ok(hash_id),
            None => {
                let hash_id = hash_entry(
                    self.entry
                        .get()
                        .get_entry()
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

    pub fn new(node_kind: NodeKind, entry: Entry) -> Self {
        Node {
            bitfield: Cell::new(
                NodeBitfield::new()
                    .with_node_kind(node_kind)
                    .with_commited(false),
            ),
            entry: Cell::new(match entry {
                Entry::Tree(tree_id) => {
                    NodeEntry::new()
                        .with_entry_kind(NodeEntryKind::Tree)
                        .with_entry_id(tree_id.into())
                }
                Entry::Blob(blob_id) => {
                    NodeEntry::new()
                        .with_entry_kind(NodeEntryKind::Blob)
                        .with_entry_id(blob_id.into())
                }
                Entry::Commit(_) => {
                    panic!()
                }
            }),
        }
    }

    pub fn get_hash_id(&self) -> Result<HashId, MerkleError> {
        self.hash_id()
            .ok_or(MerkleError::InvalidState("Missing entry hash"))
    }

    pub fn get_entry(&self) -> Option<Entry> {
        self.entry.get().get_entry()
    }

    pub fn set_entry(&self, entry: &Entry) -> Result<(), MerkleError> {
        let node_entry = match entry {
            Entry::Tree(tree_id) => {
                NodeEntry::new()
                    .with_entry_kind(NodeEntryKind::Tree)
                    .with_entry_id((*tree_id).into())
            }
            Entry::Blob(blob_id) => {
                NodeEntry::new()
                    .with_entry_kind(NodeEntryKind::Blob)
                    .with_entry_id((*blob_id).into())
            }
            Entry::Commit(_) => {
                panic!()
            }
        };

        self.entry.set(node_entry);
        Ok(())
        // self.entry
        //     .try_borrow_mut()
        //     .map_err(|_| MerkleError::InvalidState("The Entry is borrowed more than once"))?
        //     .replace(entry.clone());

        // Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::working_tree::tree_storage::NodeId;
    use crate::working_tree::string_interner::StringId;

    use super::*;

    #[test]
    fn test_node() {
        assert!(!std::mem::needs_drop::<Node>());
        assert!(!std::mem::needs_drop::<NodeId>());
        assert!(!std::mem::needs_drop::<(StringId, NodeId)>());
    }
}
