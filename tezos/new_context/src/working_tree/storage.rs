// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    cmp::Ordering,
    convert::{TryFrom, TryInto},
    mem::size_of,
};

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;
use tezos_timing::StorageMemoryUsage;

use crate::kv_store::entries::Entries;

use super::{
    string_interner::{StringId, StringInterner},
    Node,
};

#[bitfield]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeStorageId {
    /// Note: Must fit in NodeInner.entry_id (61 bits)
    #[skip]
    __unused: B14,
    start: B30,
    length: B20,
}

impl Default for TreeStorageId {
    fn default() -> Self {
        Self::empty()
    }
}

impl TreeStorageId {
    fn try_new_tree(start: usize, end: usize) -> Result<Self, StorageIdError> {
        let length = end
            .checked_sub(start)
            .ok_or(StorageIdError::TreeInvalidStartEnd)?;

        if start & !0x3FFFFFFF != 0 {
            // Must fit in 30 bits
            return Err(StorageIdError::TreeStartTooBig);
        }

        if length & !0xFFFFF != 0 {
            // Must fit in 20 bits
            return Err(StorageIdError::TreeLengthTooBig);
        }

        let tree_id = Self::new()
            .with_start(start as u32)
            .with_length(length as u32);

        debug_assert_eq!(tree_id.get(), (start as usize, end));

        Ok(tree_id)
    }

    fn get(self) -> (usize, usize) {
        let start = self.start() as usize;
        let length = self.length() as usize;

        (start, start + length)
    }

    pub fn empty() -> Self {
        // Never fails
        Self::try_new_tree(0, 0).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.length() == 0
    }
}

impl From<TreeStorageId> for u64 {
    fn from(tree_id: TreeStorageId) -> Self {
        let bytes = tree_id.into_bytes();
        u64::from_ne_bytes(bytes)
    }
}

impl From<u64> for TreeStorageId {
    fn from(entry_id: u64) -> Self {
        Self::from_bytes(entry_id.to_ne_bytes())
    }
}

#[derive(Debug)]
pub enum StorageIdError {
    BlobSliceTooBig,
    BlobStartTooBig,
    BlobLengthTooBig,
    TreeInvalidStartEnd,
    TreeStartTooBig,
    TreeLengthTooBig,
    NodeIdError,
    StringNotFound,
    TreeNotFound,
    BlobNotFound,
    NodeNotFound,
}

impl From<NodeIdError> for StorageIdError {
    fn from(_: NodeIdError) -> Self {
        Self::NodeIdError
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobStorageId {
    /// Note: Must fit in NodeInner.entry_id (61 bits)
    ///
    /// | 3 bits  | 1 bit     | 60 bits |
    /// |---------|-----------|---------|
    /// | empty   | is_inline | value   |
    ///
    /// value inline:
    /// | 4 bits | 56 bits |
    /// |--------|---------|
    /// | length | value   |
    ///
    /// value not inline:
    /// | 32 bits | 28 bits |
    /// |---------|---------|
    /// | start   | length  |
    bits: u64,
}

impl From<BlobStorageId> for u64 {
    fn from(blob_id: BlobStorageId) -> Self {
        blob_id.bits
    }
}

impl From<u64> for BlobStorageId {
    fn from(entry: u64) -> Self {
        Self { bits: entry }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BlobRef {
    Inline { length: u8, value: [u8; 7] },
    Ref { start: usize, end: usize },
}

impl BlobStorageId {
    fn try_new_inline(value: &[u8]) -> Result<Self, StorageIdError> {
        let len = value.len();

        // Inline values are 7 bytes maximum
        if len > 7 {
            return Err(StorageIdError::BlobSliceTooBig);
        }

        // We copy the slice into an array so we can use u64::from_ne_bytes
        let mut new_value: [u8; 8] = [0; 8];
        new_value[..len].copy_from_slice(value);
        let value = u64::from_ne_bytes(new_value);

        let blob_id = Self {
            bits: (1 << 60) | (len as u64) << 56 | value,
        };

        debug_assert_eq!(
            blob_id.get(),
            BlobRef::Inline {
                length: len.try_into().unwrap(),
                value: new_value[..7].try_into().unwrap()
            }
        );

        Ok(blob_id)
    }

    fn try_new(start: usize, end: usize) -> Result<Self, StorageIdError> {
        let length = end - start;

        if start & !0xFFFFFFFF != 0 {
            // Start must fit in 32 bits
            return Err(StorageIdError::BlobStartTooBig);
        }

        if length & !0xFFFFFFF != 0 {
            // Length must fit in 28 bits
            return Err(StorageIdError::BlobLengthTooBig);
        }

        let blob_id = Self {
            bits: (start as u64) << 28 | length as u64,
        };

        debug_assert_eq!(blob_id.get(), BlobRef::Ref { start, end });

        Ok(blob_id)
    }

    fn get(self) -> BlobRef {
        if self.is_inline() {
            let length = ((self.bits >> 56) & 0xF) as u8;

            // Extract the inline value and make it a slice
            let value: u64 = self.bits & 0xFFFFFFFFFFFFFF;
            let value: [u8; 8] = value.to_ne_bytes();
            let value: [u8; 7] = value[..7].try_into().unwrap(); // Never fails, `value` is [u8; 8]

            BlobRef::Inline { length, value }
        } else {
            let start = (self.bits >> 28) as usize;
            let length = (self.bits & 0xFFFFFFF) as usize;

            BlobRef::Ref {
                start,
                end: start + length,
            }
        }
    }

    pub fn is_inline(self) -> bool {
        self.bits >> 60 != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeId(u32);

#[derive(Debug)]
pub struct NodeIdError;

impl TryInto<usize> for NodeId {
    type Error = NodeIdError;

    fn try_into(self) -> Result<usize, Self::Error> {
        Ok(self.0 as usize)
    }
}

impl TryFrom<usize> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        value.try_into().map(NodeId).map_err(|_| NodeIdError)
    }
}

/// `Storage` contains all the data from the working tree.
///
/// This is where all trees/blobs/strings are allocated.
/// The working tree only has access to ids which refer to data inside `Storage`.
///
/// Because `Storage` is for the working tree only, it is cleared before
/// every checkout.
pub struct Storage {
    /// An efficient map `NodeId -> Node`
    nodes: Entries<NodeId, Node>,
    /// Concatenation of all trees in the working tree.
    /// The working tree has `TreeStorageId` which refers to a subslice of this
    /// vector `trees`
    trees: Vec<(StringId, NodeId)>,
    /// Temporary tree, this is used to avoid allocations when we
    /// manipulate `trees`
    /// For example, `Storage::insert` will create a new tree in `temp_tree`, once
    /// done it will copy that tree from `temp_tree` into the end of `trees`
    temp_tree: Vec<(StringId, NodeId)>,
    /// Concatenation of all blobs in the working tree.
    /// The working tree has `BlobStorageId` which refers to a subslice of this
    /// vector `blobs`.
    /// Note that blobs < 8 bytes are not included in this vector `blobs`, such
    /// blob is directly inlined in the `BlobStorageId`
    blobs: Vec<u8>,
    /// Concatenation of all strings in the working tree.
    /// The working tree has `StringId` which refers to a data inside `StringInterner`.
    strings: StringInterner,
}

#[derive(Debug)]
pub enum Blob<'a> {
    Inline { length: u8, value: [u8; 7] },
    Ref { blob: &'a [u8] },
}

impl<'a> AsRef<[u8]> for Blob<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Blob::Inline { length, value } => &value[..*length as usize],
            Blob::Ref { blob } => blob,
        }
    }
}

impl<'a> std::ops::Deref for Blob<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

assert_eq_size!([u32; 2], (StringId, NodeId));

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            trees: Vec::with_capacity(1024),
            temp_tree: Vec::with_capacity(128),
            blobs: Vec::with_capacity(2048),
            strings: Default::default(),
            nodes: Entries::with_capacity(2048),
        }
    }

    pub fn memory_usage(&self) -> StorageMemoryUsage {
        let nodes_cap = self.nodes.capacity();
        let trees_cap = self.trees.capacity();
        let blobs_cap = self.blobs.capacity();
        let temp_tree_cap = self.temp_tree.capacity();
        let strings = self.strings.memory_usage();
        let total_bytes = (nodes_cap * size_of::<Node>())
            .saturating_add(trees_cap * size_of::<(StringId, NodeId)>())
            .saturating_add(temp_tree_cap * size_of::<(StringId, NodeId)>())
            .saturating_add(blobs_cap)
            .saturating_add(strings.total_bytes);

        StorageMemoryUsage {
            nodes_len: self.nodes.len(),
            nodes_cap,
            trees_len: self.trees.len(),
            trees_cap,
            temp_tree_cap,
            blobs_len: self.blobs.len(),
            blobs_cap,
            strings,
            total_bytes,
        }
    }

    pub fn get_string_id(&mut self, s: &str) -> StringId {
        self.strings.get_string_id(s)
    }

    pub fn get_str(&self, string_id: StringId) -> Result<&str, StorageIdError> {
        self.strings
            .get(string_id)
            .ok_or(StorageIdError::StringNotFound)
    }

    pub fn add_blob_by_ref(&mut self, blob: &[u8]) -> Result<BlobStorageId, StorageIdError> {
        // Do not consider blobs of length zero as inlined, this never
        // happens when the node is running and fix a serialization issue
        // during testing/fuzzing
        if (1..8).contains(&blob.len()) {
            BlobStorageId::try_new_inline(blob)
        } else {
            let start = self.blobs.len();
            self.blobs.extend_from_slice(blob);
            let end = self.blobs.len();

            BlobStorageId::try_new(start, end)
        }
    }

    pub fn get_blob(&self, blob_id: BlobStorageId) -> Result<Blob, StorageIdError> {
        match blob_id.get() {
            BlobRef::Inline { length, value } => Ok(Blob::Inline { length, value }),
            BlobRef::Ref { start, end } => {
                let blob = match self.blobs.get(start..end) {
                    Some(blob) => blob,
                    None => return Err(StorageIdError::BlobNotFound),
                };
                Ok(Blob::Ref { blob })
            }
        }
    }

    pub fn get_node(&self, node_id: NodeId) -> Result<&Node, StorageIdError> {
        self.nodes.get(node_id)?.ok_or(StorageIdError::NodeNotFound)
    }

    pub fn add_node(&mut self, node: Node) -> Result<NodeId, NodeIdError> {
        self.nodes.push(node).map_err(|_| NodeIdError)
    }

    pub fn get_tree(
        &self,
        tree_id: TreeStorageId,
    ) -> Result<&[(StringId, NodeId)], StorageIdError> {
        let (start, end) = tree_id.get();
        self.trees
            .get(start..end)
            .ok_or(StorageIdError::TreeNotFound)
    }

    #[cfg(test)]
    pub fn get_owned_tree(&self, tree_id: TreeStorageId) -> Option<Vec<(String, Node)>> {
        let (start, end) = tree_id.get();
        let tree = self.trees.get(start..end)?;

        Some(
            tree.iter()
                .flat_map(|t| {
                    let key = self.strings.get(t.0)?;
                    let node = self.nodes.get(t.1).ok()??;
                    Some((key.to_string(), node.clone()))
                })
                .collect(),
        )
    }

    fn find_in_tree(
        &self,
        tree: &[(StringId, NodeId)],
        key: &str,
    ) -> Result<Result<usize, usize>, StorageIdError> {
        let mut error = None;

        let result = tree.binary_search_by(|value| match self.get_str(value.0) {
            Ok(value) => value.cmp(key),
            Err(e) => {
                // Take the error and stop the search
                error = Some(e);
                Ordering::Equal
            }
        });

        if let Some(e) = error {
            return Err(e);
        };

        Ok(result)
    }

    pub fn get_tree_node_id(&self, tree_id: TreeStorageId, key: &str) -> Option<NodeId> {
        let tree = self.get_tree(tree_id).ok()?;
        let index = self.find_in_tree(tree, key).ok()?.ok()?;

        Some(tree[index].1)
    }

    pub fn add_tree(
        &mut self,
        new_tree: &mut Vec<(StringId, NodeId)>,
    ) -> Result<TreeStorageId, StorageIdError> {
        let start = self.trees.len();
        self.trees.append(new_tree);
        let end = self.trees.len();

        TreeStorageId::try_new_tree(start, end)
    }

    /// Use `self.temp_tree` to avoid allocations
    pub fn with_new_tree<F, R>(&mut self, fun: F) -> R
    where
        F: FnOnce(&mut Self, &mut Vec<(StringId, NodeId)>) -> R,
    {
        let mut new_tree = std::mem::take(&mut self.temp_tree);
        new_tree.clear();

        let result = fun(self, &mut new_tree);

        self.temp_tree = new_tree;
        result
    }

    pub fn insert(
        &mut self,
        tree_id: TreeStorageId,
        key_str: &str,
        value: Node,
    ) -> Result<TreeStorageId, StorageIdError> {
        let key_id = self.get_string_id(key_str);
        let node_id = self.nodes.push(value)?;

        self.with_new_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Ok(tree) if !tree.is_empty() => tree,
                _ => {
                    new_tree.push((key_id, node_id));
                    return this.add_tree(new_tree);
                }
            };

            let index = this.find_in_tree(tree, key_str)?;

            match index {
                Ok(found) => {
                    new_tree.extend_from_slice(tree);
                    new_tree[found].1 = node_id;
                }
                Err(index) => {
                    new_tree.extend_from_slice(&tree[..index]);
                    new_tree.push((key_id, node_id));
                    new_tree.extend_from_slice(&tree[index..]);
                }
            }

            this.add_tree(new_tree)
        })
    }

    pub fn remove(
        &mut self,
        tree_id: TreeStorageId,
        key: &str,
    ) -> Result<TreeStorageId, StorageIdError> {
        self.with_new_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Ok(tree) if !tree.is_empty() => tree,
                _ => return Ok(tree_id),
            };

            let index = match this.find_in_tree(tree, key)? {
                Ok(index) => index,
                Err(_) => return Ok(tree_id),
            };

            if index > 0 {
                new_tree.extend_from_slice(&tree[..index]);
            }
            if index + 1 != tree.len() {
                new_tree.extend_from_slice(&tree[index + 1..]);
            }

            this.add_tree(new_tree)
        })
    }

    pub fn clear(&mut self) {
        self.strings.clear();

        if self.blobs.capacity() > 2048 {
            self.blobs = Vec::with_capacity(2048);
        } else {
            self.blobs.clear();
        }

        if self.nodes.capacity() > 4096 {
            self.nodes = Entries::with_capacity(4096);
        } else {
            self.nodes.clear();
        }

        if self.trees.capacity() > 16384 {
            self.trees = Vec::with_capacity(16384);
        } else {
            self.trees.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::working_tree::{Entry, NodeKind::Leaf};

    use super::*;

    #[test]
    fn test_storage() {
        let mut storage = Storage::new();

        let blob_id = storage.add_blob_by_ref(&[1]).unwrap();
        let entry = Entry::Blob(blob_id);

        let blob2_id = storage.add_blob_by_ref(&[2]).unwrap();
        let entry2 = Entry::Blob(blob2_id);

        let node1 = Node::new(Leaf, entry.clone());
        let node2 = Node::new(Leaf, entry2.clone());

        let tree_id = TreeStorageId::empty();
        let tree_id = storage.insert(tree_id, "a", node1.clone()).unwrap();
        let tree_id = storage.insert(tree_id, "b", node2.clone()).unwrap();
        let tree_id = storage.insert(tree_id, "0", node1.clone()).unwrap();

        assert_eq!(
            storage.get_owned_tree(tree_id).unwrap(),
            &[
                ("0".to_string(), node1.clone()),
                ("a".to_string(), node1.clone()),
                ("b".to_string(), node2.clone()),
            ]
        );
    }

    #[test]
    fn test_blob_id() {
        let mut storage = Storage::new();

        let slice1 = &[0xFF, 0xFF, 0xFF];
        let slice2 = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let slice3 = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let slice4 = &[];

        let blob1 = storage.add_blob_by_ref(slice1).unwrap();
        let blob2 = storage.add_blob_by_ref(slice2).unwrap();
        let blob3 = storage.add_blob_by_ref(slice3).unwrap();
        let blob4 = storage.add_blob_by_ref(slice4).unwrap();

        assert!(blob1.is_inline());
        assert!(!blob2.is_inline());
        assert!(blob3.is_inline());
        assert!(!blob4.is_inline());

        assert_eq!(storage.get_blob(blob1).unwrap().as_ref(), slice1);
        assert_eq!(storage.get_blob(blob2).unwrap().as_ref(), slice2);
        assert_eq!(storage.get_blob(blob3).unwrap().as_ref(), slice3);
        assert_eq!(storage.get_blob(blob4).unwrap().as_ref(), slice4);
    }
}