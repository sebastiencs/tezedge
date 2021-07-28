// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{cell::Cell, cmp::Ordering, convert::{TryFrom, TryInto}, mem::size_of, ops::Range};

use static_assertions::assert_eq_size;
use tezos_timing::StorageMemoryUsage;

use crate::hash::index as index_of_key;
use crate::kv_store::{entries::Entries, HashId};

use super::{
    string_interner::{StringId, StringInterner},
    working_tree::MerkleError,
    Node,
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeStorageId {
    /// Note: Must fit in NodeInner.entry_id (61 bits)
    ///
    /// | 3 bits |  1 bit   | 60 bits |
    /// |--------|----------|---------|
    /// | empty  | is_inode | value   |
    ///
    /// value not inode:
    /// | 32 bits | 28 bits |
    /// |---------|---------|
    /// | start   | length  |
    ///
    /// value inode:
    /// | 60 bits    |
    /// |------------|
    /// | an InodeId |
    bits: u64,
}

impl Default for TreeStorageId {
    fn default() -> Self {
        Self::empty()
    }
}

// impl From<InodeId> for TreeStorageId {
//     fn from(inode_id: InodeId) -> Self {
//         Self {
//             bits: 1 << 60 | inode_id.0 as u64
//         }
//     }
// }

impl TreeStorageId {
    fn try_new_tree(start: usize, end: usize) -> Result<Self, StorageIdError> {
        let length = end
            .checked_sub(start)
            .ok_or(StorageIdError::TreeInvalidStartEnd)?;

        if start & !0xFFFFFFFF != 0 {
            // Must fit in 32 bits
            return Err(StorageIdError::TreeStartTooBig);
        }

        if length & !0xFFFFFFF != 0 {
            // Must fit in 28 bits
            return Err(StorageIdError::TreeLengthTooBig);
        }

        let tree_id = Self {
            bits: (start as u64) << 28 | length as u64,
        };

        debug_assert_eq!(tree_id.get(), (start as usize, end));

        Ok(tree_id)
    }

    fn try_new_inode(index: usize) -> Result<Self, StorageIdError> {
        if index & !0xFFFFFFFFFFFFFFF != 0 {
            // Must fit in 60 bits
            return Err(StorageIdError::InodeIndexTooBig);
        }

        Ok(Self {
            bits: 1 << 60 | index as u64,
        })
    }

    pub fn is_inode(&self) -> bool {
        self.bits >> 60 != 0
    }

    // pub fn as_inode_id(&self) -> InodeId {
    //     assert!(self.is_inode());

    //     InodeId(self.get_inode_index() as u32)
    // }

    pub fn get_inode_id(self) -> Option<InodeId> {
        if self.is_inode() {
            Some(InodeId(self.get_inode_index() as u32))
        } else {
            None
        }
    }

    // pub fn from_inode_id(inode_id: InodeId) -> Self {
    //     Self {
    //         bits: 1 << 60 | inode_id.0 as u64
    //     }
    // }

    fn get(self) -> (usize, usize) {
        debug_assert!(!self.is_inode());

        let start = (self.bits as usize) >> 28;
        let length = (self.bits as usize) & 0xFFFFFFF;

        (start, start + length)
    }

    fn len(self) -> usize {
        debug_assert!(!self.is_inode());

        (self.bits as usize) & 0xFFFFFFF
    }

    fn get_inode_index(self) -> usize {
        debug_assert!(self.is_inode());

        (self.bits & 0xFFFFFFFFFFFFFFF) as usize
    }

    pub fn empty() -> Self {
        // Never fails
        Self::try_new_tree(0, 0).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        if self.is_inode() {
            return false;
        }

        let length = (self.bits as usize) & 0xFFFFFFF;
        length == 0
    }
}

impl From<TreeStorageId> for u64 {
    fn from(tree_id: TreeStorageId) -> Self {
        tree_id.bits
        // let bytes = tree_id.into_bytes();
        // u64::from_ne_bytes(bytes)
    }
}

impl From<u64> for TreeStorageId {
    fn from(entry_id: u64) -> Self {
        Self { bits: entry_id }
        // Self::from_bytes(entry_id.to_ne_bytes())
    }
}

impl From<InodeId> for TreeStorageId {
    fn from(inode_id: InodeId) -> Self {
        Self::try_new_inode(inode_id.0 as usize).unwrap()

        // Self::from_bytes(entry_id.to_ne_bytes())
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
    InodeIndexTooBig,
    NodeIdError,
    StringNotFound,
    TreeNotFound,
    BlobNotFound,
    NodeNotFound,
    InodeNotFound,
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

#[derive(Clone, Debug, Copy)]
pub struct InodeId(u32);

#[derive(Clone, Debug)]
pub struct PointerToInode {
    pub hash_id: Cell<Option<HashId>>,
    pub inode: Cell<InodeId>,
}

impl PointerToInode {
    pub fn new(hash_id: Option<HashId>, inode_id: InodeId) -> Self {
        Self {
            hash_id: Cell::new(hash_id),
            inode: Cell::new(inode_id),
        }
    }
}

assert_eq_size!([u8; 12], Option<PointerToInode>);

/// Inode representation used for hashing directories with >256 entries.
#[derive(Clone, Debug)]
pub enum Inode {
    Empty,
    /// Value is a list of (StringId, NodeId)
    Value(TreeStorageId),
    Tree {
        depth: u32,
        nchildren: u32,
        npointers: u8,
        pointers: [Option<PointerToInode>; 32],
    },
}

assert_eq_size!([u8; 400], Inode);

type TempTreeRange = Range<usize>;

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

    inodes: Vec<Inode>,
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
            inodes: Vec::with_capacity(256),
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
        assert!(!tree_id.is_inode());

        let (start, end) = tree_id.get();
        self.trees
            .get(start..end)
            .ok_or(StorageIdError::TreeNotFound)
    }

    #[cfg(test)]
    pub fn get_owned_tree(&self, tree_id: TreeStorageId) -> Option<Vec<(String, Node)>> {
        assert!(!tree_id.is_inode());

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

    // TODO: Change this
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

    pub(super) fn add_inode(&mut self, inode: Inode) -> Result<InodeId, StorageIdError> {
        let current = self.inodes.len();
        self.inodes.push(inode);

        // TODO: Check that current fits in u32
        Ok(InodeId(current as u32))
    }

    /// Copy tree from `Self::temp_tree` into `Self::trees` in a sorted order.
    ///
    /// `tree_range` is the range of the tree in `Self::temp_tree`
    fn copy_sorted(&mut self, tree_range: TempTreeRange) -> Result<TreeStorageId, StorageIdError> {
        let start = self.trees.len();

        for (key_id, node_id) in &self.temp_tree[tree_range] {
            let key_str = self.get_str(*key_id)?;
            let tree = &self.trees[start..];
            let index = self.find_in_tree(tree, key_str)?;

            match index {
                Ok(found) => {
                    self.trees[start + found].1 = *node_id;
                }
                Err(index) => {
                    self.trees.insert(start + index, (*key_id, *node_id));
                }
            }
        }

        let end = self.trees.len();
        TreeStorageId::try_new_tree(start, end)
    }

    fn with_temp_tree_range<Fun>(&mut self, mut fun: Fun) -> Result<TempTreeRange, StorageIdError>
    where
        Fun: FnMut(&mut Self) -> Result<(), StorageIdError>,
    {
        let start = self.temp_tree.len();
        fun(self)?;
        let end = self.temp_tree.len();

        Ok(TempTreeRange { start, end })
    }

    fn create_inode(
        &mut self,
        depth: u32,
        tree_range: TempTreeRange,
    ) -> Result<InodeId, StorageIdError> {
        let tree_range_len = tree_range.end - tree_range.start;

        if tree_range_len == 0 {
            self.add_inode(Inode::Empty)
        } else if tree_range_len <= 32 {
            // The tree in `tree_range` is not guaranted to be sorted.
            // We use `Self::copy_sorted` to copy that tree in `Storage::trees` in
            // a sorted order.

            let new_tree_id = self.copy_sorted(tree_range)?;

            self.add_inode(Inode::Value(new_tree_id))
        } else {
            let nchildren = tree_range_len as u32;
            let mut pointers: [Option<PointerToInode>; 32] = Default::default();
            let mut npointers = 0;

            for index in 0..32u8 {
                let range = self.with_temp_tree_range(|this| {
                    for i in tree_range.clone() {
                        let (key_id, node_id) = this.temp_tree[i];
                        let key = this.get_str(key_id)?;
                        if index_of_key(depth, key) as u8 == index {
                            this.temp_tree.push((key_id, node_id));
                        }
                    }
                    Ok(())
                })?;

                if range.is_empty() {
                    continue;
                }

                npointers += 1;
                let inode_id = self.create_inode(depth + 1, range)?;

                pointers[index as usize] = Some(PointerToInode {
                    hash_id: Cell::new(None),
                    inode: Cell::new(inode_id),
                })
            }

            self.add_inode(Inode::Tree {
                depth,
                nchildren,
                npointers,
                pointers,
            })
        }
    }

    fn insert_tree_single_node(
        &mut self,
        key_id: StringId,
        node: Node,
    ) -> Result<TempTreeRange, StorageIdError> {
        let node_id = self.nodes.push(node)?;

        self.with_temp_tree_range(|this| {
            this.temp_tree.push((key_id, node_id));
            Ok(())
        })
    }

    fn copy_tree_in_temp_tree(
        &mut self,
        tree_id: TreeStorageId,
    ) -> Result<TempTreeRange, StorageIdError> {
        let (tree_start, tree_end) = tree_id.get();

        self.with_temp_tree_range(|this| {
            this.temp_tree
                .extend_from_slice(&this.trees[tree_start..tree_end]);
            Ok(())
        })
    }

    fn insert_inode(
        &mut self,
        depth: u32,
        inode_id: InodeId,
        key: &str,
        key_id: StringId,
        node: Node,
    ) -> Result<InodeId, StorageIdError> {
        let inode = self.get_inode(inode_id)?;

        match inode {
            Inode::Empty => {
                let tree_id = self.insert_tree_single_node(key_id, node)?;
                self.create_inode(depth, tree_id)
            }
            Inode::Value(tree_id) => {
                let tree_id = *tree_id;
                let node_id = self.add_node(node)?;

                // Copy the existing values in `Self::temp_tree` to create an inode
                let range = self.with_temp_tree_range(|this| {
                    this.copy_tree_in_temp_tree(tree_id)?;
                    this.temp_tree.push((key_id, node_id));
                    Ok(())
                })?;

                self.create_inode(depth, range)
            }
            Inode::Tree {
                depth,
                nchildren,
                mut npointers,
                pointers,
            } => {
                let mut pointers = pointers.clone();
                let nchildren = *nchildren;
                let depth = *depth;

                let index_at_depth = index_of_key(depth, key) as usize;

                let inode_id = if let Some(pointer) = &pointers[index_at_depth] {
                    let inode_id = pointer.inode.get();
                    self.insert_inode(depth + 1, inode_id, key, key_id, node)?
                } else {
                    npointers += 1;
                    let new_tree_id = self.insert_tree_single_node(key_id, node)?;
                    self.create_inode(depth, new_tree_id)?
                };

                pointers[index_at_depth] = Some(PointerToInode {
                    hash_id: Cell::new(None),
                    inode: Cell::new(inode_id),
                });

                self.add_inode(Inode::Tree {
                    depth,
                    nchildren: nchildren + 1,
                    npointers,
                    pointers,
                })
            }
        }
    }

    fn iter_inodes_recursive<Fun>(
        &self,
        inode: &Inode,
        fun: &mut Fun
    ) -> Result<(), MerkleError>
    where
        Fun: FnMut(&(StringId, NodeId)) -> Result<(), MerkleError>,
    {
        match inode {
            Inode::Tree { pointers, .. } => {
                for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                    let inode_id = pointer.inode.get();
                    let inode = self.get_inode(inode_id)?;
                    self.iter_inodes_recursive(inode, fun)?;
                }
            }
            Inode::Value(tree_id) => {
                let tree = self.get_tree(*tree_id)?;
                for elem in tree {
                    fun(elem)?;
                }
            }
            Inode::Empty => {}
        };

        Ok(())
    }

    pub fn tree_iterate_unsorted<Fun>(
        &self,
        tree_id: TreeStorageId,
        mut fun: Fun,
    ) -> Result<(), MerkleError>
    where
        Fun: FnMut(&(StringId, NodeId)) -> Result<(), MerkleError>,
    {
        if let Some(inode_id) = tree_id.get_inode_id() {
            let inode = self.inodes.get(inode_id.0 as usize).unwrap();

            self.iter_inodes_recursive(inode, &mut fun)?;
        } else {
            let tree = self.get_tree(tree_id)?;
            for elem in tree {
                fun(elem)?;
            }
        }
        Ok(())
    }

    pub fn tree_len(&self, tree_id: TreeStorageId) -> usize {
        if let Some(inode_id) = tree_id.get_inode_id() {
            let inode = self.inodes.get(inode_id.0 as usize).unwrap();

            match inode {
                Inode::Tree {
                    nchildren: children,
                    ..
                } => *children as usize,
                _ => panic!("root is always Inode::Tree"),
            }
        } else {
            tree_id.len()
        }
    }

    /// Make a vector of `(StringId, NodeId)`
    ///
    /// The vector won't be sorted by their `StringId` for tree > 256
    pub fn tree_to_vec_unsorted(&self, tree_id: TreeStorageId) -> Vec<(StringId, NodeId)> {
        let mut vec = Vec::with_capacity(self.tree_len(tree_id));

        self.tree_iterate_unsorted(tree_id, |&(key_id, node_id)| {
            vec.push((key_id, node_id));
            Ok(())
        })
        .unwrap();

        vec
    }

    pub fn get_inode(&self, inode_id: InodeId) -> Result<&Inode, StorageIdError> {
        self.inodes
            .get(inode_id.0 as usize)
            .ok_or(StorageIdError::InodeNotFound)
    }

    pub fn insert(
        &mut self,
        tree_id: TreeStorageId,
        key_str: &str,
        node: Node,
    ) -> Result<TreeStorageId, StorageIdError> {
        let key_id = self.get_string_id(key_str);

        // Are we inserting in an Inode ?
        if let Some(inode_id) = tree_id.get_inode_id() {
            let inode = self.insert_inode(0, inode_id, key_str, key_id, node)?;
            self.temp_tree.clear();
            return TreeStorageId::try_new_inode(inode.0 as usize);
        }

        let node_id = self.nodes.push(node)?;

        let tree_id = self.with_new_tree(|this, new_tree| {
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
        })?;

        // We only check at the end of this function if the new tree length is > 256
        // because inserting an element in a tree of length 256 doesn't necessary mean
        // that the resulting tree will have a length of 257 (if the key already exist).
        if tree_id.len() <= 256 {
            Ok(tree_id)
        } else {
            // Copy the new tree in `Self::temp_tree`.
            let range = self.copy_tree_in_temp_tree(tree_id)?;
            // Remove the newly created tree from `Self::trees` to save memory.
            // It won't be used anymore as we're creating an inode.
            self.trees.truncate(self.trees.len() - tree_id.len());

            let inode_id = self.create_inode(0, range)?;
            self.temp_tree.clear();

            TreeStorageId::try_new_inode(inode_id.0 as usize)
        }
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
    fn test_sort() {
        let mut storage = Storage::new();

        let blob_id = storage.add_blob_by_ref(&[1]).unwrap();
        let entry = Entry::Blob(blob_id);

        let blob2_id = storage.add_blob_by_ref(&[2]).unwrap();
        let entry2 = Entry::Blob(blob2_id);

        let node1 = Node::new(Leaf, entry.clone());

        let mut tree_id = TreeStorageId::empty();
        for i in 0..300 {
            tree_id = storage
                .insert(tree_id, i.to_string().as_str(), node1.clone())
                .unwrap();
        }

        let tree = storage.tree_to_vec_unsorted(tree_id);

        let tree: Vec<_> = tree
            .iter()
            .map(|i| storage.get_str(i.0).unwrap().to_string())
            .collect();

        println!("RES={:#?}", tree);
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
