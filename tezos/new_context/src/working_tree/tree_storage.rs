use std::{borrow::Borrow, cell::RefCell, convert::{TryFrom, TryInto}, num::NonZeroU32};

use static_assertions::assert_eq_size;

use crate::kv_store::entries::Entries;

use super::{Node, NodeEntry, string_interner::{StringId, StringInterner, StringsMemoryUsage}};

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeStorageId {
    /// | 14 bits | 30 bits | 20 bits |
    /// | empty   | start   | length  |
    bits: u64,
}

impl std::fmt::Debug for TreeStorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (start, end) = self.get();

        f.debug_struct("TreeStorageId")
         .field("bits", &format!("{:064b}", self.bits))
         .field("start", &start)
         .field("end", &end)
         .finish()
    }
}

impl Default for TreeStorageId {
    fn default() -> Self {
        Self::empty()
    }
}

impl TreeStorageId {
    fn new(start: usize, end: usize) -> Self {
        let length = end.checked_sub(start).unwrap();

        assert_eq!(start & !0x3FFFFFFF, 0);
        assert_eq!(length & !0xFFFFF, 0);

        let tree_id = Self {
            bits: (start as u64) << 20 | length as u64
        };

        debug_assert_eq!(tree_id.get(), (start, end));

        tree_id
    }

    fn get(self) -> (usize, usize) {
        let start = (self.bits >> 20) as usize;
        let length = (self.bits & 0xFFFFF) as usize;

        (start, start + length)
    }

    pub fn empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        let length = self.bits & 0xFFFFF;
        length == 0
    }
}

impl Into<u64> for TreeStorageId {
    fn into(self) -> u64 {
        self.bits
    }
}

impl From<NodeEntry> for TreeStorageId {
    fn from(entry: NodeEntry) -> Self {
        Self { bits: entry.entry_id() }
    }
}

// #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
// pub struct BlobStorageId {
//     /// | 14 bits | 28 bits | 22 bits |
//     /// | empty   | start   | length  |
//     bits: u64,
// }

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobStorageId {
    /// | 2 bits  | 1 bit     | 61 bits |
    /// | empty   | is_inline | value   |
    ///
    /// value inline:
    /// | 5 bits | 56 bits |
    /// | length | value   |
    ///
    /// value not inline:
    /// | 32 bits | 29 bits |
    /// | start   | length  |
    bits: u64,
}

impl Into<u64> for BlobStorageId {
    fn into(self) -> u64 {
        self.bits
    }
}

impl From<NodeEntry> for BlobStorageId {
    fn from(entry: NodeEntry) -> Self {
        Self { bits: entry.entry_id() }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BlobRef {
    Inline {
        length: u8,
        value: [u8; 8],
    },
    Ref {
        start: usize,
        end: usize,
    }
}

impl BlobStorageId {
    pub fn new_inline(value: &[u8]) -> Self {
        debug_assert!(value.len() < 8);

        let len = value.len();
        let mut new_value: [u8; 8] = [0; 8];

        new_value[..len].copy_from_slice(value);
        let new_value = u64::from_ne_bytes(new_value);

        let blob_id = Self {
            bits: (1 << 61) | (len as u64) << 56 | new_value
        };

        debug_assert_eq!(blob_id.get(), BlobRef::Inline {
            length: len.try_into().unwrap(),
            value: new_value.to_ne_bytes()
        });

        blob_id
    }

    fn new(start: usize, end: usize) -> Self {
        let length = end.checked_sub(start).unwrap();

        debug_assert_eq!(start & !0xFFFFFFF, 0);
        debug_assert_eq!(length & !0x3FFFFF, 0);

        let blob_id = Self {
            bits: (start as u64) << 29 | length as u64
        };

        debug_assert_eq!(blob_id.get(), BlobRef::Ref {
            start, end
        });

        blob_id
    }

    fn get(self) -> BlobRef {
        if self.bits >> 61 != 0 {
            let length = ((self.bits >> 56) & 0x1F) as u8;

            let value: u64 = self.bits & 0xFFFFFFFFFFFFFF;
            //let value: u64 = self.bits & 0xFFFFFFFFFFFFF;
            let value: [u8; 8] = value.to_ne_bytes();

            BlobRef::Inline {
                length,
                value,
            }
        } else {
            let start = (self.bits >> 29) as usize;
            let length = (self.bits & 0x1FFFFFF) as usize;

            BlobRef::Ref {
                start,
                end: start + length
            }
        }
    }

    pub fn is_inline(self) -> bool {
        self.bits >> 61 != 0
    }

    // fn get(self) -> (usize, usize) {
    //     let start = (self.bits >> 22) as usize;
    //     let length = (self.bits & 0x3FFFFF) as usize;

    //     (start, start + length)
    // }
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
        let value: u32 = value.try_into().map_err(|_| NodeIdError)?;
        Ok(NodeId(value))
    }
}

#[derive(Debug)]
pub struct StorageMemoryUsage {
    node_cap: usize,
    trees_cap: usize,
    temp_vec_cap: usize,
    temp_vec_mod_cap: usize,
    values_cap: usize,
    strings: StringsMemoryUsage,
}

pub struct TreeStorage
{
    nodes: Entries<NodeId, Node>,
    trees: Vec<(StringId, NodeId)>,
    temp_vec: Vec<(StringId, Node)>,
    temp_vec_mod: Vec<(StringId, NodeId)>,
    values: Vec<u8>,
    strings: StringInterner,
    // inlined_blob: [u8; 8],
}

#[derive(Debug)]
pub enum Blob<'a> {
    Inline {
        length: u8,
        value: [u8; 8]
    },
    Ref {
        blob: &'a [u8]
    }
}

// impl<'a> std::fmt::Debug for Blob<'a> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("Blob")
//          .field("data", &self.as_ref())
//          .finish()
//     }
// }

impl<'a> AsRef<[u8]> for Blob<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Blob::Inline { length, value } => {
                &value[..*length as usize]
            }
            Blob::Ref { blob } => {
                blob
            }
        }
    }
}

impl<'a> std::ops::Deref for Blob<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

// impl<'a> Blob<'a> {
//     fn to_vec(&self) -> Vec<u8> {
//         self.as_ref().to_vec()
//     }
// }

assert_eq_size!([u32; 2], (StringId, NodeId));

impl Default for TreeStorage
{
    fn default() -> Self {
        Self::new()
    }
}

impl TreeStorage
{
    pub fn new() -> Self {
        Self {
            trees: Vec::with_capacity(1024),
            temp_vec: Vec::with_capacity(128),
            temp_vec_mod: Vec::with_capacity(128),
            values: Vec::with_capacity(2048),
            strings: Default::default(),
            nodes: Entries::with_capacity(2048),
        }
    }

    pub fn memory_usage(&self) -> StorageMemoryUsage {
        StorageMemoryUsage {
            node_cap: self.nodes.capacity(),
            trees_cap: self.trees.capacity(),
            temp_vec_cap: self.temp_vec.capacity(),
            temp_vec_mod_cap: self.temp_vec_mod.capacity(),
            values_cap: self.values.capacity(),
            strings: self.strings.memory_usage(),
        }
    }

    pub fn get_string_id(&mut self, s: &str) -> StringId {
        self.strings.get_string_id(s)
    }

    pub fn get_str(&self, string_id: StringId) -> &str {
        self.strings.get(string_id).unwrap()
    }

    pub fn add_blob_by_ref(&mut self, value: &[u8]) -> BlobStorageId {
        if value.len() < 8 {
            //println!("ADD_BLOB={:?}", value);
            let inline = BlobStorageId::new_inline(value);
            //println!("INLINED={:?}", inline.get());
            inline
        } else {
            let start = self.values.len();
            self.values.extend_from_slice(value);
            let end = self.values.len();

            BlobStorageId::new(start, end)
        }
    }

    pub fn get_blob(&self, blob_id: BlobStorageId) -> Option<Blob> {
        match blob_id.get() {
            BlobRef::Inline { length, value } => {
                Some(Blob::Inline { length, value })
            }
            BlobRef::Ref { start, end } => {
                let blob = self.values.get(start..end)?;
                Some(Blob::Ref { blob })
            }
        }
    }

    pub fn new_tree(&self) -> TreeStorageId {
        TreeStorageId::new(0, 0)
    }

    pub fn get_node(&self, node_id: NodeId) -> Option<&Node> {
        self.nodes.get(node_id).ok()?
    }

    #[cfg(test)]
    pub fn get_own_tree(&self, tree_id: TreeStorageId) -> Option<Vec<(String, Node)>> {
        let (start, end) = tree_id.get();
        let tree = self.trees.get(start..end)?;

        Some(tree.iter().flat_map(|t| {
            let key = self.strings.get(t.0)?;
            let node = self.nodes.get(t.1).ok()??;
            Some((key.to_string(), node.clone()))
        }).collect())
    }

    pub fn get_tree<'a>(&'a self, tree_id: TreeStorageId) -> Option<&[(StringId, NodeId)]> {
        let (start, end) = tree_id.get();
        self.trees.get(start..end)
    }

    pub fn get_tree_node_id<'a>(&'a self, tree_id: TreeStorageId, key: &str) -> Option<NodeId>
    {
        let tree = self.get_tree(tree_id)?;

        let index = tree
            .binary_search_by(|value| {
                let value = self.get_str(value.0);
                value.cmp(key)
            })
            .ok()?;

        Some(tree[index].1)
    }

    fn add_tree_with<F>(&mut self, fun: F) -> TreeStorageId
    where
    F: FnOnce(&mut Vec<(StringId, NodeId)>),
    {
        let start = self.trees.len();
        fun(&mut self.trees);
        let end = self.trees.len();

        TreeStorageId::new(start, end)
    }

    pub fn add_tree_with_result<F, E>(&mut self, fun: F) -> Result<TreeStorageId, E>
    where
        F: FnOnce(&mut StringInterner, &mut Vec<(StringId, Node)>) -> Result<(), E>,
    {
        self.temp_vec.clear();
        let result = fun(&mut self.strings, &mut self.temp_vec);

        let start = self.trees.len();
        for (string_id, node) in &self.temp_vec {
            let node_id = self.nodes.push(node.clone()).unwrap();
            self.trees.push((*string_id, node_id));
        }
        let end = self.trees.len();

        let tree_id = TreeStorageId::new(start, end);
        result.map(|_| tree_id)
    }

    /// Use `self.temp_vec` to avoid 1 allocation in insert/remove
    fn with_temporary_tree<F, R>(&mut self, fun: F) -> R
    where
    F: FnOnce(&mut Self, &mut Vec<(StringId, NodeId)>) -> R,
    {
        let mut new_tree = std::mem::take(&mut self.temp_vec_mod);
        new_tree.clear();

        let result = fun(self, &mut new_tree);

        self.temp_vec_mod = new_tree;
        result
    }

    pub fn insert(&mut self, tree_id: TreeStorageId, key_str: &str, value: Node) -> TreeStorageId
    {
        let key = self.get_string_id(key_str);
        let node_id = self.nodes.push(value).unwrap();

        self.with_temporary_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Some(tree) if !tree.is_empty() => tree,
                _ => {
                    return this.add_tree_with(|trees| {
                        trees.push((key, node_id));
                    });
                }
            };

            let index = tree.binary_search_by(|value| {
                let value = this.get_str(value.0);
                value.cmp(&key_str)
            });

            match index {
                Ok(found) => {
                    new_tree.extend_from_slice(tree);
                    new_tree[found].1 = node_id;
                }
                Err(index) => {
                    new_tree.extend_from_slice(&tree[..index]);
                    new_tree.push((key, node_id));
                    new_tree.extend_from_slice(&tree[index..]);
                }
            }

            this.add_tree_with(|trees| {
                trees.append(new_tree);
            })
        })
    }

    pub fn remove(&mut self, tree_id: TreeStorageId, key: &str) -> TreeStorageId
    {
        self.with_temporary_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Some(tree) if !tree.is_empty() => tree,
                _ => {
                    return tree_id;
                }
            };

            let index = match tree.binary_search_by(|value| {
                let value = this.get_str(value.0);
                value.cmp(key)
            }) {
                Ok(index) => index,
                Err(_) => {
                    return tree_id;
                }
            };

            if index > 0 {
                new_tree.extend_from_slice(&tree[..index]);
            }
            if index + 1 != tree.len() {
                new_tree.extend_from_slice(&tree[index + 1..]);
            }

            this.add_tree_with(|trees| {
                trees.append(new_tree);
            })
        })
    }

    pub fn clear(&mut self) {
        // let now = std::time::Instant::now();
        self.strings.clear();
        // let elapsed = now.elapsed();

        // self.time_clear += elapsed;
        // println!("TIME_CLEAR_STRING={:?}", self.time_clear);

        // unsafe {
        //     self.values.set_len(0);
        //     self.nodes.clear();
        //     self.trees.set_len(0);
        // }

        if self.values.capacity() > 2048 {
            self.values = Vec::with_capacity(2048);
        } else {
            self.values.clear();
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

        // self.values.clear();
        // self.nodes.clear();
        // self.trees.clear();

        // self.trees.clear();
        // self.temp_vec.clear();
        // self.temp_vec_mod.clear();
        // self.values.clear();
        // self.nodes.clear();

        // *self = Self {
        //     trees: Vec::with_capacity(2048),
        //     temp_vec: Vec::with_capacity(2048),
        //     temp_vec_mod: Vec::with_capacity(2048),
        //     values: Vec::with_capacity(2048),
        //     nodes: Entries::with_capacity(2048),
        //     strings: std::mem::take(&mut self.strings),
        // };
    }
}

#[cfg(test)]
mod tests {
    use crate::working_tree::{Entry, NodeKind::Leaf};

    use super::*;

    #[test]
    fn test_tree_storage() {
        println!("ID={:?}", std::mem::size_of::<TreeStorageId>());
        println!(
            "Option<TreeStorageId>={:?}",
            std::mem::size_of::<Option<TreeStorageId>>()
        );
        println!(
            "Option<Option<TreeStorageId>>={:?}",
            std::mem::size_of::<Option<Option<TreeStorageId>>>()
        );

        let mut tree_storage = TreeStorage::new();

        let blob_id = tree_storage.add_blob_by_ref(&[1]);
        let entry = Entry::Blob(blob_id);

        let blob2_id = tree_storage.add_blob_by_ref(&[2]);
        let entry2 = Entry::Blob(blob2_id);

        let node1 = Node::new(Leaf, entry.clone());
        let node2 = Node::new(Leaf, entry2.clone());

        let tree_id = tree_storage.new_tree();
        let tree_id = tree_storage.insert(tree_id, "a", node1.clone());
        let tree_id = tree_storage.insert(tree_id, "b", node2.clone());
        let tree_id = tree_storage.insert(tree_id, "0", node1.clone());

        assert_eq!(
            tree_storage.get_own_tree(tree_id).unwrap(),
            &[("0".to_string(), node1.clone()), ("a".to_string(), node1.clone()), ("b".to_string(), node2.clone()),]
        );
    }

    #[test]
    fn test_blob_id() {
        let mut tree_storage = TreeStorage::new();

        let blob1 = tree_storage.add_blob_by_ref(&[1,2,3]);
        let blob2 = tree_storage.add_blob_by_ref(&[1,2,3,4,5,6,7,8]);
        let blob3 = tree_storage.add_blob_by_ref(&[115,115,115,115,115,115,115]);

        assert_eq!(tree_storage.get_blob(blob1).unwrap().as_ref(), &[1,2,3]);
        assert_eq!(tree_storage.get_blob(blob2).unwrap().as_ref(), &[1,2,3,4,5,6,7,8]);
        assert_eq!(tree_storage.get_blob(blob3).unwrap().as_ref(), &[115,115,115,115,115,115,115]);
    }
}
