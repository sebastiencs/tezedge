use std::{borrow::Cow, cmp::Ordering, collections::{BTreeMap, HashMap, hash_map::DefaultHasher}, convert::TryInto, hash::Hasher, ops::{Index, IndexMut, Range}};

use blake2::VarBlake2b;
use blake2::digest::{InvalidOutputSize, Update, VariableOutput};
use crypto::hash::HashType;

use crate::{context::{ContextKey, ContextKeyValueStore, ContextValue, EntryHash, StringTreeEntry, StringTreeMap}, persistent::Flushable};

use super::{hash::{ENTRY_HASH_LEN, HashingError}, merkle_storage::MerkleError, merkle_storage_stats::{MerklePerfStats, MerkleStoragePerfReport, MerkleStorageStatistics}};

use serde::{Deserialize, Serialize};
use memchr::Memchr;

// struct NodeKey (usize);

// struct Node {
//     child: NodeKey,
//     next_sibling: NodeKey,
// }

// struct NewMerkle {
//     nodes: Vec<Node>,
//     root: NodeKey,
// }

// Calculates hash of commit
// uses BLAKE2 binary 256 length hash function
// hash is calculated as:
// <hash length (8 bytes)><tree hash bytes>
// <length of parent hash (8bytes)><parent hash bytes>
// <time in epoch format (8bytes)
// <commit author name length (8bytes)><commit author name bytes>
// <commit message length (8bytes)><commit message bytes>
pub(crate) fn hash_commit(commit: &Commit) -> Result<EntryHash, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;
    hasher.update(&(ENTRY_HASH_LEN as u64).to_be_bytes());
    hasher.update(&commit.root_hash);

    if commit.parent_commit_hash.is_none() {
        hasher.update(&(0_u64).to_be_bytes());
    } else {
        let parent_commit_hash = commit
            .parent_commit_hash
            .ok_or(HashingError::ValueExpected("parent_commit_hash"))?;
        hasher.update(&(1_u64).to_be_bytes()); // # of parents; we support only 1
        hasher.update(&(parent_commit_hash.len() as u64).to_be_bytes());
        hasher.update(&parent_commit_hash);
    }
    hasher.update(&(commit.time as u64).to_be_bytes());
    hasher.update(&(commit.author.len() as u64).to_be_bytes());
    hasher.update(&commit.author.clone().into_bytes());
    hasher.update(&(commit.message.len() as u64).to_be_bytes());
    hasher.update(&commit.message.clone().into_bytes());

    Ok(hasher.finalize_boxed().as_ref().try_into()?)
}

// Calculates hash of BLOB
// uses BLAKE2 binary 256 length hash function
// hash is calculated as <length of data (8 bytes)><data>
pub(crate) fn hash_blob(blob: &ContextValue) -> Result<EntryHash, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;
    hasher.update(&(blob.len() as u64).to_be_bytes());
    hasher.update(blob);

    Ok(hasher.finalize_boxed().as_ref().try_into()?)
}

fn encode_irmin_node_kind(kind: &NodeKind) -> [u8; 8] {
    match kind {
        NodeKind::NonLeaf => [0, 0, 0, 0, 0, 0, 0, 0],
        NodeKind::Leaf => [255, 0, 0, 0, 0, 0, 0, 0],
    }
}

// hash is calculated as:
// <number of child nodes (8 bytes)><CHILD NODE>
// where:
// - CHILD NODE - <NODE TYPE><length of string (1 byte)><string/path bytes><length of hash (8bytes)><hash bytes>
// - NODE TYPE - leaf node(0xff0000000000000000) or internal node (0x0000000000000000)
fn hash_short_inode(tree: &[(Vec<u8>, TreeNode)]) -> Result<EntryHash, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;

    // Node list:
    //
    // |    8   |     n_1      | ... |      n_k     |
    // +--------+--------------+-----+--------------+
    // |   \k   | prehash(e_1) | ... | prehash(e_k) |

    hasher.update(&(tree.len() as u64).to_be_bytes());

    // Node entry:
    //
    // |   8   |   (LEB128)   |  len(name)  |   8   |   32   |
    // +-------+--------------+-------------+-------+--------+
    // | kind  |  \len(name)  |    name     |  \32  |  hash  |

    for (k, v) in tree {
        // println!("KEY='{:?}'", k);
        hasher.update(encode_irmin_node_kind(&v.node_kind));
        // Key length is written in LEB128 encoding
        leb128::write::unsigned(&mut hasher, k.len() as u64)?;
        hasher.update(k.as_slice());
        hasher.update(&(ENTRY_HASH_LEN as u64).to_be_bytes());
        hasher.update(&v.entry_hash);
    }

    Ok(hasher.finalize_boxed().as_ref().try_into()?)
}

//fn hash_tree(tree: &Tree) -> Result<EntryHash, HashingError> {
fn hash_tree(tree: &[(Vec<u8>, TreeNode)]) -> Result<EntryHash, HashingError> {
    hash_short_inode(tree)
}

// // Calculates hash of tree
// // uses BLAKE2 binary 256 length hash function
// pub(crate) fn hash_tree(tree: &Tree) -> Result<EntryHash, HashingError> {
//     // If there are >256 entries, we need to partition the tree and hash the resulting inode
//     if tree.len() > 256 {
//         let entries: Vec<(&Arc<String>, &Node)> =
//             tree.iter().map(|(s, n)| (s, n.as_ref())).collect();
//         let inode = partition_entries(0, &entries)?;
//         hash_long_inode(&inode)
//     } else {
//         hash_short_inode(tree)
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NodeKind {
    NonLeaf,
    Leaf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TreeNode {
    pub node_kind: NodeKind,
    pub entry_hash: EntryHash,

    // #[serde(serialize_with = "ensure_non_null_entry_hash")]
    // pub entry_hash: RefCell<Option<EntryHash>>,
    // #[serde(skip)]
    // pub entry: RefCell<Option<Entry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Tree<'a> (Cow<'a, [(Vec<u8>, TreeNode)]>);

impl<'a> Tree<'a> {
    fn get(&self, key: &str) -> Option<&TreeNode> {
        match self.0.binary_search_by(|node| {
            node.0.as_slice().cmp(key.as_bytes())
        }) {
            Ok(index) => Some(&self.0[index].1),
            _ => {
                None
            }
        }
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Commit {
    pub(crate) parent_commit_hash: Option<EntryHash>,
    pub(crate) root_hash: EntryHash,
    pub(crate) time: u64,
    pub(crate) author: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry<'a> {
    Tree(Tree<'a>),
    Blob(Cow<'a, ContextValue>),
    Commit(Commit),
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Node {
    key: NodeKey,
    //key: Vec<String>,
    value: Option<ContextValue>,
    hash: Option<EntryHash>,
    removed: bool,
    kind: NodeKind,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct NodeKey {
    data: Vec<u8>,
}

impl std::fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec: Vec<_> = self.data.split(|b| *b == 0).collect();
        let vec: Vec<_> = vec.iter().map(|bytes| String::from_utf8(bytes.to_vec()).unwrap()).collect();
        vec.fmt(f)
    }
}

impl<T> From<T> for NodeKey
where
    T: AsRef<[u8]>
{
    fn from(elem: T) -> Self {
        NodeKey {
            data: elem.as_ref().to_vec()
        }
    }
}

impl NodeKey {
    fn new(key: &[String]) -> Self {
        let key_length = key.len();

        if key_length == 0 {
            return Self { data: vec![] };
        }

        let length = key.iter().fold(0, |acc, b| acc + b.len());
        let mut data = Vec::with_capacity(length + key_length - 1);

        for (index, k) in key.iter().enumerate() {
            data.extend_from_slice(k.as_bytes());
            if index < key_length - 1 {
                data.push(0);
            }
        }

        Self {
            data
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    fn nwords(&self) -> usize {
        let nzeros = Memchr::new(0, &self.data).count();
        if self.data.len() > 0 {
            nzeros + 1
        } else {
            0
        }
    }

    fn get(&self, index: usize) -> Option<&[u8]> {
        match self.data.split(|b| *b == 0).nth(index) {
            Some(elem) if elem.is_empty() => None,
            elem => elem
        }
    }

    fn get_str(&self, index: usize) -> Option<&str> {
        match self.data.split(|b| *b == 0).nth(index).map(|d| std::str::from_utf8(d).unwrap()) {
            Some(elem) if elem.is_empty() => None,
            elem => elem
        }
    }

    fn split_last(&self) -> Option<(&[u8], &[u8])> {
        let last_index = Memchr::new(0, &self.data).last()?;

        Some((&self.data.get(..last_index)?, &self.data.get(last_index + 1..)?))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn hash_slice(&self, range: Range<usize>) -> u64 {
        let slice = self.get_slice(range);

        self.hash(slice)
    }

    fn hash(&self, slice: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(slice);
        hasher.finish()
    }

    fn hash_full(&self) -> u64 {
        self.hash(self.data.as_slice())
    }

    fn get_slice(&self, range: Range<usize>) -> &[u8] {
        // println!("CALLED WITH START={:?} END={:?}", range.start, range.end);
        // let start = range.start;

        if range.start == range.end {
            return &[];
        }

        // let index = 0;
        // let index_start = 0;

        let mut start = 0;
        // let end = None;

        for (index, elem_index) in Memchr::new(0, &self.data).enumerate() {
            let index = index + 1;

            // println!("LOOP START={:?} INDEX={:?} ELEM_INDEX={:?}", start, index, elem_index);

            if index == range.start {
                start = elem_index + 1;
            } else if index == range.end {
                return &self.data[start..elem_index];
            }
        }

        &self.data[start..]

        // for b in &self.data {
        //     if *b == 0 {

        //     }
        // }


        // let a = self.data.split(|b| *b == 0).skip(range.start).take(range.end);

        // match self.data.split(|b| *b == 0).nth(index) {
        //     Some(elem) if elem.is_empty() => None,
        //     elem => elem
        // }

        // None
    }
}

struct Nodes {
    keys: Vec<usize>,
    nodes: slab::Slab<Node>,
}

impl Nodes {
    fn new() -> Nodes {
        Nodes {
            keys: vec![],
            nodes: slab::Slab::new(),
        }
    }

    fn get(&self, index: usize) -> Option<&Node> {
        let key = self.keys.get(index).copied()?;
        self.nodes.get(key)
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut Node> {
        let key = self.keys.get(index).copied()?;
        self.nodes.get_mut(key)
    }

    fn insert(&mut self, index: usize, elem: Node) {
        let key = self.nodes.insert(elem);
        self.keys.insert(index, key);
    }

    fn remove(&mut self, index: usize) {
        let key = match self.keys.get(index).copied() {
            Some(key) => key,
            None => return
        };

        self.keys.remove(index);
        self.nodes.remove(key);
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.keys.clear();
    }

    fn binary_search_by<'a, F>(&'a self, mut fun: F) -> Result<usize, usize>
    where
        F: FnMut(&'a Node) -> Ordering,
    {
        self.keys.binary_search_by(|key| {
            let node = &self.nodes[*key];
            fun(node)
        })
    }

    pub fn retain<F>(&mut self, mut fun: F)
    where
        F: FnMut(&Node) -> bool,
    {
        let nodes = &self.nodes;

        self.keys.retain(|key| {
            let node = &nodes[*key];
            fun(node)
        })
    }

    fn clone_range(&self, start: usize, end: usize) -> Vec<Node> {
        let mut vec = Vec::with_capacity(end - start + 1);

        for index in start..=end {
            let key = self.keys[index];
            let node = &self.nodes[key];
            vec.push(node.clone());
        }

        vec
    }

    fn iter(&self) -> NodesIterator {
        NodesIterator { nodes: self, current: 0 }
    }
}

struct NodesIterator<'a> {
    nodes: &'a Nodes,
    current: usize
}

impl<'a> Iterator for NodesIterator<'a> {
    type Item = &'a Node;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.nodes.keys.get(self.current).copied()?;
        let node = &self.nodes.nodes[key];
        self.current += 1;
        Some(node)
    }
}

impl Index<usize> for Nodes {
    type Output = Node;

    fn index(&self, index: usize) -> &Self::Output {
        let key = self.keys[index];
        &self.nodes[key]
    }
}

impl IndexMut<usize> for Nodes {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let key = self.keys[index];
        &mut self.nodes[key]
    }
}

pub struct NewMerkle {
    nodes: Nodes,
    /// key value storage backend
    db: Box<ContextKeyValueStore>,
    hashes: HashMap<u64, EntryHash>,
    /// Last commit hash
    last_commit_hash: Option<EntryHash>,
}

struct MerkleSerializer<'a> {
    nodes: &'a Nodes,
    last_sibling: usize,
    serialize: bool,
    saved_hashes: HashMap<u64, EntryHash>,
    stack_hashes: Vec<(Vec<u8>, TreeNode)>,
    serialized: Vec<(EntryHash, ContextValue)>,
}

fn display_hash(hash: &[u8]) -> String {
    let mut s = String::new();
    for h in hash {
        s.push_str(&format!("{:X}", h));
    }
    s
}

impl<'a> MerkleSerializer<'a> {

    fn next_child(&self, row: usize, column: usize) -> Option<usize> {
        self.nodes.get(row)?.key.get(column + 1).map(|_| column + 1)

        // let node = self.nodes.get(row)?;
        // println!("LAAA={:?} BYTES={:?} COL={:?}", node.key, node.key.as_bytes(), column + 1);
        // node.key.get(column + 1).map(|_| column + 1)
        //self.nodes.get(row)?.key.get(column + 1).map(|_| column + 1)
    }

    // fn next_sibling(&self, row: usize, column: usize, depth: usize) -> Option<usize> {
    //     let current = self.nodes.get(row)?.key.get(..=column)?;

    //     loop {
    //         let row = row + 1;
    //         let sibling = self.nodes.get(row)?.key.get(..=column)?;
    //         if sibling.get(..sibling.len() - 1) == current.get(..current.len() - 1) {
    //             if sibling.last() != current.last() {
    //                 return Some(row);
    //             }
    //         } else {
    //             return None;
    //         }
    //     }
    // }

    fn next_sibling(&self, row: usize, column: usize, depth: usize) -> Option<usize> {
        //let current = self.nodes.get(row)?.key.get(..=column)?;
        //let current = self.nodes.get(row)?.key.get_slice(0..column - 1)?;

        // println!("next_sibling", );

        // let (current_path, current_file) = self.nodes.get(row)?.key.split_last()?;

        let current = self.nodes.get(row)?;
        let current_path = current.key.get_slice(0..column);
        let current_file = current.key.get(column);

        // println!("{}CURRENT PATH={:?} FILE={:?} FULL={:?}", " ".repeat(depth), current_path, current_file, &current.key);

        // let mut row = row + 1;

        // loop {

        for row in row + 1.. {

            let sibling = self.nodes.get(row)?;
            let sibling_path = sibling.key.get_slice(0..column);
            let sibling_file = sibling.key.get(column);

            // println!(
            //     "{}SIBLING PATH={:?} FILE={:?} CURRENT PATH={:?} FILE={:?} ROW={:?} COL={:?} FULL_SIB={:?} FULL_CURRENT={:?}",
            //     " ".repeat(depth), sibling_path, sibling_file, current_path, current_file, row, column,
            //     sibling.key.as_bytes(), current.key.as_bytes()
            // );

            // let (sibling_path, sibling_file) = self.nodes.get(row)?.key.split_last()?;

            // let sibling = self.nodes.get(row)?.key.get(..=column)?;
            if sibling_path == current_path {
                // println!("SAME {:?} {:?}", sibling_path, current_path);
                if sibling_file != current_file {
                    // println!("OK", );
                    return Some(row);
                }
            } else {
                // println!("NOT SAME {:?} {:?}", sibling_path, current_path);
                return None;
            }
            // row += 1;
        }

        None
    }

    fn recursive(&mut self, row: usize, column: usize, depth: usize) -> usize {
        // println!("{}CALLED ROW={} COL={} {:?} DEPTH={}", " ".repeat(depth), row, column, &self.nodes[row].key, depth);

        let mut row = row;
        let mut nentries = 1;

        // TODO: Skip when we already have the hash of the tree

        if let Some(new_column) = self.next_child(row, column) {
            // println!("{}TREE1={}", " ".repeat(depth), self.nodes[row].key.get_str(column).unwrap());

            let node = &self.nodes[row];
            let hash_slice = node.key.hash_slice(0..column + 1);
            if let Some(hash) = self.saved_hashes.get(&hash_slice).copied() {
                // println!("FOUND TREE", );
                self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode {
                    node_kind: NodeKind::NonLeaf,
                    entry_hash: hash,
                }));
            } else {
                // println!("RECOMPUTE TREE", );
                let tree_nentries = self.recursive(row, new_column, depth + 1);
                self.process_tree(&self.nodes[row], column, tree_nentries);
            }

            // let tree_nentries = self.recursive(row, new_column, depth + 1);
            // self.process_tree(&self.nodes[row], column, tree_nentries);
        } else {
            if self.nodes.get(row).is_none() {
                return 0;
            }

            let hash = self.process_leaf(&self.nodes[row], column);

            // println!("{}LEAF1 {:?} HASH={:?}", " ".repeat(depth), self.nodes[row].key.get_str(column), display_hash(&hash));
        }

        // println!("GO TO SIBLING", );

        while let Some(sibling) = self.next_sibling(row.max(self.last_sibling), column, depth) {
            // println!("{}FOUND SIBLING row={} col={} current_row={} {:?}", " ".repeat(depth), sibling, column, row, self.nodes[sibling].key.get_str(column));
            self.last_sibling = sibling;

            nentries += 1;

            if let Some(new_col) = self.next_child(sibling, column) {
                // println!("{}TREE2={}", " ".repeat(depth), self.nodes[sibling].key.get_str(column).unwrap());

                let node = &self.nodes[sibling];
                let hash_slice = node.key.hash_slice(0..column + 1);
                if let Some(hash) = self.saved_hashes.get(&hash_slice).copied() {
                    self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode {
                        node_kind: NodeKind::NonLeaf,
                        entry_hash: hash,
                    }));
                } else {
                    let tree_nentries = self.recursive(sibling, new_col, depth + 1);
                    self.process_tree(&self.nodes[sibling], column, tree_nentries);
                }

                // let tree_nentries = self.recursive(sibling, new_col, depth + 1);
                // self.process_tree(&self.nodes[sibling], column, tree_nentries);
            } else {
                let hash = self.process_leaf(&self.nodes[sibling], column);

                // println!("{}LEAF2 {:?} HASH={:?}", " ".repeat(depth), self.nodes[sibling].key.get_str(column), display_hash(&hash));
            }
            row = sibling;
        }

        // println!("DONE WITH THIS", );

        nentries
    }

    fn start_recursive(mut self) -> (EntryHash, Vec<(EntryHash, ContextValue)>, HashMap<u64, EntryHash>) {
        self.recursive(0, 0, 0);

        // println!("REMAINING={:}", self.hashes.len());

        // let tree = Tree(hashes);
        let hash_tree = hash_tree(&self.stack_hashes).unwrap();
        // println!("ROOT_HASH={:?}", display_hash(&hash_tree));

        if self.serialize {
            self.serialized.push((hash_tree, bincode::serialize(&Entry::Tree(Tree(Cow::Borrowed(&self.stack_hashes)))).unwrap()));
        }

        (hash_tree, self.serialized, self.saved_hashes)

        // let mut row = 0;

        // while let Some(child) = self.next_sibling(row, 0, 0) {
        //     println!("## CALLED NEW REC AT {}", child);
        //     self.recursive(child, 0, 0);
        //     row = child;
        // }
    }

    fn new(nodes: &Nodes, saved_hashes: HashMap<u64, EntryHash>, serialize: bool) -> MerkleSerializer {
        MerkleSerializer {
            nodes,
            saved_hashes,
            serialize,
            last_sibling: 0,
            stack_hashes: vec![],
            serialized: vec![],
        }
    }

    // fn process_leaf(
    //     &mut self,
    //     node: &Node,
    //     column: usize,
    // ) -> EntryHash {
    //     let entry_hash = match node.hash {
    //         Some(hash) => hash,
    //         None => {
    //             let entry_hash = hash_blob(&node.value.as_ref().unwrap()).unwrap();
    //             self.serialized.push((entry_hash, bincode::serialize(&Entry::Blob(Cow::Borrowed(node.value.as_ref().unwrap()))).unwrap()));
    //             entry_hash
    //         }
    //     };

    //     self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode { entry_hash, node_kind: node.kind.clone() }));

    //     entry_hash
    // }

    fn process_leaf(
        &mut self,
        node: &Node,
        column: usize,
    ) -> EntryHash {
        //println!("NODE KEY={:?} SLICE={:?} COL={:?}", node.key, node.key.get_slice(0..column + 1), column);

        let hash_slice = node.key.hash_slice(0..column + 1);

        let hash_leaf = match node.hash.or_else(|| self.saved_hashes.get(&hash_slice).copied()) {
            Some(hash) => {
                // println!("FOUND HASH LEAF {:?}", display_hash(&hash));
                hash
            },
            None => {
                let hash = hash_blob(&node.value.as_ref().unwrap()).unwrap();

                // println!("RECOMPUTE LEAF {:?} {:?}", node.key, display_hash(&hash));

                self.saved_hashes.insert(hash_slice, hash);
                hash
            }
        };

        if self.serialize {
            if let Some(value) = node.value.as_ref() {
                self.serialized.push((hash_leaf, bincode::serialize(&Entry::Blob(Cow::Borrowed(value))).unwrap()));
            } else {
                // println!("MISSING HERE {:?}", node.key);
            }
        }

        // self.serialized.push((hash_leaf, bincode::serialize(&Entry::Blob(Cow::Borrowed(node.value.as_ref().unwrap()))).unwrap()));
        self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode { entry_hash: hash_leaf, node_kind: node.kind.clone() }));

        hash_leaf
    }

    fn process_tree(&mut self, node: &Node, column: usize, tree_entries: usize) {
        let start = self.stack_hashes.len() - tree_entries;
        let hash_slice = node.key.hash_slice(0..column + 1);
        let tree = &self.stack_hashes[start..];

        // println!("TREE {:?}", node.key);
        // println!("GOT HASH={:?}", node.hash.as_ref().map(|a| display_hash(a)));
        // println!("COMPUTED HASH={:?}", display_hash(&hash_tree(tree).unwrap()));

        // let hash_tree = match self.saved_hashes.get(&hash_slice).copied() {
        //     Some(hash) => {
        //         // println!("FOUND HASH TREE {:?}", display_hash(&hash));
        //         hash
        //     },
        //     None => {
        //         // println!("RECOMPUTE TREE {:?}", node.key.get_slice(0..column + 1));
        //         let hash = hash_tree(tree).unwrap();
        //         self.saved_hashes.insert(hash_slice, hash);
        //         hash
        //     }
        // };

        // let hash_tree = match node.hash.or_else(|| self.saved_hashes.get(&hash_slice).copied()) {
        //     Some(hash) => {
        //         // println!("FOUND HASH TREE {:?}", display_hash(&hash));
        //         hash
        //     },
        //     None => {
        //         let hash = hash_tree(tree).unwrap();
        //         self.saved_hashes.insert(hash_slice, hash);
        //         hash
        //     }
        // };

        // let tree = &self.stack_hashes[start..];
        let hash_tree = hash_tree(tree).unwrap();

        // println!("RESOLVED HASH={:?}", self.saved_hashes.get(&hash_slice).as_ref().map(|a| display_hash(&a[..])));

        // println!("{}FULL_HASH_TREE={:?}", " ".repeat(depth), display_hash(&hash_tree));

        if self.serialize {
            self.serialized.push((hash_tree, bincode::serialize(&Entry::Tree(Tree(Cow::Borrowed(tree)))).unwrap()));
        }

        self.stack_hashes.truncate(start);
        self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode {
            node_kind: NodeKind::NonLeaf,
            entry_hash: hash_tree,
        }));
    }

    // fn process_tree(&mut self, node: &Node, column: usize, tree_entries: usize) {
    //     let start = self.stack_hashes.len() - tree_entries;

    //     let tree = &self.stack_hashes[start..];
    //     let hash_tree = hash_tree(tree).unwrap();
    //     // println!("{}FULL_HASH_TREE={:?}", " ".repeat(depth), display_hash(&hash_tree));

    //     self.serialized.push((hash_tree, bincode::serialize(&Entry::Tree(Tree(Cow::Borrowed(tree)))).unwrap()));

    //     self.stack_hashes.truncate(start);
    //     self.stack_hashes.push((node.key.get(column).unwrap().to_vec(), TreeNode {
    //         node_kind: NodeKind::NonLeaf,
    //         entry_hash: hash_tree,
    //     }));
    // }
}

impl NewMerkle {
    pub fn new(db: Box<ContextKeyValueStore>) -> NewMerkle {
        NewMerkle {
            db,
            nodes: Nodes::new(),
            last_commit_hash: None,
            hashes: HashMap::new(),
            // last_sibling: 0,
            // root: NodeKey(0),
        }
    }

// key=["a", "aaa"] value=[97, 98]
// key=["a", "foo", "abc"] value=[97, 98]
// key=["a", "foo", "baa"] value=[97, 98]
// key=["a", "goo"] value=[97, 98]
// key=["b", "abc"] value=[97, 98]
// key=["c", "foo"] value=[3, 4]
// key=["c", "moo"] value=[3, 4]
// key=["c", "zoo"] value=[1, 2]

    fn serialize(&mut self, serialize: bool) -> (EntryHash, Vec<(EntryHash, ContextValue)>) {
        let saved_hashes = std::mem::replace(&mut self.hashes, HashMap::new());

        // (hash_tree, self.serialized, self.saved_hashes)

        let (hash_tree, serialized, saved_hashes) = MerkleSerializer::new(&self.nodes, saved_hashes, serialize)
            .start_recursive();

        self.hashes = saved_hashes;

        (hash_tree, serialized)
    }

    pub fn commit(
        &mut self,
        time: u64,
        author: String,
        message: String,
    ) -> EntryHash {
        println!("AAAAAA", );
        let (root_hash, mut batch) = self.serialize(true);
        let parent_commit_hash = self.last_commit_hash;
        println!("BBBBB", );

        let new_commit = Commit {
            root_hash,
            parent_commit_hash,
            time,
            author,
            message,
        };
        let new_commit_hash = hash_commit(&new_commit).unwrap();

        batch.push((new_commit_hash, bincode::serialize(&Entry::Commit(new_commit)).unwrap()));
        self.db.write_batch(batch).unwrap();

        self.last_commit_hash = Some(new_commit_hash);

        // println!("COMMIT_HASH={:?}", display_hash(&new_commit_hash));

        new_commit_hash
    }

    fn apply_tree_to_root(&mut self, tree: &Tree) {
        let mut index = if self.nodes.is_empty() {
            0
        } else {
            let (key, _) = match tree.0.first() {
                Some(first) => first,
                None => return
            };

            let key = NodeKey::from(key);
            match self.nodes.binary_search_by(|node| {
                node.key.cmp(&key)
            }) {
                Ok(index) => index,
                Err(index) => index,
            }
        };

        for (key, node) in tree.0.iter() {
            let key = NodeKey::from(key);

            // println!("LAAAAAA", );
            self.invalidate_hashes(&key);
            // println!("LAAAAAA DONE", );

            let hash = key.hash_full();
            self.hashes.insert(hash, node.entry_hash);

            self.nodes.insert(index, Node {
                key: NodeKey::from(key),
                value: None,
                hash: Some(node.entry_hash),
                removed: false,
                kind: node.node_kind.clone(),
            });
            index += 1;
        }
    }

    fn apply_tree_to_working_tree(&mut self, prefix: &[u8], tree: &Tree) {
        let mut insert_at = match self.nodes.binary_search_by(|node| {
            node.key.as_bytes().cmp(prefix)
        }) {
            Ok(index) => index,
            Err(_) => {
                println!("ERROR", );
                return;
            },
        };

        self.nodes.remove(insert_at);

        for (key, node) in tree.0.iter() {
            let mut key_prefix = prefix.to_vec();
            key_prefix.push(0);
            key_prefix.extend_from_slice(key.as_slice());
            let key = NodeKey { data: key_prefix };

            // println!("LAAAAAA", );
            self.invalidate_hashes(&key);
            // println!("LAAAAAA2 DONE", );

            let hash = key.hash_full();
            self.hashes.insert(hash, node.entry_hash);

            self.nodes.insert(insert_at, Node {
                key,
                value: None,
                hash: Some(node.entry_hash),
                removed: false,
                kind: node.node_kind.clone(),
            });
            insert_at += 1;
        }
    }

    fn apply_tree_to_working_tree_at(&mut self, mut insert_at: usize, tree: Tree) {
        let prefix = self.nodes[insert_at].key.as_bytes().to_vec();
        self.nodes.remove(insert_at);

        for (key, node) in tree.0.iter() {
            let mut key_prefix = prefix.clone();
            key_prefix.push(0);
            key_prefix.extend_from_slice(key.as_slice());
            let key = NodeKey { data: key_prefix };

            self.invalidate_hashes(&key);

            let hash = key.hash_full();
            self.hashes.insert(hash, node.entry_hash);

            self.nodes.insert(insert_at, Node {
                key,
                value: None,
                hash: Some(node.entry_hash),
                removed: false,
                kind: node.node_kind.clone(),
            });
            insert_at += 1;
        }
    }

    fn display(&self) {
        for node in self.nodes.iter() {
            // if !node.removed {
            //     println!("key={:?} value={:?}", node.key, node.value);
            // }
            println!(
                "key={:?} value={:?} removed={:?} hash={:?}",
                node.key, node.value.as_ref().map(|v| v.len()), node.removed, node.hash.as_ref().map(|h| display_hash(h))
            );
        }
    }

    fn invalidate_hashes(&mut self, key: &NodeKey) {
        for index in 0..key.nwords() {
            //println!("REMOVING HASH FOR {:?}", key.get_slice(0..index + 1).split(|b| *b == 0).map(|s| std::str::from_utf8(s)).collect::<Vec<_>>());
            let hash = key.hash_slice(0..index + 1);
            // let slice = key.get_slice(0..index + 1);
            // let hash = key.hash_full();
            self.hashes.remove(&hash);
        }
    }

    pub fn set(&mut self, key: &ContextKey, value: ContextValue) {
        // let now = std::time::Instant::now();
        // let mut time_clone = None;
        // let mut time_insert = None;

        let key_bytes = NodeKey::new(key);

        let res = self.find_path(&key_bytes);

        // println!("RES={:?}", res);

        // let time_bytes = now.elapsed();

        // self.find_path(&key).is_ok()

        // let res = self.nodes.binary_search_by(|node| {
        //     node.key.cmp(&key_bytes)
        // });

        // let search = now.elapsed() - time_bytes;

        self.invalidate_hashes(&key_bytes);

        let mut index = match res {
            Ok(index_found) => {
                let node = &mut self.nodes[index_found];
                node.value = Some(value);

                node.hash = None;
                index_found
            }
            Err(index_missing) => {
                let key = key_bytes.clone();
                // time_clone = Some(now.elapsed() - search);

                self.nodes.insert(index_missing, Node {
                    key,
                    value: Some(value),
                    hash: None,
                    kind: NodeKind::Leaf,
                    removed: false,
                });

                // time_insert = Some(now.elapsed() - time_clone.unwrap());

                index_missing
            }
        };

        let mut check_after = index + 1;
        let key_len = key.len();

        // let mut bbb = 0;

        while index > 0 {
            let node = &mut self.nodes[index - 1];
            let node_bytes = node.key.as_bytes();
            let len = node.key.nwords();

            // let len = node_bytes.len();
            // if len < key_len && node_bytes == &key[..len] {
            if len < key_len && node_bytes == key_bytes.get_slice(0..len) {
                node.removed = true;
            } else {
                break;
            }
            index -= 1;
            // bbb += 1;
        }

        // let mut aaa = 0;

        loop {
            let node = match self.nodes.get_mut(check_after) {
                Some(node) => node,
                None => {
                    // println!("AFTER SET {:?}", key);
                    // self.display();
                    return;
                }
            };
            let node_bytes = node.key.get_slice(0..key_len);
            let len = node.key.nwords();
            // let len = node.key.len();
            if len > key_len && node_bytes == key_bytes.as_bytes() {
                node.removed = true;
            } else {
                break;
            }
            check_after += 1;
            // aaa += 1;
        }

        // println!("AFTER SET {:?}", key);
        // self.display();

        // println!(
        //     "SET SEARCH={:?} INSERT={:?} CLONE={:?} BYTES={:?} TOTAL={:?} LA={:?} BBB={:?} LENGTH={:?}",
        //     search, time_insert, time_clone, time_bytes, now.elapsed(), aaa, bbb, self.nodes.len()
        // );
    }

    pub fn get(&mut self, key: &ContextKey) -> Option<ContextValue> {
        println!("GET {:?}", key);
        self.display();

        self.get_impl(key).or_else(|| Some(Vec::new()))
    }

    pub fn get_impl(&mut self, key: &ContextKey) -> Option<ContextValue> {
        let key = NodeKey::new(key);

        self.find_path(&key)
            .ok()
            .and_then(|index| {
                self.nodes[index].value.clone()
            })
    }

    // Find the key in the working tree
    // Fetch the underlying storage if needed
    fn find_path(&mut self, key: &NodeKey) -> Result<usize, usize> {
        loop {
            let res = self.nodes.binary_search_by(|node| {
                node.key.cmp(&key)
            });

            let (index, index_err) = match res {
                Ok(index) => {
                    if self.nodes[index].value.is_some() {
                        return Ok(index)
                    }
                    (index, index)
                }
                Err(index) => {
                    (index.saturating_sub(1), index)
                }
            };

            let entry_hash = {
                let node = match self.nodes.get(index) {
                    Some(node) => node,
                    None => return Err(index_err),
                };

                if node.value.is_some() {
                    return Err(index_err);
                }

                let nwords = node.key.nwords();
                if node.key.as_bytes() != key.get_slice(0..nwords) {
                    return Err(index_err);
                }

                // println!("FOUND HERE prev={:?} key={:?} key={:?} node_key={:?}", node.key.as_bytes(), key.get_slice(0..nwords), key, node.key);

                match node.hash {
                    Some(hash) => hash,
                    None => {
                        let hash = node.key.hash_full();
                        // let hash = key.hash_full();
                        self.hashes.get(&hash).copied().unwrap()
                    }
                }

                // match self.hashes.get(&hash).copied() {
                //     Some(hash) => hash,
                //     None => return Err(index_err)
                // }

                // node.hash.as_ref().unwrap().clone()
            };

            println!("HASH HERE {:?}", display_hash(&entry_hash));

            match self.get_entry_from_hash(&entry_hash).map_err(|_| index)? {
                Entry::Tree(tree) => {
                    println!("FOUND TREE {:?}", tree);
                    let tree = Tree(Cow::Owned(tree.0.into_owned()));
                    self.apply_tree_to_working_tree_at(index, tree);
                }
                Entry::Blob(blob) => {
                    println!("FOUND BLOB {:?}", blob);
                    self.nodes[index].value = Some(blob.into_owned());
                    return Ok(index);
                }
                Entry::Commit(_) => {
                }
            }
        }
    }

    pub fn copy(
        &mut self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) {
        let key_from_bytes = NodeKey::new(from_key);
        let key_to_bytes = NodeKey::new(to_key);
        // self.serialize();

        self.invalidate_hashes(&key_to_bytes);

        let _ = self.find_path(&key_to_bytes);
        let res = self.find_path(&key_from_bytes);

        // let res = self.nodes.binary_search_by(|node| {
        //     node.key.cmp(&key_from_bytes)
        // });

        let mut index = match res {
            Ok(index_found) => {
                // println!("FOUND AT {:?}", index_found);
                index_found
            },
            Err(i) => {
                // println!("NOT FOUND AT {:?} WITH NODE={:?} FROM={:?}", i, self.nodes[i].key, from_key);
                let node_bytes = &self.nodes[i].key.as_bytes();
                if &node_bytes[..key_from_bytes.as_bytes().len()] == key_from_bytes.as_bytes() {
                //if &self.nodes[i].key[..from_key.len()] == from_key {
                    i
                } else {
                    return
                }
            },
        };

        let start = index;

        while index < self.nodes.len() - 1 && cmp_array(&self.nodes[index + 1].key.as_bytes(), key_from_bytes.as_bytes()) {
            index += 1;
        }

        let mut new = self.nodes.clone_range(start, index);
        // let mut new = self.nodes[start..=index].to_vec();

        self.delete(to_key);

        // println!("COPY FROM {} TO {}", start, index);

        let res = self.nodes.binary_search_by(|node| {
            node.key.cmp(&key_to_bytes)
        });

        let index = match res {
            Ok(index_found) => index_found,
            Err(i) => i,
        };

        // println!("INSERT AT {:?}", index);

        for node in new.iter_mut() {
            // let mut to = to_key.clone();
            // to.extend_from_slice(&node.key[from_key.len()..]);

            // println!("TO_BYTES={:?}", key_to_bytes.as_bytes());

            let mut to = key_to_bytes.clone();
            to.data.push(0);
            to.data.extend_from_slice(node.key.get_slice(from_key.len()..node.key.len()));

            // println!("NEW FROM={:?} TO={:?}", node.key.as_bytes(), to.as_bytes());

            node.key = to;
        }

        while let Some(node) = new.pop() {
            self.nodes.insert(index, node);
        }

        // self.serialize();
    }

    pub fn mem(&mut self, key: &ContextKey) -> bool {
        let key = NodeKey::new(key);

        self.find_path(&key).is_ok()

        // let res = self.nodes.binary_search_by(|node| {
        //     // if node.key.len() > key.len() {
        //     //     node.key[..key.len()].cmp(key)
        //     //     // node.key[..key.len()].cmp(key)
        //     // } else {
        //         node.key.cmp(&key_bytes)
        //     // }
        // });

        // if let Ok(res) = res {
        //     if !self.nodes[res].removed {
        //         return true;
        //     }
        // };

        // false
        // res.is_ok()
    }

    pub fn dirmem(&mut self, key: &ContextKey) -> bool {
        let key_bytes = NodeKey::new(key);

        let _ = self.find_path(&key_bytes);

        let res = self.nodes.binary_search_by(|node| {
            if node.key.nwords() > key.len() {
                node.key.get_slice(0..key.len()).cmp(key_bytes.as_bytes())
            } else {
                node.key.cmp(&key_bytes)
            }
        });

        let res = match res {
            Ok(res) => res,
            Err(_) => return false
        };

        if self.nodes[res].key.nwords() > key.len() {
            // TODO: It's not the only case where it's true
            return true;
        }

        false
    }

    pub fn delete(&mut self, key: &ContextKey) {
        let key_bytes = NodeKey::new(key);

        // println!("DELETE {:?}", key);

        self.invalidate_hashes(&key_bytes);

        // let index = self.nodes.binary_search_by(|node| {
        //     // if node.key.len() > key.len() {
        //     //     node.key[..key.len()].cmp(key)
        //     // } else {
        //         node.key.cmp(&key_bytes)
        //     // }
        // });

        // let mut index = match index {
        //     Ok(index) => index,
        //     Err(_) => return
        // };

        let mut index = match self.find_path(&key_bytes) {
            Ok(index) => index,
            Err(_) => return
        };

        while index > 0 && cmp_array(&self.nodes[index - 1].key.as_bytes(), key_bytes.as_bytes()) {
            index -= 1;
        }

        let start = index;

        while index < self.nodes.len() - 1 && cmp_array(&self.nodes[index + 1].key.as_bytes(), key_bytes.as_bytes()) {
            index += 1;
        }

        let end = index;

        // let _: Vec<_> = self.nodes.drain(start..end + 1).collect();

        for index in start..=end {
            let node = &mut self.nodes[index];
            node.removed = true;
        }

        // while start <= end {
        //     let node = &mut self.nodes[start];
        //     node.removed = true;
        //     start += 1;
        // }

        // for node in &mut self.nodes[start..=end] {
        //     node.removed = true;
        // }

        self.nodes.retain(|n| !n.removed);
    }

    /// Flush the working tree and and move to work on a certain commit from history.
    pub fn checkout(&mut self, context_hash: &EntryHash) -> Result<(), MerkleError> {
        // let stat_updater = StatUpdater::new(MerkleStorageAction::Checkout, None);

        self.nodes.clear();
        let commit = self.get_commit(&context_hash)?;
        let entry = self.get_entry_from_hash(&commit.root_hash)?;
        let tree = self.get_tree(&entry)?;
        let tree = Tree(Cow::Owned(tree.0.into_owned()));
        self.apply_tree_to_root(&tree);

        // self.nodes = nodes;

        // let entry = self.get_entry_from_hash(&commit.root_hash)?;
        // let tree = self.get_tree(&entry)?.clone();

        // self.trees = HashMap::new();
        // self.set_working_tree_root(tree, 0);
        self.last_commit_hash = Some(context_hash.clone());

        // stat_updater.update_execution_stats(&mut self.stats);
        Ok(())
    }

    fn get_entry_from_hash(&self, hash: &EntryHash) -> Result<Entry, MerkleError> {
        let entry_bytes = self.db.get(&hash)?;
        match entry_bytes {
            None => Err(MerkleError::EntryNotFound {
                hash: HashType::ContextHash.hash_to_b58check(hash)?,
            }),
            Some(entry_bytes) => Ok(bincode::deserialize(&entry_bytes)?),
        }
    }

    fn get_commit(&self, hash: &EntryHash) -> Result<Commit, MerkleError> {
        match self.get_entry_from_hash(hash)? {
            Entry::Commit(commit) => Ok(commit),
            Entry::Tree(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
        }
    }

    fn get_tree<'e>(&self, entry: &'e Entry) -> Result<Tree<'e>, MerkleError> {
        match entry {
            Entry::Tree(tree) => Ok(Tree(Cow::Borrowed(&tree.0))),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn get_from_tree(&self, root: &Tree, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let file = key.last().ok_or(MerkleError::KeyEmpty)?;
        let path = &key[..key.len() - 1];

        // find tree by path
        let node = self.find_tree(&root, &path)?;

        // get file node from tree
        let node = match node.get(file) {
            None => {
                return Err(MerkleError::ValueNotFound {
                    key: self.key_to_string(key),
                });
            }
            Some(entry) => entry,
        };

        // get blob by hash
        match self.get_entry_from_hash(&node.entry_hash)? {
            Entry::Blob(blob) => Ok((*blob).clone()),
            _ => Err(MerkleError::ValueIsNotABlob {
                key: self.key_to_string(key),
            }),
        }
    }

    /// Convert key in array form to string form
    fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    /// Find tree by path and return a copy. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    fn find_tree(&self, root: &Tree, key: &[String]) -> Result<Tree, MerkleError> {
        let first = match key.first() {
            Some(first) => first,
            None => {
                // terminate recursion if end of path was reached
                return Ok(Tree(Cow::Owned(root.0.to_vec())));
            }
        };

        // get node at key
        let child_node = match root.get(first) {
            Some(hash) => hash,
            None => {
                return Ok(Tree(Cow::Owned(vec![])));
            }
        };

        // get entry by hash (from working tree or DB)
        match self.get_entry_from_hash(&child_node.entry_hash)? {
            Entry::Tree(tree) => self.find_tree(&tree, &key[1..]),
            Entry::Blob(_) => Ok(Tree(Cow::Owned(vec![]))),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    /// Get value from historical context identified by commit hash.
    pub fn get_history(
        &mut self,
        commit_hash: &EntryHash,
        key: &ContextKey,
    ) -> Result<ContextValue, MerkleError> {
        // let stat_updater = StatUpdater::new(MerkleStorageAction::GetHistory, Some(key));

        let commit = self.get_commit(commit_hash)?;
        let entry = self.get_entry_from_hash(&commit.root_hash)?;
        let rv = self.get_from_tree(&self.get_tree(&entry)?, key);

        // stat_updater.update_execution_stats(&mut self.stats);
        rv
    }

    /// Get last committed hash
    pub fn get_last_commit_hash(&self) -> Option<EntryHash> {
        self.last_commit_hash
    }

    // pub fn flush(&self) {}

    pub fn get_working_tree_root_hash(&mut self) -> Result<EntryHash, MerkleError> {
        let (root_hash, _) = self.serialize(false);
        return Ok(root_hash)
    }

    pub fn get_memory_usage(&self) -> Result<usize, MerkleError> {
        Ok(0)
    }

    /// Get various merkle storage statistics
    pub fn get_merkle_stats(&self) -> Result<MerkleStoragePerfReport, MerkleError> {
        Ok(MerkleStoragePerfReport {
            perf_stats: MerklePerfStats::default(),
            kv_store_stats: self.db.total_get_mem_usage()?,
        })
    }
}

fn cmp_array(node: &[u8], key: &[u8]) -> bool {
    let node_len = node.len();
    let key_len = key.len();

    &node[..key_len.min(node_len)] == key
}

// fn cmp_array(node: &[String], key: &[String]) -> bool {
//     let node_len = node.len();
//     let key_len = key.len();

//     &node[..key_len.min(node_len)] == key
// }

impl Flushable for NewMerkle {
    fn flush(&self) -> Result<(), failure::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use crate::context::kv_store::{SupportedContextKeyValueStore, test_support::TestContextKvStoreFactoryInstance};

    use super::*;
    use super::NewMerkle as MerkleStorage;

    // #[test]
    // fn test_new_impl() {
    fn test_new_impl(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        println!("START", );

        let mut storage = NewMerkle::new(
            kv_store_factory
                .create("test_new_impl")
                .unwrap(),
        );

        let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
        let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];

        storage.set(&vec!["c".to_string(), "zoo".to_string()], vec![1, 2]);
        storage.set(&vec!["a".to_string(), "goo".to_string()], vec![97, 98]); // TODO: goo should be removed because of the next line
        storage.set(&vec!["a".to_string()], vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "foo".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "foo".to_string()], vec![3, 4]);
        storage.set(&vec!["a".to_string(), "foo".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "abc".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "moo".to_string()], vec![3, 4]);
        storage.set(&vec!["a".to_string(), "foo".to_string(), "baa".to_string()], vec![97, 98]);
        storage.set(&vec!["a".to_string(), "foo".to_string(), "abc".to_string()], vec![97, 98]);

        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e1".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e2".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e3".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e4".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "aaaaa".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);

        storage.set(&vec!["g".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);

        storage.display();

        let res = storage.mem(&vec!["a".to_string()]);
        println!("MEM a {:?}", res);
        let res = storage.mem(&vec!["a".to_string(), "foo".to_string()]);
        println!("MEM a foo {:?}", res);
        let res = storage.mem(&vec!["a".to_string(), "foo".to_string(), "a".to_string()]);
        println!("MEM a foo a {:?}", res);
        let res = storage.mem(&vec!["z".to_string()]);
        println!("MEM z {:?}", res);

        println!("BEFORE DELETE", );
        storage.display();

        // storage.delete(&vec!["a".to_string(), "foo".to_string(), "abc".to_string()]);
        // storage.delete(&vec!["a".to_string(), "foo".to_string()]);
        // storage.delete(&vec!["c".to_string(), "zoo".to_string()]);
        // storage.delete(&vec![]);

        println!("AFTER DELETE", );

        storage.nodes.retain(|n| !n.removed);
        storage.display();

        println!("START RECURSIVE", );

        // storage.aaaa();
        // storage.recursive(0, 0, 0);
        // storage.start_recursive();
        // storage.serialize();

        let hash = storage.get_working_tree_root_hash().unwrap();
        println!("HASH1={:?}", display_hash(&hash));
        let hash = storage.commit(1, "seb".to_string(), "ok".to_string());

        assert_eq!(display_hash(&hash), "64DA7B104C91B59BBC84F6971F4E92CA1ABE7265803FF37CD62F653CFE5E1748");

        println!("LAAAAAAAAABC", );
        let hash = storage.get_working_tree_root_hash().unwrap();
        println!("HASH2={:?}", display_hash(&hash));

        // storage.run();
    }

    // #[test]
    // fn test_new_impl() {
    fn test_apply_tree(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        println!("START", );

        let mut storage = NewMerkle::new(
            kv_store_factory
                .create("test_new_impl")
                .unwrap(),
        );

        let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
        let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];

        storage.set(&vec!["c".to_string(), "zoo".to_string()], vec![1, 2]);
        storage.set(&vec!["a".to_string(), "goo".to_string()], vec![97, 98]); // TODO: goo should be removed because of the next line
        storage.set(&vec!["a".to_string()], vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);

        storage.set(&vec!["g".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);
        storage.nodes.retain(|n| !n.removed);

        println!("BEFORE APPLY", );
        storage.display();

        storage.apply_tree_to_root(&Tree(Cow::Owned(vec![
            (vec![101], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] }),
            (vec![102], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] })
        ])));

        println!("AFTER APPLY", );
        storage.display();

        storage.apply_tree_to_working_tree(&[101], &Tree(Cow::Owned(vec![
            (vec![102], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] }),
            (vec![103], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] }),
            (vec![104], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] }),
            (vec![105], TreeNode { node_kind: NodeKind::Leaf, entry_hash: [1; 32] })
        ])));

        println!("AFTER APPLY2", );
        storage.display();

        // storage.display();

        // println!("START RECURSIVE", );

        // storage.aaaa();
        // storage.recursive(0, 0, 0);
        // storage.start_recursive();
        // storage.serialize();

        let hash = storage.commit(1, "seb".to_string(), "ok".to_string());

        assert_eq!(display_hash(&hash), "4F81106771A4EDED7B3E9A18AFE172A2F00BCDB3DF4BDC4867281C93269C967");

        // storage.run();
    }

    // #[test]
    // fn test_new_impl() {
    fn test_recompute(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        println!("START", );

        let mut storage = NewMerkle::new(
            kv_store_factory
                .create("test_new_impl")
                .unwrap(),
        );

        storage.set(&vec!["a".to_string(), "goo".to_string()], vec![97, 98]); // TODO: goo should be removed because of the next line
        storage.set(&vec!["a".to_string(), "aaa".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "aaa".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "ccc".to_string()], vec![97, 98]);

        println!("ROOT_HASH", );
        storage.get_working_tree_root_hash().unwrap();
        println!("ROOT_HASH DONE", );

        storage.set(&vec!["a".to_string(), "bbb".to_string()], vec![97, 98]);

        println!("COMMIT", );
        let hash = storage.commit(1, "seb".to_string(), "ok".to_string());

        // assert_eq!(display_hash(&hash), "4F81106771A4EDED7B3E9A18AFE172A2F00BCDB3DF4BDC4867281C93269C967");

        // storage.run();
    }

    fn test_my_copy(kv_store_factory: &TestContextKvStoreFactoryInstance) {

        let mut storage = NewMerkle::new(
            kv_store_factory
                .create("test_my_copy")
                .unwrap(),
        );

        let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
        let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];

        storage.set(&vec!["c".to_string(), "zoo".to_string()], vec![1, 2]);
        storage.set(&vec!["a".to_string(), "goo".to_string()], vec![97, 98]); // TODO: goo should be removed because of the next line
        storage.set(&vec!["a".to_string()], vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "foo".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "foo".to_string()], vec![3, 4]);
        storage.set(&vec!["a".to_string(), "foo".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "abc".to_string()], vec![97, 98]);
        storage.set(&vec!["c".to_string(), "moo".to_string()], vec![3, 4]);
        storage.set(&vec!["a".to_string(), "foo".to_string(), "baa".to_string()], vec![97, 98]);
        storage.set(&vec!["a".to_string(), "foo".to_string(), "abc".to_string()], vec![97, 98]);

        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e1".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e2".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "f".to_string(), "e3".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e4".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "aaaaa".to_string()], vec![97, 98]);
        storage.set(&vec!["b".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);

        storage.set(&vec!["g".to_string(), "o".to_string(), "m".to_string(), "e5".to_string(), "bbbbb".to_string()], vec![97, 98]);

        storage.nodes.retain(|n| !n.removed);
        // storage.commit(1, "a".to_string(), "b".to_string());

        storage.display();

        storage.copy(
            &vec!["b".to_string(), "o".to_string(), "f".to_string()],
            &vec!["b".to_string(), "o".to_string(), "tar".to_string()]
        );

    }

    // #[test]
    // fn test_new_impl_fix() {
    //     println!("START", );

    //     let mut storage = NewMerkle::new();

    //     let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
    //     let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];

    //     storage.set(&vec!["c".to_string(), "zoo".to_string()], vec![1, 2]);
    //     storage.set(&vec!["a".to_string(), "goo".to_string()], vec![97, 98]); // TODO: goo should be removed because of the next line

    //     println!("BEFORE", );
    //     storage.display();

    //     storage.set(&vec!["a".to_string()], vec![97, 98]);

    //     println!("AFTER", );
    //     storage.display();
    // }

    // #[test]
    // fn test_new_impl_dirmem() {
    //     let mut storage = NewMerkle::new();

    //     let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
    //     let key_ab: &ContextKey = &vec!["a".to_string(), "b".to_string()];
    //     let key_a: &ContextKey = &vec!["a".to_string()];

    //     assert_eq!(storage.dirmem(&key_a), false);
    //     assert_eq!(storage.dirmem(&key_ab), false);
    //     assert_eq!(storage.dirmem(&key_abc), false);
    //     storage.set(key_abc, vec![1u8, 2u8]);
    //     assert_eq!(storage.dirmem(&key_a), true);
    //     assert_eq!(storage.dirmem(&key_ab), true);
    //     assert_eq!(storage.dirmem(&key_abc), false);

    //     println!("BEFORE DELETE", );
    //     storage.display();

    //     storage.delete(key_abc);

    //     println!("AFTER DELETE", );
    //     storage.display();

    //     assert_eq!(storage.dirmem(&key_a), false);
    //     assert_eq!(storage.dirmem(&key_ab), false);
    //     assert_eq!(storage.dirmem(&key_abc), false);

    //     println!("DIRMEM OK");
    // }

    fn test_duplicate_entry_in_working_tree(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = NewMerkle::new(
            kv_store_factory
                .create("test_duplicate_entry_in_working_tree")
                .unwrap(),
        );

        let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
        let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];
        storage
            .set(&vec!["a".to_string(), "foo".to_string()], vec![97, 98]);
        storage
            .set(&vec!["c".to_string(), "zoo".to_string()], vec![1, 2]);
        storage
            .set(&vec!["c".to_string(), "foo".to_string()], vec![97, 98]);
        storage
            .delete(&vec!["c".to_string(), "zoo".to_string()]);
        // now c/ is the same tree as a/ - which means there are two references to single entry in working tree
        // modify the tree and check that the other one was kept intact
        storage
            .set(&vec!["c".to_string(), "foo".to_string()], vec![3, 4]);
        let commit = storage
            .commit(0, "Tezos".to_string(), "Genesis".to_string());

        assert_eq!(storage.get_history(&commit, a_foo).unwrap(), vec![97, 98]);
        assert_eq!(storage.get_history(&commit, c_foo).unwrap(), vec![3, 4]);
    }

    fn test_tree_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_tree_hash").unwrap());

        storage
            .set(
                &vec!["a".to_string(), "foo".to_string()],
                vec![97, 98, 99],
            ); // abc
        storage
            .set(&vec!["b".to_string(), "boo".to_string()], vec![97, 98]);
        storage
            .set(
                &vec!["a".to_string(), "aaa".to_string()],
                vec![97, 98, 99, 100],
            );
        storage.set(&vec!["x".to_string()], vec![97]);
        storage
            .set(
                &vec!["one".to_string(), "two".to_string(), "three".to_string()],
                vec![97],
            );
        storage
            .commit(0, "Tezos".to_string(), "Genesis".to_string());

        let hash = storage.get_working_tree_root_hash().unwrap();

        assert_eq!([0xDB, 0xAE, 0xD7, 0xB6], hash[0..4]);
    }

    fn test_commit_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_commit_hash").unwrap());

        storage.set(&vec!["a".to_string()], vec![97, 98, 99]);

        let commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());

        assert_eq!([0xCF, 0x95, 0x18, 0x33], commit[0..4]);

        storage.set(&vec!["data".to_string(), "x".to_string()], vec![97]);
        let commit = storage.commit(0, "Tezos".to_string(), "".to_string());

        assert_eq!([0xCA, 0x7B, 0xC7, 0x02], commit[0..4]);
        // full irmin hash: ca7bc7022ffbd35acc97f7defb00c486bb7f4d19a2d62790d5949775eb74f3c8
    }

    fn test_multiple_commit_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(
            kv_store_factory
                .create("test_multiple_commit_hash")
                .unwrap(),
        );

        let _commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());

        storage
            .set(
                &vec!["data".to_string(), "a".to_string(), "x".to_string()],
                vec![97],
            );
        println!("LAAAA");
        storage.display();

        storage
            .copy(
                &vec!["data".to_string(), "a".to_string()],
                &vec!["data".to_string(), "b".to_string()],
            );
        println!("AFTER");
        storage
            .delete(
                &vec!["data".to_string(), "b".to_string(), "x".to_string()],
            );

        storage.display();

        let commit = storage.commit(0, "Tezos".to_string(), "".to_string());

        assert_eq!([0x9B, 0xB0, 0x0D, 0x6E], commit[0..4]);

        storage.display();
    }

    fn test_get(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let db_name = "test_get";

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];

        let kv_store = kv_store_factory.create(db_name).unwrap();
        let mut storage = MerkleStorage::new(kv_store);

        let res = storage.get(&vec![]);

        println!("RES={:?}", res);

        assert_eq!(res.unwrap().is_empty(), true);
        let res = storage.get(&vec!["a".to_string()]);
        assert_eq!(res.unwrap().is_empty(), true);

        storage.set(key_abc, vec![1u8, 2u8]);
        storage.set(key_abx, vec![3u8]);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
        let commit1 = storage.commit(0, "".to_string(), "".to_string());

        storage.set(key_az, vec![4u8]);
        storage.set(key_abx, vec![5u8]);
        storage.set(key_d, vec![6u8]);
        storage.set(key_eab, vec![7u8]);
        assert_eq!(storage.get(key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get(key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get(key_eab).unwrap(), vec![7u8]);
        let commit2 = storage.commit(0, "".to_string(), "".to_string());

        assert_eq!(
            storage.get_history(&commit1, key_abc).unwrap(),
            vec![1u8, 2u8]
        );
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);
    }

    fn test_mem(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_mem").unwrap());

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];

        assert_eq!(storage.mem(&key_abc), false);
        assert_eq!(storage.mem(&key_abx), false);
        storage.set(key_abc, vec![1u8, 2u8]);
        assert_eq!(storage.mem(&key_abc), true);
        assert_eq!(storage.mem(&key_abx), false);
        storage.set(key_abx, vec![3u8]);
        assert_eq!(storage.mem(&key_abc), true);
        assert_eq!(storage.mem(&key_abx), true);
        storage.delete(key_abx);
        assert_eq!(storage.mem(&key_abc), true);
        assert_eq!(storage.mem(&key_abx), false);
    }

    fn test_dirmem(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_dirmem").unwrap());

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_ab: &ContextKey = &vec!["a".to_string(), "b".to_string()];
        let key_a: &ContextKey = &vec!["a".to_string()];

        assert_eq!(storage.dirmem(&key_a), false);
        assert_eq!(storage.dirmem(&key_ab), false);
        assert_eq!(storage.dirmem(&key_abc), false);
        storage.set(key_abc, vec![1u8, 2u8]);
        assert_eq!(storage.dirmem(&key_a), true);
        assert_eq!(storage.dirmem(&key_ab), true);
        assert_eq!(storage.dirmem(&key_abc), false);
        storage.delete(key_abc);
        assert_eq!(storage.dirmem(&key_a), false);
        assert_eq!(storage.dirmem(&key_ab), false);
        assert_eq!(storage.dirmem(&key_abc), false);
    }

    fn test_copy(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_copy").unwrap());

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, vec![1_u8]);

        println!("BEFORE");
        storage.display();

        storage.copy(&vec!["a".to_string()], &vec!["z".to_string()]);

        println!("AFTER");
        storage.display();

        assert_eq!(
            vec![1_u8],
            storage
                .get(&vec!["z".to_string(), "b".to_string(), "c".to_string()]).unwrap()

        );
        // TODO test copy over commits
    }

    fn test_delete(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(kv_store_factory.create("test_delete").unwrap());

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, vec![2_u8]);
        storage.set(key_abx, vec![3_u8]);
        storage.delete(key_abx);
        let commit1 = storage.commit(0, "".to_string(), "".to_string());

        assert!(storage.get_history(&commit1, &key_abx).is_err());
    }

    fn test_deleted_entry_available(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(
            kv_store_factory
                .create("test_deleted_entry_available")
                .unwrap(),
        );

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, vec![2_u8]);
        let commit1 = storage.commit(0, "".to_string(), "".to_string());
        storage.delete(key_abc);
        let _commit2 = storage.commit(0, "".to_string(), "".to_string());

        assert_eq!(vec![2_u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    fn test_delete_in_separate_commit(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let mut storage = MerkleStorage::new(
            kv_store_factory
                .create("test_delete_in_separate_commit")
                .unwrap(),
        );

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, vec![2_u8]);
        storage.set(key_abx, vec![3_u8]);
        storage.commit(0, "".to_string(), "".to_string());

        storage.delete(key_abx);
        let commit2 = storage.commit(0, "".to_string(), "".to_string());

        assert!(storage.get_history(&commit2, &key_abx).is_err());
    }

    fn test_checkout(kv_store_factory: &TestContextKvStoreFactoryInstance) {
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];

        let mut storage = MerkleStorage::new(kv_store_factory.create("test_checkout").unwrap());

        storage.set(key_abc, vec![1u8]);
        storage.set(key_abx, vec![2u8]);

        println!("BEFORE COMMIT1");
        storage.display();

        let commit1 = storage.commit(0, "".to_string(), "".to_string());

        storage.set(key_abc, vec![3u8]);
        storage.set(key_abx, vec![4u8]);
        let commit2 = storage.commit(0, "".to_string(), "".to_string());

        storage.checkout(&commit1).unwrap();

        println!("AFTER CHECKOUT COMMIT1");
        storage.display();
        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
        // this set be wiped by checkout
        storage.set(key_abc, vec![8u8]);

        storage.checkout(&commit2).unwrap();
        assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
    }

    macro_rules! tests_with_storage {
        ($storage_tests_name:ident, $kv_store_factory:expr) => {
            mod $storage_tests_name {
                #[test]
                fn test_new_impl() {
                    super::test_new_impl($kv_store_factory)
                }
                #[test]
                fn test_apply_tree() {
                    super::test_apply_tree($kv_store_factory)
                }
                #[test]
                fn test_duplicate_entry_in_working_tree() {
                    super::test_duplicate_entry_in_working_tree($kv_store_factory)
                }
                #[test]
                fn test_tree_hash() {
                    super::test_tree_hash($kv_store_factory)
                }
                #[test]
                fn test_commit_hash() {
                    super::test_commit_hash($kv_store_factory)
                }
                #[test]
                fn test_my_copy() {
                    super::test_my_copy($kv_store_factory)
                }
                #[test]
                fn test_recompute() {
                    super::test_recompute($kv_store_factory)
                }
                #[test]
                fn test_multiple_commit_hash() {
                    super::test_multiple_commit_hash($kv_store_factory)
                }
                #[test]
                fn test_get() {
                    super::test_get($kv_store_factory)
                }
                #[test]
                fn test_mem() {
                    super::test_mem($kv_store_factory)
                }
                #[test]
                fn test_dirmem() {
                    super::test_dirmem($kv_store_factory)
                }
                #[test]
                fn test_copy() {
                    super::test_copy($kv_store_factory)
                }
                #[test]
                fn test_delete() {
                    super::test_delete($kv_store_factory)
                }
                #[test]
                fn test_deleted_entry_available() {
                    super::test_deleted_entry_available($kv_store_factory)
                }
                #[test]
                fn test_delete_in_separate_commit() {
                    super::test_delete_in_separate_commit($kv_store_factory)
                }
                #[test]
                fn test_checkout() {
                    super::test_checkout($kv_store_factory)
                }
                // #[test]
                // fn test_get_context_tree_by_prefix() {
                //     super::test_get_context_tree_by_prefix($kv_store_factory)
                // }
                // #[test]
                // fn test_backtracking_on_set() {
                //     super::test_backtracking_on_set($kv_store_factory)
                // }
                // #[test]
                // fn test_backtracking_on_delete() {
                //     super::test_backtracking_on_delete($kv_store_factory)
                // }
                // #[test]
                // fn test_fail_to_checkout_stage_from_before_commit() {
                //     super::test_checkout_stage_from_before_commit($kv_store_factory)
                // }
            }
        };
    }

    tests_with_storage!(
        kv_store_inmemory_tests,
        super::SUPPORTED_KV_STORES
            .get(&crate::context::kv_store::SupportedContextKeyValueStore::InMem)
            .unwrap()
    );

    lazy_static::lazy_static! {
        static ref SUPPORTED_KV_STORES: std::collections::HashMap<SupportedContextKeyValueStore, TestContextKvStoreFactoryInstance> = crate::context::kv_store::test_support::all_kv_stores(out_dir_path());
    }

    fn out_dir_path() -> PathBuf {
        let out_dir = env::var("OUT_DIR").expect(
            "OUT_DIR is not defined - please add build.rs to root or set env variable OUT_DIR",
        );
        out_dir.as_str().into()
    }

    #[test]
    fn test_node_key() {
        let key = NodeKey::new(&["abc".to_string(), "baa".to_string()]);

        assert_eq!(key.get(0).unwrap(), &[97,98,99]);
        assert_eq!(key.get(1).unwrap(), &[98, 97, 97]);
        assert!(key.get(2).is_none());
        assert_eq!(key.nwords(), 2);

        let key = NodeKey::new(&["a".to_string(), "b".to_string()]);

        assert_eq!(key.get(0).unwrap(), &[97]);
        assert_eq!(key.get(1).unwrap(), &[98]);
        assert!(key.get(2).is_none());


        let key = NodeKey::new(&[]);
        assert!(key.get(0).is_none());

        let key = NodeKey::new(&["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string(), "e".to_string()]);
        assert_eq!(key.get_slice(0..1), &[97]);
        assert_eq!(key.get_slice(0..2), &[97, 0, 98]);
        assert_eq!(key.get_slice(0..3), &[97, 0, 98, 0, 99]);
        assert_eq!(key.get_slice(1..3), &[98, 0, 99]);
        assert_eq!(key.get_slice(2..3), &[99]);
        assert_eq!(key.get_slice(0..5), &[97, 0, 98, 0, 99, 0, 100, 0, 101]);
        assert_eq!(key.get_slice(1..4), &[98, 0, 99, 0, 100]);
        assert_eq!(key.get_slice(1..5), &[98, 0, 99, 0, 100, 0, 101]);

        assert_eq!(key.split_last().unwrap(), (&[97, 0, 98, 0, 99, 0, 100][..], &[101][..]));

        let key = vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string(), "e".to_string()];
        assert_eq!(key.get(0..1).unwrap(), &["a".to_string()]);
        assert_eq!(key.get(0..2).unwrap(), &["a".to_string(), "b".to_string()]);
        assert_eq!(key.get(0..3).unwrap(), &["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(key.get(1..3).unwrap(), &["b".to_string(), "c".to_string()]);
        assert_eq!(key.get(2..3).unwrap(), &["c".to_string()]);
        assert_eq!(key.get(0..5).unwrap(), &["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string(), "e".to_string()]);
        assert_eq!(key.get(1..4).unwrap(), &["b".to_string(), "c".to_string(), "d".to_string()]);
        assert_eq!(key.get(1..5).unwrap(), &["b".to_string(), "c".to_string(), "d".to_string(), "e".to_string()]);

        let key = NodeKey::new(&["b".to_string(), "abc".to_string()]);
    }

    #[test]
    fn test_hash() {
        let mut hasher = DefaultHasher::new();
        hasher.write(b"abc");
        let res = hasher.finish();

        println!("RES={:?}", res);

        let mut hasher = DefaultHasher::new();
        hasher.write(b"abc");
        let res = hasher.finish();
        println!("RES={:?}", res);
    }
}
