use std::{cmp::Ordering, collections::BTreeMap, convert::TryInto};

use blake2::VarBlake2b;
use blake2::digest::{InvalidOutputSize, Update, VariableOutput};
use crypto::hash::HashType;

use crate::context::{ContextKey, ContextKeyValueStore, ContextValue, EntryHash};

use super::{hash::{ENTRY_HASH_LEN, HashingError}, merkle_storage::MerkleError};

use serde::{Deserialize, Serialize};

struct NodeKey (usize);

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
fn hash_short_inode(tree: &[(String, TreeNode)]) -> Result<EntryHash, HashingError> {
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
        hasher.update(encode_irmin_node_kind(&v.node_kind));
        // Key length is written in LEB128 encoding
        leb128::write::unsigned(&mut hasher, k.len() as u64)?;
        hasher.update(k.as_bytes());
        hasher.update(&(ENTRY_HASH_LEN as u64).to_be_bytes());
        hasher.update(&v.entry_hash);
    }

    Ok(hasher.finalize_boxed().as_ref().try_into()?)
}

//fn hash_tree(tree: &Tree) -> Result<EntryHash, HashingError> {
fn hash_tree(tree: &[(String, TreeNode)]) -> Result<EntryHash, HashingError> {
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
pub struct Tree (Vec<(String, TreeNode)>);

impl Tree {
    fn get(&self, key: &str) -> Option<&TreeNode> {
        match self.0.binary_search_by(|node| {
            node.0.as_str().cmp(key)
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
pub enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Commit),
}

#[derive(Debug, Clone)]
struct Node {
    key: Vec<String>,
    value: ContextValue,
    removed: bool,
}

pub struct NewMerkle {
    nodes: Vec<Node>,
    /// key value storage backend
    db: Box<ContextKeyValueStore>,
    /// Last commit hash
    last_commit_hash: Option<EntryHash>,
}

struct MerkleSerializer<'a> {
    nodes: &'a [Node],
    last_sibling: usize,
    hashes: Vec<(String, TreeNode)>,
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
    }

    fn next_sibling(&self, row: usize, column: usize, depth: usize) -> Option<usize> {
        let current = self.nodes.get(row)?.key.get(..=column)?;

        loop {
            let row = row + 1;
            let sibling = self.nodes.get(row)?.key.get(..=column)?;
            if sibling.get(..sibling.len() - 1) == current.get(..current.len() - 1) {
                if sibling.last() != current.last() {
                    return Some(row);
                }
            } else {
                return None;
            }
        }
    }

    fn recursive(&mut self, row: usize, column: usize, depth: usize) -> usize {
        // println!("{}CALLED ROW={} COL={} {:?} DEPTH={}", " ".repeat(depth), row, column, &self.nodes[row].key, depth);

        let mut row = row;
        let mut nentries = 1;

        if let Some(new_column) = self.next_child(row, column) {
            println!("{}TREE1={}", " ".repeat(depth), self.nodes[row].key[column]);

            let tree_nentries = self.recursive(row, new_column, depth + 1);
            self.process_tree(&self.nodes[row], column, tree_nentries);
        } else {
            if self.nodes.get(row).is_none() {
                return 0;
            }

            let hash = self.process_leaf(&self.nodes[row], column);

            println!("{}LEAF1 {:?} HASH={:?}", " ".repeat(depth), self.nodes[row].key[column], display_hash(&hash));
        }

        while let Some(sibling) = self.next_sibling(row.max(self.last_sibling), column, depth) {
            // println!("{}FOUND SIBLING row={} col={} current_row={} {:?}", " ".repeat(depth), sibling, column, row, self.nodes[sibling].key.get(column));
            self.last_sibling = sibling;

            nentries += 1;

            if let Some(new_col) = self.next_child(sibling, column) {
                println!("{}TREE2={}", " ".repeat(depth), self.nodes[sibling].key[column]);

                let tree_nentries = self.recursive(sibling, new_col, depth + 1);
                self.process_tree(&self.nodes[sibling], column, tree_nentries);
            } else {
                let hash = self.process_leaf(&self.nodes[sibling], column);

                println!("{}LEAF2 {:?} HASH={:?}", " ".repeat(depth), self.nodes[sibling].key[column], display_hash(&hash));
            }
            row = sibling;
        }

        nentries
    }

    fn start_recursive(mut self) -> (EntryHash, Vec<(EntryHash, ContextValue)>) {
        self.recursive(0, 0, 0);

        println!("REMAINING={:}", self.hashes.len());

        // let tree = Tree(hashes);
        let hash_tree = hash_tree(&self.hashes).unwrap();
        println!("ROOT_HASH={:?}", display_hash(&hash_tree));

        self.serialized.push((hash_tree, bincode::serialize(&Entry::Tree(Tree(self.hashes.to_vec()))).unwrap()));

        (hash_tree, self.serialized)

        // let mut row = 0;

        // while let Some(child) = self.next_sibling(row, 0, 0) {
        //     println!("## CALLED NEW REC AT {}", child);
        //     self.recursive(child, 0, 0);
        //     row = child;
        // }
    }

    fn new(nodes: &[Node]) -> MerkleSerializer {
        MerkleSerializer {
            nodes,
            last_sibling: 0,
            hashes: vec![],
            serialized: vec![],
        }
    }

    fn process_leaf(
        &mut self,
        node: &Node,
        column: usize,
    ) -> EntryHash {
        let hash_blob = hash_blob(&node.value).unwrap();

        self.serialized.push((hash_blob, bincode::serialize(&Entry::Blob(node.value.clone())).unwrap()));
        self.hashes.push((node.key[column].clone(), TreeNode { node_kind: NodeKind::Leaf, entry_hash: hash_blob }));

        hash_blob
    }

    fn process_tree(&mut self, node: &Node, column: usize, tree_entries: usize) {
        let start = self.hashes.len() - tree_entries;

        let tree = &self.hashes[start..];
        let hash_tree = hash_tree(tree).unwrap();
        // println!("{}FULL_HASH_TREE={:?}", " ".repeat(depth), display_hash(&hash_tree));

        self.serialized.push((hash_tree, bincode::serialize(&Entry::Tree(Tree(tree.to_vec()))).unwrap()));

        self.hashes.truncate(start);
        self.hashes.push((node.key[column].clone(), TreeNode {
            node_kind: NodeKind::NonLeaf,
            entry_hash: hash_tree,
        }));
    }
}

impl NewMerkle {
    fn new(db: Box<ContextKeyValueStore>) -> NewMerkle {
        NewMerkle {
            db,
            nodes: Vec::new(),
            last_commit_hash: None,
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

    fn serialize(&self) -> (EntryHash, Vec<(EntryHash, ContextValue)>) {
        MerkleSerializer::new(&self.nodes)
            .start_recursive()
    }

    fn commit(
        &mut self,
        time: u64,
        author: String,
        message: String,
    ) -> EntryHash {
        let (root_hash, mut batch) = self.serialize();
        let parent_commit_hash = self.last_commit_hash;

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

        println!("COMMIT_HASH={:?}", display_hash(&new_commit_hash));

        new_commit_hash
    }

    // pub fn commit(
    //     &mut self,
    //     time: u64,
    //     author: String,
    //     message: String,
    // ) -> Result<EntryHash, MerkleError> {
    //     let root_hash = self.get_working_tree_root_hash()?;
    //     let parent_commit_hash = self.last_commit_hash;

    //     let new_commit = Commit {
    //         root_hash,
    //         parent_commit_hash,
    //         time,
    //         author,
    //         message,
    //     };
    //     let new_commit_hash = hash_commit(&new_commit)?;
    //     let entry = Entry::Commit(new_commit);

    //     self.display_tree(&self.working_tree.0, 0);

    //     // persist working tree entries to db
    //     let mut batch: Vec<(EntryHash, ContextValue)> = Vec::new();
    //     self.get_entries_recursively(&entry, Some(&self.working_tree.0), &mut batch)?;
    //     // write all entries at once (depends on backend)
    //     self.db.write_batch(batch)?;

    //     self.last_commit_hash = Some(new_commit_hash);
    //     let rv = Ok(new_commit_hash);

    //     stat_updater.update_execution_stats(&mut self.stats);
    //     rv
    // }

    fn display(&self) {
        for node in &self.nodes {
            // if !node.removed {
            //     println!("key={:?} value={:?}", node.key, node.value);
            // }
            println!("key={:?} value={:?} removed={:?}", node.key, node.value, node.removed);
        }
    }

    fn set(&mut self, key: &ContextKey, value: ContextValue) {
        let res = self.nodes.binary_search_by(|node| {
            node.key.cmp(key)
        });

        let mut index = match res {
            Ok(index_found) => {
                self.nodes[index_found].value = value;
                index_found
            }
            Err(index_missing) => {
                self.nodes.insert(index_missing, Node {
                    key: key.to_vec(),
                    value,
                    removed: false,
                });
                index_missing
            }
        };

        let mut check_after = index + 1;
        let key_len = key.len();

        while index > 0 {
            let node = &mut self.nodes[index - 1];
            let len = node.key.len();
            if len < key_len && node.key == &key[..len] {
                node.removed = true;
            } else {
                break;
            }
            index -= 1;
        }

        loop {
            let node = match self.nodes.get_mut(check_after) {
                Some(node) => node,
                None => return
            };
            let len = node.key.len();
            if len > key_len && &node.key[..key_len] == key {
                node.removed = true;
            } else {
                break;
            }
            check_after += 1;
        }
    }

    fn get(&self, key: &ContextKey) -> Option<ContextValue> {
        let res = self.nodes.binary_search_by(|node| {
            node.key.cmp(key)
        });

        match res {
            Ok(index_found) => {
                if !self.nodes[index_found].removed {
                    return Some(self.nodes[index_found].value.clone())
                }
                Some(vec![])
            }
            Err(_index_missing) => {
                Some(vec![])
                // None
            }
        }
    }

    fn copy(
        &mut self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) {
        // self.serialize();

        let res = self.nodes.binary_search_by(|node| {
            node.key.cmp(from_key)
        });

        let mut index = match res {
            Ok(index_found) => {
                println!("FOUND AT {:?}", index_found);
                index_found
            },
            Err(i) => {
                println!("NOT FOUND AT {:?} WITH NODE={:?} FROM={:?}", i, self.nodes[i].key, from_key);
                if &self.nodes[i].key[..from_key.len()] == from_key {
                    i
                } else {
                    return
                }
            },
        };

        let start = index;

        while index < self.nodes.len() - 1 && cmp_array(&self.nodes[index + 1].key, from_key) {
            index += 1;
        }

        let mut new = self.nodes[start..=index].to_vec();

        self.delete(to_key);

        println!("COPY FROM {} TO {}", start, index);

        let res = self.nodes.binary_search_by(|node| {
            node.key.cmp(to_key)
        });

        let index = match res {
            Ok(index_found) => index_found,
            Err(i) => i,
        };

        println!("INSERT AT {:?}", index);

        for node in new.iter_mut() {
            let mut to = to_key.clone();
            to.extend_from_slice(&node.key[from_key.len()..]);

            println!("NEW FROM={:?} TO={:?}", node.key, to);

            node.key = to;
        }

        while let Some(node) = new.pop() {
            self.nodes.insert(index, node);
        }

        // self.serialize();
    }

    fn mem(&self, key: &ContextKey) -> bool {
        let res = self.nodes.binary_search_by(|node| {
            if node.key.len() > key.len() {
                node.key[..key.len()].cmp(key)
            } else {
                node.key.cmp(key)
            }
        });

        if let Ok(res) = res {
            if !self.nodes[res].removed {
                return true;
            }
        };

        false
        // res.is_ok()
    }

    fn dirmem(&self, key: &ContextKey) -> bool {
        let res = self.nodes.binary_search_by(|node| {
            if node.key.len() > key.len() {
                node.key[..key.len()].cmp(key)
            } else {
                node.key.cmp(key)
            }
        });

        let res = match res {
            Ok(res) => res,
            Err(_) => return false
        };

        if self.nodes[res].key.len() > key.len() {
            // TODO: It's not the only case where it's true
            return true;
        }

        false
    }

    fn delete(&mut self, key: &ContextKey) {
        let index = self.nodes.binary_search_by(|node| {
            if node.key.len() > key.len() {
                node.key[..key.len()].cmp(key)
            } else {
                node.key.cmp(key)
            }
        });

        let mut index = match index {
            Ok(index) => index,
            Err(_) => return
        };

        while index > 0 && cmp_array(&self.nodes[index - 1].key, key) {
            index -= 1;
        }

        let start = index;

        while index < self.nodes.len() - 1 && cmp_array(&self.nodes[index + 1].key, key) {
            index += 1;
        }

        let end = index;

        // let _: Vec<_> = self.nodes.drain(start..end + 1).collect();

        for node in &mut self.nodes[start..=end] {
            node.removed = true;
        }

        self.nodes.retain(|n| !n.removed);
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

    fn get_tree<'e>(&self, entry: &'e Entry) -> Result<&'e Tree, MerkleError> {
        match entry {
            Entry::Tree(tree) => Ok(tree),
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
            Entry::Blob(blob) => Ok(blob),
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
                return Ok(root.clone());
            }
        };

        // get node at key
        let child_node = match root.get(first) {
            Some(hash) => hash,
            None => {
                return Ok(Tree(vec![]));
            }
        };

        // get entry by hash (from working tree or DB)
        match self.get_entry_from_hash(&child_node.entry_hash)? {
            Entry::Tree(tree) => self.find_tree(&tree, &key[1..]),
            Entry::Blob(_) => Ok(Tree(vec![])),
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
        let rv = self.get_from_tree(self.get_tree(&entry)?, key);

        // stat_updater.update_execution_stats(&mut self.stats);
        rv
    }

    pub fn get_working_tree_root_hash(&self) -> Result<EntryHash, MerkleError> {
        let (root_hash, _) = self.serialize();
        return Ok(root_hash)
    }
}

fn cmp_array(node: &[String], key: &[String]) -> bool {
    let node_len = node.len();
    let key_len = key.len();

    &node[..key_len.min(node_len)] == key
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

        let hash = storage.commit(1, "seb".to_string(), "ok".to_string());

        assert_eq!(display_hash(&hash), "64DA7B104C91B59BBC84F6971F4E92CA1ABE7265803FF37CD62F653CFE5E1748");

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
        storage.copy(&vec!["a".to_string()], &vec!["z".to_string()]);

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

    // fn test_checkout(kv_store_factory: &TestContextKvStoreFactoryInstance) {
    //     let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
    //     let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];

    //     let mut storage = MerkleStorage::new(kv_store_factory.create("test_checkout").unwrap());

    //     storage.set(key_abc, vec![1u8]);
    //     storage.set(key_abx, vec![2u8]);
    //     let commit1 = storage.commit(0, "".to_string(), "".to_string());

    //     storage.set(key_abc, vec![3u8]);
    //     storage.set(key_abx, vec![4u8]);
    //     let commit2 = storage.commit(0, "".to_string(), "".to_string());

    //     storage.checkout(&commit1).unwrap();
    //     assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
    //     assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
    //     // this set be wiped by checkout
    //     storage.set(key_abc, vec![8u8]);

    //     storage.checkout(&commit2).unwrap();
    //     assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
    //     assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
    // }

    macro_rules! tests_with_storage {
        ($storage_tests_name:ident, $kv_store_factory:expr) => {
            mod $storage_tests_name {
                #[test]
                fn test_new_impl() {
                    super::test_new_impl($kv_store_factory)
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
                // #[test]
                // fn test_checkout() {
                //     super::test_checkout($kv_store_factory)
                // }
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
}
