use std::{collections::BTreeMap, convert::TryInto};

use blake2::VarBlake2b;
use blake2::digest::{InvalidOutputSize, Update, VariableOutput};

use crate::context::{ContextKey, ContextValue, EntryHash};

use super::hash::{ENTRY_HASH_LEN, HashingError};

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

struct Node {
    key: Vec<String>,
    value: ContextValue,
    removed: bool,
}

struct NewMerkle {
    nodes: Vec<Node>,
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
    fn new() -> NewMerkle {
        NewMerkle {
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
        let mut ser = MerkleSerializer::new(&self.nodes);
        ser.start_recursive()
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
                None
            }
            Err(_index_missing) => {
                None
            }
        }
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

        for n in start..=end {
            self.nodes[n].removed = true;
        }
    }
}

fn cmp_array(node: &[String], key: &[String]) -> bool {
    let node_len = node.len();
    let key_len = key.len();

    &node[..key_len.min(node_len)] == key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_impl() {
        println!("START", );

        let mut storage = NewMerkle::new();

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

        storage.commit(1, "seb".to_string(), "ok".to_string());


        // storage.run();
    }

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

}
