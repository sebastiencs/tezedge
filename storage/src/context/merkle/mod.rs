// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::context::merkle::hash::EntryHash;
use crate::context::ContextValue;

pub mod hash;
pub mod merkle_storage;
pub mod merkle_storage_stats;

// Tree must be an ordered structure for consistent hash in hash_tree.
// The entry names *must* be in lexicographical order, as required by the hashing algorithm.
// Currently immutable OrdMap is used to allow cloning trees without too much overhead.
pub type Tree = im::OrdMap<String, Arc<Node>>;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NodeKind {
    NonLeaf,
    Leaf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Node {
    pub node_kind: NodeKind,
    pub entry: Arc<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Commit {
    pub(crate) parent_commit_hash: Option<EntryHash>,
    pub(crate) root_hash: EntryHash,
    pub(crate) root: Arc<Tree>,
    pub(crate) time: u64,
    pub(crate) author: String,
    pub(crate) message: String,
}

impl core::hash::Hash for Commit {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.parent_commit_hash.hash(state);
        self.root_hash.hash(state);
        self.time.hash(state);
        self.author.hash(state);
        self.message.hash(state);
    }
}

// #[automatically_derived]
// #[allow(unused_qualifications)]
// impl ::core::hash::Hash for Commit {
//     fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
//         match *self {
//             Commit {
//                 parent_commit_hash: ref __self_0_0,
//                 root_hash: ref __self_0_1,
//                 time: ref __self_0_2,
//                 author: ref __self_0_3,
//                 message: ref __self_0_4,
//             } => {
//                 ::core::hash::Hash::hash(&(*__self_0_0), state);
//                 ::core::hash::Hash::hash(&(*__self_0_1), state);
//                 ::core::hash::Hash::hash(&(*__self_0_2), state);
//                 ::core::hash::Hash::hash(&(*__self_0_3), state);
//                 ::core::hash::Hash::hash(&(*__self_0_4), state)
//             }
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Commit),
}
