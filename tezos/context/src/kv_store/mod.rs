// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This sub module provides different implementations of the `repository` used to store objects.

use std::convert::{TryFrom, TryInto};

use modular_bitfield::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{chunks::SharedIndexMap, ObjectHash};

pub mod hashes;
pub mod in_memory;
pub mod index_map;
pub mod persistent;
pub mod readonly_ipc;

pub const INMEM: &str = "inmem";

#[allow(dead_code)]
#[bitfield]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct NonZero6BytesInner {
    bytes: B48,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct NonZero6Bytes {
    inner: NonZero6BytesInner,
}

impl NonZero6Bytes {
    fn get(self) -> u64 {
        self.inner.bytes()
    }

    fn new(n: u64) -> Option<Self> {
        if n == 0 || n > 0xFFFFFFFFFFFF {
            return None;
        } else {
            Some(NonZero6Bytes {
                inner: NonZero6BytesInner::new().with_bytes(n),
            })
        }
    }

    // This avoid having compilation warnings
    #[allow(dead_code)]
    fn unused(self) {
        NonZero6BytesInner::from_bytes(self.inner.into_bytes());
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashId(NonZero6Bytes);

impl std::fmt::Debug for HashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hash_id = self.as_u64();
        f.debug_tuple("HashId").field(&hash_id).finish()
    }
}

#[derive(Debug)]
pub struct HashIdError;

impl TryInto<usize> for HashId {
    type Error = HashIdError;

    fn try_into(self) -> Result<usize, Self::Error> {
        Ok(self.0.get().checked_sub(1).ok_or(HashIdError)? as usize)
    }
}

impl TryFrom<usize> for HashId {
    type Error = HashIdError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let value: u64 = value.try_into().map_err(|_| HashIdError)?;

        value
            .checked_add(1)
            .and_then(NonZero6Bytes::new)
            .map(HashId)
            .ok_or(HashIdError)
    }
}

/// `HashId` is a `NonZeroU64` (8 bytes), but in the working tree and repo,
/// they are 6 bytes. So the shift must be 47, or we will get overflow if
/// shift is more than 47.
/// See `DirEntryInner::object_hash_id`, `PointerToInodeInner::hash_id`, and
/// `serialize::serialize_hash_id`.
const SHIFT: usize = 48 - 1;
/// Bit set when the HashId hasn't been commited
const IN_WORKING_TREE: u64 = 1 << SHIFT;

impl HashId {
    pub fn new(value: u64) -> Option<Self> {
        Some(HashId(NonZero6Bytes::new(value)?))
    }

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }

    pub fn set_in_working_tree(&mut self) -> Result<(), HashIdError> {
        let hash_id = self.0.get();

        self.0 = NonZero6Bytes::new(hash_id | IN_WORKING_TREE).ok_or(HashIdError)?;

        Ok(())
    }

    pub fn get_in_working_tree(self) -> Result<Option<HashId>, HashIdError> {
        let hash_id = self.0.get();
        if hash_id & IN_WORKING_TREE != 0 {
            Ok(Some(HashId(
                NonZero6Bytes::new(hash_id & !IN_WORKING_TREE).ok_or(HashIdError)?,
            )))
        } else {
            Ok(None)
        }
    }
}

pub struct VacantObjectHash<'a> {
    entry: Option<&'a mut ObjectHash>,
    map: Option<&'a mut SharedIndexMap<HashId, Option<Box<ObjectHash>>>>,
    hash_id: HashId,
}

// enum Vacant<'a> {
//     ByRef {
//         entry_ref: &'a mut ObjectHash,
//         hash_id: HashId
//     },
//     Push {
//         map: &'a mut SharedIndexMap<HashId, Option<Box<ObjectHash>>>,
//     },
//     UseFree {
//         map: &'a mut SharedIndexMap<HashId, Option<Box<ObjectHash>>>,
//         hash_id: HashId,
//     }
// }

impl<'a> VacantObjectHash<'a> {
    pub fn new(entry: &'a mut ObjectHash, hash_id: HashId) -> Self {
        Self {
            entry: Some(entry),
            hash_id,
            map: None,
        }
    }

    pub fn write_with<F>(self, fun: F) -> HashId
    where
        F: FnOnce(&mut ObjectHash),
    {
        if let Some(entry) = self.entry {
            fun(entry);
        } else if let Some(map) = self.map {
            let mut hash = Box::<ObjectHash>::default();
            fun(&mut hash);

            let hash_id: usize = self.hash_id.try_into().unwrap();

            // let hash_clone = hash.clone();

            // let mut is_push = false;

            // let before = map.len();

            if map.len() == hash_id {
                let new = map.push(hash).unwrap();
                assert_eq!(self.hash_id, new);
                // is_push = true;
            } else {
                map.insert_at(self.hash_id, hash).unwrap();
            }

            // let copy_hash_id = self.hash_id;

            // let chunk = map.chunk_index_of(self.hash_id).unwrap();

            // map.with(self.hash_id, |hash| {
            //     assert!(
            //         hash.unwrap().is_some(),
            //         "IS_PUSH={:?} HASH_ID={:?} MAP_LEN={:?} MAP_LEN_BEFORE={:?} CHUNK({:?})",
            //         is_push, copy_hash_id, map.len(), before, chunk
            //     );
            //     let a = hash.unwrap().as_ref().unwrap();
            //     assert_eq!(a, &hash_clone);
            // }).unwrap();
        }
        self.hash_id
    }

    pub(crate) fn set_readonly_runner(mut self) -> Result<Self, HashIdError> {
        self.hash_id.set_in_working_tree()?;
        Ok(self)
    }
}
