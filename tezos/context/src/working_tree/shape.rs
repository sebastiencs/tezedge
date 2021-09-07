// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    collections::{
        btree_map::Entry::{Occupied, Vacant},
        hash_map::DefaultHasher,
        BTreeMap,
    },
    convert::{TryFrom, TryInto},
    hash::Hasher,
};

use crate::kv_store::index_map::IndexMap;
use serde::{Deserialize, Serialize};

use super::{
    storage::{DirEntryId, Storage},
    string_interner::StringId,
};

#[derive(Debug)]
pub enum ShapeError {
    ShapeIdNotFound,
    CannotFindKey,
    IdFromUSize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ShapeId(u32);

impl TryInto<usize> for ShapeId {
    type Error = ShapeError;

    fn try_into(self) -> Result<usize, Self::Error> {
        Ok(self.0 as usize)
    }
}

impl TryFrom<usize> for ShapeId {
    type Error = ShapeError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let value: u32 = value.try_into().map_err(|_| ShapeError::IdFromUSize)?;
        Ok(Self(value))
    }
}

impl ShapeId {
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl From<u32> for ShapeId {
    fn from(shape_id: u32) -> Self {
        Self(shape_id)
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShapeHash(u64);

/// Contains the shape (keys) of a directory.
///
/// A `ShapeId` maps to a slice of `StringId`
pub struct Shapes {
    /// Map `ShapeHash` to its `ShapeId` and strings
    hash_to_strings: BTreeMap<ShapeHash, (ShapeId, Box<[StringId]>)>,
    /// Map the `ShapeId` to its `ShapeHash`.
    id_to_hash: IndexMap<ShapeId, ShapeHash>,
    temp: Vec<StringId>,
}

impl Default for Shapes {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ShapeStrings<'a> {
    Ids(&'a [StringId]),
    Owned(Vec<String>),
}

impl Shapes {
    pub fn new() -> Self {
        Self {
            hash_to_strings: BTreeMap::default(),
            id_to_hash: IndexMap::with_capacity(1024),
            temp: Vec::with_capacity(256),
        }
    }

    pub fn get_shape(&self, shape_id: ShapeId) -> Result<&[StringId], ShapeError> {
        let hash = match self.id_to_hash.get(shape_id)?.copied() {
            Some(hash) => hash,
            None => return Err(ShapeError::ShapeIdNotFound),
        };

        self.hash_to_strings
            .get(&hash)
            .map(|s| &*s.1)
            .ok_or(ShapeError::ShapeIdNotFound)
    }

    pub fn get_shape_owned(
        &self,
        shape_id: ShapeId,
        storage: &Storage,
    ) -> Result<Vec<String>, ShapeError> {
        let shape = self.get_shape(shape_id)?;

        shape
            .iter()
            .map(|s| {
                storage
                    .get_str(*s)
                    .map_err(|_| ShapeError::CannotFindKey)
                    .map(|s| s.to_string())
            })
            .collect()
    }

    pub fn make_shape(
        &mut self,
        dir: &[(StringId, DirEntryId)],
        storage: &Storage,
    ) -> Result<Option<ShapeId>, ShapeError> {
        self.temp.clear();

        let mut hasher = DefaultHasher::new();
        hasher.write_usize(dir.len());

        for (key_id, _) in dir {
            if key_id.is_big() {
                return Ok(None);
            }

            let key = storage
                .get_str(*key_id)
                .map_err(|_| ShapeError::CannotFindKey)?;

            hasher.write(key.as_bytes());
            self.temp.push(*key_id);
        }

        let shape_hash = ShapeHash(hasher.finish());

        match self.hash_to_strings.entry(shape_hash) {
            Occupied(entry) => Ok(Some(entry.get().0)),
            Vacant(entry) => {
                let shape_id = self.id_to_hash.push(shape_hash)?;
                entry.insert((shape_id, Box::from(self.temp.as_slice())));
                Ok(Some(shape_id))
            }
        }
    }
}
