// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{collections::{BTreeMap, btree_map::Entry::{Occupied, Vacant}, hash_map::{
        DefaultHasher,
        // Entry::{Occupied, Vacant},
    }}, convert::{TryFrom, TryInto}, hash::Hasher};

use crate::{kv_store::entries::Entries, Map};

use super::{
    storage::{NodeId, Storage},
    string_interner::StringId,
};

#[derive(Debug)]
enum ShapeError {
    ShapeIdNotFound,
    CannotFindKey,
    IdFromUSize,
}

#[derive(Debug, Clone, Copy)]
struct ShapeId(u32);

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

//#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq)]
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShapeHash(u64);

struct Shape {
    hashes: BTreeMap<ShapeHash, (ShapeId, Box<[StringId]>)>,
    ids: Entries<ShapeId, ShapeHash>,
    temp: Vec<StringId>,
}

impl Shape {
    fn new() -> Self {
        Self {
            hashes: BTreeMap::default(),
            ids: Entries::with_capacity(1024),
            temp: Vec::with_capacity(256),
        }
    }

    fn get_shape(&self, shape_id: ShapeId) -> Result<&[StringId], ShapeError> {
        let hash = match self.ids.get(shape_id)?.copied() {
            Some(hash) => hash,
            None => return Err(ShapeError::ShapeIdNotFound),
        };

        self.hashes
            .get(&hash)
            .map(|s| &*s.1)
            .ok_or(ShapeError::ShapeIdNotFound)
    }

    fn make_shape(
        &mut self,
        dir: &[(StringId, NodeId)],
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

        match self.hashes.entry(shape_hash) {
            Occupied(entry) => Ok(Some(entry.get().0)),
            Vacant(entry) => {
                let shape_id = self.ids.push(shape_hash)?;
                entry.insert((shape_id, Box::from(self.temp.as_slice())));
                Ok(Some(shape_id))
            }
        }
    }
}
