use std::{collections::hash_map::{DefaultHasher, Entry::{Occupied, Vacant}}, convert::{TryFrom, TryInto}, hash::Hasher};

use crate::{Map, kv_store::entries::Entries};

use super::{storage::{NodeId, Storage}, string_interner::StringId};

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

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq)]
struct ShapeHash(u64);

struct Shape {
    hashes: Map<ShapeHash, Box<[StringId]>>,
    ids: Entries<ShapeId, ShapeHash>,
    temp: Vec<StringId>,
}

impl Shape {
    fn get_shape(&self, shape_id: ShapeId) -> Result<&[StringId], ShapeError> {
        let hash = match self.ids.get(shape_id)?.copied() {
            Some(hash) => hash,
            None => return Err(ShapeError::ShapeIdNotFound),
        };

        let slice = match self.hashes.get(&hash) {
            Some(slice) => slice,
            None => return Err(ShapeError::ShapeIdNotFound),
        };

        Ok(slice)
    }

    fn make_shape(&mut self, dir: &[(StringId, NodeId)], storage: &Storage) -> Result<Option<ShapeId>, ShapeError> {
        let mut temp = std::mem::take(&mut self.temp);

        let mut hasher = DefaultHasher::new();

        for (key_id, _) in dir {
            if key_id.is_big() {
                return Ok(None);
            }

            let key = storage.get_str(*key_id).map_err(|_| ShapeError::CannotFindKey)?;
            hasher.write(key.as_bytes());

            temp.push(*key_id);
        }

        let hashed = ShapeHash(hasher.finish());

        let mut new_shape = false;

        // match self.hashes.entry(hashed) {
        //     Occupied(entry) => {
        //         //entry.
        //     },
        //     Vacant(_) => todo!(),
        // }

        self.hashes
            .entry(hashed)
            .or_insert_with(|| {
                new_shape = true;
                Box::from(temp.as_slice())
            });

        self.temp = temp;

        if new_shape {
            self.ids.push(hashed).map(Some)
        } else {
            todo!()
        }
    }
}
