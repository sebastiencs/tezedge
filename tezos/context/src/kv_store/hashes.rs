// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::{TryFrom, TryInto};

use crate::{
    kv_store::{HashId, VacantObjectHash},
    persistent::DBError,
    ObjectHash,
};

use super::index_map::IndexMap;

/// Container for `ObjectHash`
///
/// It separates `ObjectHash` between the ones created with methods
/// such as `WorkingTree::hash` and the ones being commited.
pub struct HashesContainer {
    /// `ObjectHash` created during the working tree manipulation
    working_tree: IndexMap<HashId, ObjectHash>,
    /// `ObjectHash` ready to be commited to disk
    commiting: IndexMap<HashId, ObjectHash>,
    /// `true` when we create `ObjectHash` to must be commited
    is_commiting: bool,
    /// First `HashId` in `Self::working_tree` and `Self::commiting`
    first_index: usize,
}

impl HashesContainer {
    pub fn new(first_index: usize) -> Self {
        Self {
            working_tree: IndexMap::with_capacity(1000),
            commiting: IndexMap::new(),
            is_commiting: false,
            first_index,
        }
    }

    pub fn set_is_commiting(&mut self) {
        self.is_commiting = true;
    }

    pub fn commited(&mut self) {
        self.first_index += self.commiting.len();
        self.working_tree.clear();
        self.commiting.clear();
        self.is_commiting = false;
    }

    pub fn get_commiting(&self) -> &[ObjectHash] {
        self.commiting.as_slice()
    }

    pub fn total_number_of_hashes(&self) -> usize {
        if self.is_commiting {
            self.first_index + self.commiting.len()
        } else {
            self.first_index + self.working_tree.len()
        }
    }

    pub fn make_hash_id_ready_for_commit(&mut self, hash_id: HashId) -> Result<HashId, DBError> {
        if !self.is_commiting {
            // We are not commiting, keep the same HashId
            return Ok(hash_id);
        }

        // We are commiting, move the `ObjectHash` from `Self::working_tree`
        // into `Self::commiting` and return the new `HashId`.

        let hash_id = match hash_id.get_in_working_tree()? {
            Some(hash_id) => hash_id,
            None => return Ok(hash_id), // HashId is already correct (referring to `Self::commiting`)
        };

        // Get the index in `Self::working_tree`
        let working_tree_index: usize = hash_id.try_into()?;
        let working_tree_index = working_tree_index - self.first_index;

        // Get the `ObjectHash`
        let hash =
            self.working_tree
                .get_index(working_tree_index)
                .ok_or(DBError::HashNotFound {
                    object_ref: hash_id.into(),
                })?;

        // Push the `ObjectHash` into `Self::commiting`
        let commiting_index: usize = self.commiting.push(*hash)?.try_into()?;

        // Retrieve the `HashId`
        let hash_id = HashId::try_from(self.first_index + commiting_index)?;

        // Return the new `HashId`
        Ok(hash_id)
    }

    /// Return the `ObjectHash` if `hash_id` refers to a not yet commited hash
    ///
    /// Returns `None` if the hash was already commited
    pub fn try_get_hash(&self, hash_id: HashId) -> Result<Option<&ObjectHash>, DBError> {
        let (hash_id, hashes) = if let Some(hash_id) = hash_id.get_in_working_tree()? {
            (hash_id, &self.working_tree)
        } else {
            (hash_id, &self.commiting)
        };

        let hash_id_index: usize = hash_id.try_into()?;

        if hash_id_index < self.first_index {
            return Ok(None);
        }

        let hash_id_index = hash_id_index - self.first_index;

        match hashes.get_index(hash_id_index) {
            Some(hash) => Ok(Some(hash)),
            None => Err(DBError::HashNotFound {
                object_ref: hash_id.into(),
            }),
        }
    }

    pub fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        let hashes = if self.is_commiting {
            &mut self.commiting
        } else {
            &mut self.working_tree
        };

        let in_memory_length = hashes.len();
        let hash_id_index = self.first_index + in_memory_length;
        let mut hash_id = HashId::try_from(hash_id_index)?;

        let (_, entry) = hashes.get_vacant_entry()?;

        if !self.is_commiting {
            hash_id.set_in_working_tree()?;
        }

        Ok(VacantObjectHash::new(entry, hash_id))
    }
}
