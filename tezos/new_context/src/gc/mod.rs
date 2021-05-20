// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This sub module provides different KV alternatives for context persistence

use std::array::TryFromSliceError;
use std::collections::{HashMap, HashSet};
use std::sync::PoisonError;

use blake2::digest::InvalidOutputSize;
use failure::Fail;

use crypto::hash::{FromBytesError, HashType};

use crate::hash::HashingError;
use crate::persistent::{DBError, KeyValueStoreBackend};
use crate::working_tree::Entry;
use crate::{ContextKeyValueStoreSchema, EntryHash};

pub mod mark_move_gced;
pub mod mark_sweep_gced;

pub trait GarbageCollector {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError>;

    fn block_applied(&mut self, commit: EntryHash) -> Result<(), GarbageCollectionError>;
}

pub trait NotGarbageCollected {}

impl<T: NotGarbageCollected> GarbageCollector for T {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        Ok(())
    }

    fn block_applied(&mut self, _commit: EntryHash) -> Result<(), GarbageCollectionError> {
        Ok(())
    }
}

/// helper function for fetching and deserializing entry from the store
pub fn fetch_entry_from_store(
    store: &dyn KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    hash: &EntryHash,
    path: &str,
) -> Result<Entry, GarbageCollectionError> {
    match store.get(&hash)? {
        None => Err(GarbageCollectionError::EntryNotFound {
            hash: HashType::ContextHash.hash_to_b58check(hash)?,
            path: path.to_string(),
        }),
        Some(entry_bytes) => Ok(bincode::deserialize(&entry_bytes)?),
    }
}

pub fn collect_hashes_recursively(
    entry: &Entry,
    entry_hash: &EntryHash,
    mut cache: HashMap<EntryHash, HashSet<EntryHash>>,
    store: &dyn KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    path: &mut String,
) -> Result<HashMap<EntryHash, HashSet<EntryHash>>, GarbageCollectionError> {
    let mut entries = HashSet::new();
    collect_hashes(entry, entry_hash, &mut entries, &mut cache, store, path)?;
    Ok(cache)
}

/// collects entries from tree like structure recursively
pub fn collect_hashes(
    entry: &Entry,
    entry_hash: &EntryHash,
    batch: &mut HashSet<EntryHash>,
    cache: &mut HashMap<EntryHash, HashSet<EntryHash>>,
    store: &dyn KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    path: &mut String,
) -> Result<(), GarbageCollectionError> {
    batch.insert(*entry_hash);

    match cache.get(entry_hash) {
        // if we know subtree already lets just use it
        Some(v) => {
            batch.extend(v);
            Ok(())
        }
        None => {
            match entry {
                Entry::Blob(_) => Ok(()),
                Entry::Tree(tree) => {
                    // FIXME: what is this comment about?
                    // Go through all descendants and gather errors. Remap error if there is a failure
                    // anywhere in the recursion paths. TODO: is revert possible?
                    let mut b = HashSet::new();
                    for (fragment, child_node) in tree.iter() {
                        let child_entry_hash = &child_node.entry_hash()?;
                        let base_len = path.len();
                        path.push('/');
                        path.push_str(&fragment.0);
                        let child_entry = fetch_entry_from_store(store, child_entry_hash, path)?;
                        collect_hashes(&child_entry, child_entry_hash, &mut b, cache, store, path)?;
                        path.truncate(base_len);
                    }
                    cache.insert(*entry_hash, b.clone());
                    batch.extend(b);
                    Ok(())
                }
                Entry::Commit(commit) => {
                    let root_entry = fetch_entry_from_store(store, &commit.root_hash, "/")?;
                    Ok(collect_hashes(
                        &root_entry,
                        &commit.root_hash,
                        batch,
                        cache,
                        store,
                        path,
                    )?)
                }
            }
        }
    }
}

#[derive(Debug, Fail)]
pub enum GarbageCollectionError {
    #[fail(display = "Column family {} is missing", name)]
    MissingColumnFamily { name: &'static str },
    #[fail(display = "Backend Error")]
    BackendError,
    #[fail(display = "SledDB error: {}", error)]
    SledDBError { error: sled::Error },
    #[fail(display = "Guard Poison {} ", error)]
    GuardPoison { error: String },
    #[fail(display = "Serialization error: {:?}", error)]
    SerializationError { error: bincode::Error },
    #[fail(display = "DBError error: {:?}", error)]
    DBError { error: DBError },
    #[fail(display = "Failed to convert hash to array: {}", error)]
    HashConversionError { error: TryFromSliceError },
    #[fail(display = "GarbageCollector error: {}", error)]
    GarbageCollectorError { error: String },
    #[fail(display = "Mutex/lock lock error! Reason: {:?}", reason)]
    LockError { reason: String },
    #[fail(display = "Entry not found in store: path={:?} hash={:?}", path, hash)]
    EntryNotFound { hash: String, path: String },
    #[fail(display = "Failed to convert hash into string: {}", error)]
    HashToStringError { error: FromBytesError },
    #[fail(display = "Failed to encode hash: {}", error)]
    HashingError { error: HashingError },
    #[fail(display = "Invalid output size")]
    InvalidOutputSize,
    #[fail(display = "Expected value instead of `None` for {}", _0)]
    ValueExpected(&'static str),
}

impl From<sled::Error> for GarbageCollectionError {
    fn from(error: sled::Error) -> Self {
        GarbageCollectionError::SledDBError { error }
    }
}

impl From<DBError> for GarbageCollectionError {
    fn from(error: DBError) -> Self {
        GarbageCollectionError::DBError { error }
    }
}

impl From<bincode::Error> for GarbageCollectionError {
    fn from(error: bincode::Error) -> Self {
        GarbageCollectionError::SerializationError { error }
    }
}

impl From<TryFromSliceError> for GarbageCollectionError {
    fn from(error: TryFromSliceError) -> Self {
        GarbageCollectionError::HashConversionError { error }
    }
}

impl<T> From<PoisonError<T>> for GarbageCollectionError {
    fn from(pe: PoisonError<T>) -> Self {
        GarbageCollectionError::LockError {
            reason: format!("{}", pe),
        }
    }
}

impl From<FromBytesError> for GarbageCollectionError {
    fn from(error: FromBytesError) -> Self {
        GarbageCollectionError::HashToStringError { error }
    }
}

impl From<InvalidOutputSize> for GarbageCollectionError {
    fn from(_: InvalidOutputSize) -> Self {
        GarbageCollectionError::InvalidOutputSize
    }
}

impl From<HashingError> for GarbageCollectionError {
    fn from(error: HashingError) -> Self {
        Self::HashingError { error }
    }
}

impl slog::Value for GarbageCollectionError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}
