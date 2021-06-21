// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This sub module provides different KV alternatives for context persistence

use std::num::NonZeroUsize;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::EntryHash;

pub mod entries;
pub mod in_memory;
pub mod readonly_ipc;
pub mod stats;

pub const INMEM: &str = "inmem";

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashId(NonZeroUsize); // NonZeroUsize so that `Option<HashId>` is 8 bytes

impl Into<usize> for HashId {
    fn into(self) -> usize {
        self.0.get().checked_sub(1).unwrap()
    }
}

impl From<usize> for HashId {
    fn from(v: usize) -> Self {
        let v = v.checked_add(1).unwrap();
        HashId(NonZeroUsize::new(v).unwrap())
    }
}

pub struct VacantEntryHash<'a> {
    entry: Option<&'a mut EntryHash>,
    hash_id: HashId,
}

impl<'a> VacantEntryHash<'a> {
    pub(crate) fn write_with<F>(self, fun: F) -> HashId
    where
        F: FnOnce(&mut EntryHash),
    {
        if let Some(entry) = self.entry {
            fun(entry)
        };
        self.hash_id
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, EnumIter)]
pub enum SupportedContextKeyValueStore {
    InMem,
}

impl SupportedContextKeyValueStore {
    pub fn possible_values() -> Vec<&'static str> {
        let mut possible_values = Vec::new();
        for sp in SupportedContextKeyValueStore::iter() {
            possible_values.extend(sp.supported_values());
        }
        possible_values
    }

    fn supported_values(&self) -> Vec<&'static str> {
        match self {
            SupportedContextKeyValueStore::InMem => vec!["inmem"],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseKeyValueStoreBackendError(String);

impl FromStr for SupportedContextKeyValueStore {
    type Err = ParseKeyValueStoreBackendError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        for sp in SupportedContextKeyValueStore::iter() {
            if sp.supported_values().contains(&s.as_str()) {
                return Ok(sp);
            }
        }

        Err(ParseKeyValueStoreBackendError(format!(
            "Invalid variant name: {}",
            s
        )))
    }
}

pub mod test_support {
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::path::PathBuf;

    use strum::IntoEnumIterator;

    use crate::persistent::Persistable;
    use crate::working_tree::Entry;
    use crate::ContextKeyValueStore;
    use crate::EntryHash;

    use super::SupportedContextKeyValueStore;

    pub type TestKeyValueStoreError = failure::Error;
    pub type TestContextKvStoreFactoryInstance = Box<dyn TestContextKvStoreFactory>;

    pub fn blob(value: Vec<u8>) -> Entry {
        Entry::Blob(value)
    }

    pub fn entry_hash(key: &[u8]) -> EntryHash {
        assert!(key.len() < 32);
        let bytes: Vec<u8> = key
            .iter()
            .chain(std::iter::repeat(&0u8))
            .take(32)
            .cloned()
            .collect();

        EntryHash::try_from(bytes).unwrap()
    }

    pub fn blob_serialized(value: Vec<u8>) -> Vec<u8> {
        bincode::serialize(&blob(value)).unwrap()
    }

    pub fn all_kv_stores(
        _base_dir: PathBuf, // TODO - TE-261: not used anymore now
    ) -> HashMap<SupportedContextKeyValueStore, TestContextKvStoreFactoryInstance> {
        let mut store_factories: HashMap<
            SupportedContextKeyValueStore,
            TestContextKvStoreFactoryInstance,
        > = HashMap::new();

        for sckvs in SupportedContextKeyValueStore::iter() {
            let _ = match sckvs {
                SupportedContextKeyValueStore::InMem => store_factories.insert(
                    SupportedContextKeyValueStore::InMem,
                    Box::new(InMemoryBackendTestContextKvStoreFactory),
                ),
            };
        }

        assert_eq!(
            SupportedContextKeyValueStore::iter().count(),
            store_factories.len(),
            "There must be registered test factory for every supported kv-store!"
        );

        store_factories
    }

    pub trait TestContextKvStoreFactory: 'static + Send + Sync + Persistable {
        /// Creates new storage and also clean all existing data
        fn create(&self, name: &str) -> Result<Box<ContextKeyValueStore>, TestKeyValueStoreError>;
    }

    /// In-memory kv-store
    pub struct InMemoryBackendTestContextKvStoreFactory;

    impl TestContextKvStoreFactory for InMemoryBackendTestContextKvStoreFactory {
        fn create(&self, _: &str) -> Result<Box<ContextKeyValueStore>, TestKeyValueStoreError> {
            use crate::kv_store::in_memory::InMemory;
            Ok(Box::new(InMemory::new()))
        }
    }

    impl Persistable for InMemoryBackendTestContextKvStoreFactory {
        fn is_persistent(&self) -> bool {
            false
        }
    }
}
