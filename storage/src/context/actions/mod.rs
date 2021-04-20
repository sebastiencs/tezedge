// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;

use bytes::Buf;
use failure::Fail;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use tezos_context::channel::ContextAction;

use crate::context::merkle::hash::HashingError;
use crate::context::{EntryHash, TreeId};

pub mod action_file;
pub mod action_file_storage;
pub mod context_action_storage;

pub const ROCKSDB: &str = "rocksdb";

#[derive(PartialEq, Debug, Clone, EnumIter)]
pub enum ContextActionStoreBackend {
    NoneBackend,
    RocksDB,
    FileStorage { path: PathBuf },
}

impl ContextActionStoreBackend {
    pub fn possible_values() -> Vec<&'static str> {
        let mut possible_values = Vec::new();
        for sp in ContextActionStoreBackend::iter() {
            possible_values.extend(sp.supported_values());
        }
        possible_values
    }

    pub fn supported_values(&self) -> Vec<&'static str> {
        match self {
            ContextActionStoreBackend::RocksDB => vec![ROCKSDB],
            ContextActionStoreBackend::FileStorage { .. } => vec!["file"],
            ContextActionStoreBackend::NoneBackend => vec!["none"],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseContextActionStoreBackendError(String);

impl FromStr for ContextActionStoreBackend {
    type Err = ParseContextActionStoreBackendError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        for sp in ContextActionStoreBackend::iter() {
            if sp.supported_values().contains(&s.as_str()) {
                return Ok(sp);
            }
        }

        Err(ParseContextActionStoreBackendError(format!(
            "Invalid variant name: {}",
            s
        )))
    }
}

pub trait ActionRecorder {
    fn record(&mut self, action: &ContextAction) -> Result<(), ActionRecorderError>;
}

#[derive(Debug, Fail)]
pub enum ActionRecorderError {
    #[fail(display = "Failed to store action, reason: {}", reason)]
    StoreError { reason: String },
    #[fail(display = "Missing actions for block {}.", hash)]
    MissingActions { hash: String },
}

pub fn get_new_tree_hash(action: &ContextAction) -> Result<Option<EntryHash>, HashingError> {
    match &action {
        ContextAction::Set { new_tree_hash, .. }
        | ContextAction::Copy { new_tree_hash, .. }
        | ContextAction::Delete { new_tree_hash, .. }
        | ContextAction::RemoveRecursively { new_tree_hash, .. } => match new_tree_hash.as_ref() {
            Some(hash) => {
                let mut buffer: EntryHash = [0; 32];
                hash.reader()
                    .read_exact(&mut buffer)
                    .map_err(|e| HashingError::InvalidHash(format!("{}", e)))?;
                Ok(Some(buffer))
            }
            None => Ok(None),
        },
        ContextAction::Get { .. }
        | ContextAction::Mem { .. }
        | ContextAction::DirMem { .. }
        | ContextAction::Commit { .. }
        | ContextAction::Fold { .. }
        | ContextAction::Checkout { .. }
        | ContextAction::Shutdown => Ok(None),
    }
}

pub fn get_tree_id(action: &ContextAction) -> Option<TreeId> {
    match &action {
        ContextAction::Get { tree_id, .. }
        | ContextAction::Mem { tree_id, .. }
        | ContextAction::DirMem { tree_id, .. }
        | ContextAction::Set { tree_id, .. }
        | ContextAction::Copy { tree_id, .. }
        | ContextAction::Delete { tree_id, .. }
        | ContextAction::RemoveRecursively { tree_id, .. }
        | ContextAction::Commit { tree_id, .. }
        | ContextAction::Fold { tree_id, .. } => Some(*tree_id),
        ContextAction::Checkout { .. } | ContextAction::Shutdown => None,
    }
}

pub fn get_new_tree_id(action: &ContextAction) -> Option<TreeId> {
    match &action {
        | ContextAction::Set { new_tree_id, .. }
        | ContextAction::Copy { new_tree_id, .. }
        | ContextAction::Delete { new_tree_id, .. }
        | ContextAction::RemoveRecursively { new_tree_id, .. } => Some(*new_tree_id),
        _ => None
    }
}

pub fn get_operation_hash(action: &ContextAction) -> Option<Vec<u8>> {
    match &action {
        ContextAction::Get { operation_hash, .. }
        | ContextAction::Mem { operation_hash, .. }
        | ContextAction::DirMem { operation_hash, .. }
        | ContextAction::Set { operation_hash, .. }
        | ContextAction::Copy { operation_hash, .. }
        | ContextAction::Delete { operation_hash, .. }
        | ContextAction::RemoveRecursively { operation_hash, .. }
        // | ContextAction::Commit { operation_hash, .. }
        | ContextAction::Fold { operation_hash, .. } => operation_hash.clone(),
        ContextAction::Checkout { .. }
        | ContextAction::Shutdown => None,
        | ContextAction::Commit { .. } => None,
    }
}
