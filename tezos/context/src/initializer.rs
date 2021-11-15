// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::{Arc, RwLock};

use ipc::IpcError;
use ocaml_interop::BoxRoot;
pub use tezos_context_api::ContextKvStoreConfiguration;
use tezos_context_api::TezosContextTezEdgeStorageConfiguration;
use thiserror::Error;

use crate::kv_store::in_memory::InMemory;
use crate::kv_store::persistent::Persistent;
use crate::kv_store::readonly_ipc::ReadonlyIpcBackend;
use crate::serialize::DeserializationError;
use crate::{ContextKeyValueStore, PatchContextFunction, TezedgeContext, TezedgeIndex};

/// IPC communication errors
#[derive(Debug, Error)]
pub enum IndexInitializationError {
    #[error("Failure when initializing IPC context: {reason}")]
    IpcError { reason: IpcError },
    #[error("Attempted to initialize an IPC context without a socket path")]
    IpcSocketPathMissing,
    #[error("Unexpected IO error occurred, {reason}")]
    IoError { reason: std::io::Error },
    #[error("Deserialization error occured, {reason}")]
    DeserializationError {
        #[from]
        reason: DeserializationError,
    },
    #[error("Mutex/lock lock error! Reason: {reason}")]
    LockError { reason: String },
}

impl From<IpcError> for IndexInitializationError {
    fn from(error: IpcError) -> Self {
        Self::IpcError { reason: error }
    }
}

impl From<std::io::Error> for IndexInitializationError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError { reason: error }
    }
}

pub fn initialize_tezedge_index(
    configuration: &TezosContextTezEdgeStorageConfiguration,
    patch_context: Option<BoxRoot<PatchContextFunction>>,
) -> Result<TezedgeIndex, IndexInitializationError> {
    let repository: Arc<RwLock<ContextKeyValueStore>> = match configuration.backend {
        ContextKvStoreConfiguration::ReadOnlyIpc => match configuration.ipc_socket_path.clone() {
            None => return Err(IndexInitializationError::IpcSocketPathMissing),
            Some(ipc_socket_path) => Arc::new(RwLock::new(ReadonlyIpcBackend::try_connect(
                ipc_socket_path,
            )?)),
        },
        ContextKvStoreConfiguration::InMem => Arc::new(RwLock::new(Persistent::try_new(None)?)),
        // ContextKvStoreConfiguration::InMem => Arc::new(RwLock::new(InMemory::try_new()?)),
        ContextKvStoreConfiguration::OnDisk(ref db_path) => {
            Arc::new(RwLock::new(Persistent::try_new(Some(db_path.as_str()))?))
        }
    };

    // When the context is reloaded/restarted, the existings strings (found the the db file)
    // are in the repository.
    // We want `TezedgeIndex` to have its string interner updated with the one
    // from the repository.
    // This assumes that `initialize_tezedge_index` is called only once.
    let string_interner = repository
        .write()
        .map_err(|e| IndexInitializationError::LockError {
            reason: format!("{:?}", e),
        })?
        .take_strings_on_reload();

    Ok(TezedgeIndex::new(
        repository,
        patch_context,
        string_interner,
    ))
}

pub fn initialize_tezedge_context(
    configuration: &TezosContextTezEdgeStorageConfiguration,
) -> Result<TezedgeContext, IndexInitializationError> {
    let index = initialize_tezedge_index(configuration, None)?;
    Ok(TezedgeContext::new(index, None, None))
}
