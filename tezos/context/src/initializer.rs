// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use ipc::IpcError;
use ocaml_interop::BoxRoot;
pub use tezos_context_api::ContextKvStoreConfiguration;
use tezos_context_api::TezosContextTezEdgeStorageConfiguration;
use thiserror::Error;

use crate::kv_store::in_memory::InMemory;
use crate::kv_store::persistent::Persistent;
use crate::kv_store::readonly_ipc::ReadonlyIpcBackend;
use crate::persistent::file::OpenFileError;
use crate::persistent::lock::LockDatabaseError;
use crate::serialize::DeserializationError;
use crate::{ContextKeyValueStore, PatchContextFunction, TezedgeContext, TezedgeIndex};

/// IPC communication errors
#[derive(Debug, Error)]
pub enum IndexInitializationError {
    #[error("Failure when initializing IPC context: {reason}")]
    IpcError {
        #[from]
        reason: IpcError,
    },
    #[error("Attempted to initialize an IPC context without a socket path")]
    IpcSocketPathMissing,
    #[error("Unexpected IO error occurred, {reason}")]
    IoError {
        #[from]
        reason: std::io::Error,
    },
    #[error("Deserialization error occured, {reason}")]
    DeserializationError {
        #[from]
        reason: DeserializationError,
    },
    #[error("Mutex/lock lock error! Reason: {reason}")]
    LockError { reason: String },
    #[error("Failed to open database file, {reason}")]
    OpenFileError {
        #[from]
        reason: OpenFileError,
    },
    #[error("Failed to lock database file, {reason}")]
    LockDatabaseError {
        #[from]
        reason: LockDatabaseError,
    },
    #[error("Sizes & checksums of the files do not match values in `sizes.db`")]
    InvalidIntegrity,
    #[error("Failed to join deserializing thread: {reason}")]
    ThreadJoinError { reason: String },
}

fn spawn_reload_database(
    repository: Arc<RwLock<ContextKeyValueStore>>,
) -> std::io::Result<JoinHandle<()>> {
    let thread = std::thread::Builder::new().name("db-reload".to_string());
    let (sender, recv) = std::sync::mpsc::channel();

    let result = thread.spawn(move || {
        log!("Reloading context");
        let mut repository = match repository.write() {
            Ok(repository) => repository,
            Err(e) => {
                elog!("Failed to lock repository for reloading: {:?}", e);
                return;
            }
        };

        // Notify the main thread that the repository is locked
        if let Err(e) = sender.send(()) {
            elog!("Failed to notify main thread that repo is locked: {:?}", e);
        }

        if let Err(e) = repository.reload_database() {
            elog!("Failed to reload repository: {:?}", e);
        }
        log!("Context reloaded");
    });

    // Wait for the spawned thread to lock the repository.
    // This is necessary to make sure that `TezedgeIndex` doesn't start
    // processing queries without having the repository synchronized
    // with data on disk
    if let Err(e) = recv.recv_timeout(Duration::from_secs(10)) {
        elog!("Failed to get notified that repo is locked: {:?}", e);
    }

    result
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
        ContextKvStoreConfiguration::InMem => Arc::new(RwLock::new(InMemory::try_new()?)),
        ContextKvStoreConfiguration::OnDisk(ref db_path) => {
            // TODO: Use `enable_checksums` from `ContextKvStoreConfiguration::OnDisk`
            let enable_checksums: bool = true;
            Arc::new(RwLock::new(Persistent::try_new(
                Some(db_path.as_str()),
                enable_checksums,
            )?))
        }
    };

    // We reload the database in another thread, to avoid blocking on
    // `initialize_tezedge_index`: Reloading can take lots of time
    if let Err(e) = spawn_reload_database(Arc::clone(&repository)) {
        elog!("Failed to spawn thread to reload database: {:?}", e);
    }

    Ok(TezedgeIndex::new(repository, patch_context))
}

pub fn initialize_tezedge_context(
    configuration: &TezosContextTezEdgeStorageConfiguration,
) -> Result<TezedgeContext, IndexInitializationError> {
    let index = initialize_tezedge_index(configuration, None)?;
    Ok(TezedgeContext::new(index, None, None))
}
