// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use crate::kv_store::in_memory::InMemory;
use crate::kv_store::persistent::Persistent;
use crate::kv_store::readonly_ipc::ReadonlyIpcBackend;
use crate::persistent::file::OpenFileError;
use crate::persistent::lock::LockDatabaseError;
use crate::serialize::DeserializationError;
use crate::working_tree::string_interner::StringInterner;
use crate::{ContextKeyValueStore, PatchContextFunction, TezedgeContext, TezedgeIndex};
use ipc::IpcError;
use ocaml_interop::BoxRoot;
pub use tezos_context_api::ContextKvStoreConfiguration;
use tezos_context_api::TezosContextTezEdgeStorageConfiguration;
use thiserror::Error;

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
    #[error("Sizes of the files do not match values in `sizes.db`")]
    InvalidSizesOfFiles,
}

#[derive(Default)]
struct RepoWithStrings {
    readonly: Option<Arc<RwLock<ContextKeyValueStore>>>,
    writable: Option<Arc<RwLock<ContextKeyValueStore>>>,
    string_interner: Rc<RefCell<StringInterner>>,
}

thread_local! {
    static GLOBAL_REPO_WITH_STRINGS: RefCell<RepoWithStrings> = Default::default();
}

fn initialize_repository(
    configuration: &TezosContextTezEdgeStorageConfiguration,
) -> Result<
    (
        Arc<RwLock<ContextKeyValueStore>>,
        Rc<RefCell<StringInterner>>,
    ),
    IndexInitializationError,
> {
    let repository: Arc<RwLock<ContextKeyValueStore>> = match configuration.backend {
        ContextKvStoreConfiguration::ReadOnlyIpc => match configuration.ipc_socket_path.clone() {
            None => return Err(IndexInitializationError::IpcSocketPathMissing),
            Some(ipc_socket_path) => Arc::new(RwLock::new(ReadonlyIpcBackend::try_connect(
                ipc_socket_path,
            )?)),
        },
        ContextKvStoreConfiguration::InMem => Arc::new(RwLock::new(InMemory::try_new()?)),
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
        .take_strings_on_reload()
        .map(|s| Rc::new(RefCell::new(s)))
        .unwrap_or_default();

    Ok((repository, string_interner))
}

pub fn initialize_tezedge_index(
    configuration: &TezosContextTezEdgeStorageConfiguration,
    patch_context: Option<BoxRoot<PatchContextFunction>>,
) -> Result<TezedgeIndex, IndexInitializationError> {
    use ContextKvStoreConfiguration::*;

    // `initialize_tezedge_index` might be called multiple times:
    // At least twice: to initialize the readonly context and the writable one.
    //
    // We must handle when `initialize_tezedge_index` is called more than once
    // for the same backend.
    // We must not create twice the repository with the same backend.
    // For example, if `initialize_tezedge_index` is called twice for the
    // readonly backend, we must reuse the previously initialized readonly backend.

    let (repository, string_interner) = GLOBAL_REPO_WITH_STRINGS.with(|global| {
        let mut global = global.borrow_mut();

        match (&configuration.backend, &global.readonly, &global.writable) {
            (ReadOnlyIpc, None, _) => {
                // We want to initialize the readonly context, and it hasn't been
                // created yet
                let (repo, _) = initialize_repository(configuration).unwrap();
                global.readonly = Some(Arc::clone(&repo));

                (repo, None)
            }
            (ReadOnlyIpc, Some(readonly_repo), _) => (
                // The readonly context was already initialized, use the one
                // previsouly created
                Arc::clone(readonly_repo),
                None,
            ),
            (_, _, None) => {
                // Initialize the writable context, it hasn't been created yet
                let (repo, strings) = initialize_repository(configuration).unwrap();
                global.writable = Some(Arc::clone(&repo));
                global.string_interner = Rc::clone(&strings);

                (repo, Some(strings))
            }
            (_, _, Some(writable_repo)) => (
                // Re-use the previously initialized writable context
                Arc::clone(writable_repo),
                Some(Rc::clone(&global.string_interner)),
            ),
        }
    });

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_context_only_once() {
        let writable = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
            backend: ContextKvStoreConfiguration::InMem,
            ipc_socket_path: None,
        })
        .unwrap();

        let writable2 = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
            backend: ContextKvStoreConfiguration::InMem,
            ipc_socket_path: None,
        })
        .unwrap();

        // We must get the same pointers, and not initialize different repositories
        assert!(Arc::ptr_eq(
            &writable.index.repository,
            &writable2.index.repository
        ));
        assert!(Rc::ptr_eq(
            &writable.index.string_interner,
            &writable2.index.string_interner
        ));
    }
}
