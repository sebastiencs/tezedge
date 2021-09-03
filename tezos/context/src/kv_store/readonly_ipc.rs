// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Implementation of a repository that is accessed through IPC calls.
//! It is used by read-only protocol runners to be able to access the in-memory context
//! owned by the writable protocol runner.

use std::{borrow::Cow, path::Path, sync::Arc};

use crypto::hash::ContextHash;
use failure::Error;
use slog::{error, info};
use tezos_timing::RepositoryMemoryUsage;

use crate::persistent::{DBError, Flushable, Persistable};
use crate::working_tree::shape::{ShapeError, ShapeId, ShapeStrings};
use crate::working_tree::storage::{DirEntryId, Storage};
use crate::working_tree::string_interner::{StringId, StringInterner};
use crate::ContextValue;
use crate::{
    ffi::TezedgeIndexError, gc::NotGarbageCollected, persistent::KeyValueStoreBackend, ObjectHash,
};

pub struct ReadonlyIpcBackend {
    client: IpcContextClient,
    hashes: HashValueStore,
}

// TODO - TE-261: quick hack to make the initializer happy, but must be fixed.
// Probably needs a separate thread for the controller, and communication
// should happen through a channel.
unsafe impl Send for ReadonlyIpcBackend {}
unsafe impl Sync for ReadonlyIpcBackend {}

impl ReadonlyIpcBackend {
    /// Connects the IPC backend to a socket in `socket_path`. This operation is blocking.
    /// Will wait for a few seconds if the socket file is not found yet.
    pub fn try_connect<P: AsRef<Path>>(socket_path: P) -> Result<Self, IpcError> {
        let client = IpcContextClient::try_connect(socket_path)?;
        Ok(Self {
            client,
            hashes: HashValueStore::new(None),
        })
    }
}

impl NotGarbageCollected for ReadonlyIpcBackend {}

impl KeyValueStoreBackend for ReadonlyIpcBackend {
    fn write_batch(&mut self, _batch: Vec<(HashId, Arc<[u8]>)>) -> Result<(), DBError> {
        // This context is readonly
        Ok(())
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        if let Some(hash_id) = hash_id.get_readonly_id()? {
            self.hashes.contains(hash_id).map_err(Into::into)
        } else {
            self.client
                .contains_object(hash_id)
                .map_err(|reason| DBError::IpcAccessError { reason })
        }
    }

    fn put_context_hash(&mut self, _hash_id: HashId) -> Result<(), DBError> {
        // This context is readonly
        Ok(())
    }

    fn get_context_hash(&self, context_hash: &ContextHash) -> Result<Option<HashId>, DBError> {
        self.client
            .get_context_hash_id(context_hash)
            .map_err(|reason| DBError::IpcAccessError { reason })
    }

    fn get_hash(&self, hash_id: HashId) -> Result<Option<Cow<ObjectHash>>, DBError> {
        if let Some(hash_id) = hash_id.get_readonly_id()? {
            Ok(self.hashes.get_hash(hash_id)?.map(Cow::Borrowed))
        } else {
            self.client
                .get_hash(hash_id)
                .map_err(|reason| DBError::IpcAccessError { reason })
        }
    }

    fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, DBError> {
        if let Some(hash_id) = hash_id.get_readonly_id()? {
            Ok(self.hashes.get_value(hash_id)?.map(Cow::Borrowed))
        } else {
            self.client
                .get_value(hash_id)
                .map_err(|reason| DBError::IpcAccessError { reason })
        }
    }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        self.hashes
            .get_vacant_object_hash()?
            .set_readonly_runner()
            .map_err(Into::into)
    }

    fn clear_objects(&mut self) -> Result<(), DBError> {
        self.hashes.clear();
        Ok(())
    }

    fn memory_usage(&self) -> RepositoryMemoryUsage {
        self.hashes.get_memory_usage()
    }

    fn get_shape(&self, shape_id: ShapeId) -> Result<ShapeStrings, DBError> {
        let res = self
            .client
            .get_shape(shape_id)
            .map_err(|reason| DBError::IpcAccessError { reason });

        eprintln!("GET_SHAPE RESULT {:?}", res);

        res.map(ShapeStrings::Owned)
    }

    fn make_shape(
        &mut self,
        _dir: &[(StringId, DirEntryId)],
        _storage: &Storage,
    ) -> Result<Option<ShapeId>, DBError> {
        Ok(None)
    }

    fn update_strings(&mut self, _string_interner: &StringInterner) -> Result<(), DBError> {
        Ok(())
    }

    fn take_new_strings(&self) -> Result<Option<StringInterner>, DBError> {
        let res = self
            .client
            .get_string_interner()
            .map(Some)
            .map_err(|reason| DBError::IpcAccessError { reason });

        eprintln!("TAKE_NEW_STRINGS RESULT={:?}", res);

        res
    }

    fn clone_string_interner(&self) -> Option<StringInterner> {
        None
    }
}

impl Flushable for ReadonlyIpcBackend {
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl Persistable for ReadonlyIpcBackend {
    fn is_persistent(&self) -> bool {
        false
    }
}

// IPC communication

use std::{cell::RefCell, time::Duration};

use failure::Fail;
use ipc::{IpcClient, IpcError, IpcReceiver, IpcSender, IpcServer};
use serde::{Deserialize, Serialize};
use slog::{warn, Logger};
use strum_macros::IntoStaticStr;

use super::{in_memory::HashValueStore, HashId, VacantObjectHash};

/// This request is generated by a readonly protool runner and is received by the writable protocol runner.
#[derive(Clone, Serialize, Deserialize, Debug, IntoStaticStr)]
enum ContextRequest {
    GetContextHashId(ContextHash),
    GetHash(HashId),
    GetValue(HashId),
    GetShape(ShapeId),
    ContainsObject(HashId),
    GetStringInterner,
    ShutdownCall, // TODO: is this required?
}

/// This is generated as a response to the `ContextRequest` command.
#[derive(Serialize, Deserialize, Debug, IntoStaticStr)]
enum ContextResponse {
    GetContextHashResponse(Result<Option<ObjectHash>, String>),
    GetContextHashIdResponse(Result<Option<HashId>, String>),
    GetValueResponse(Result<Option<ContextValue>, String>),
    GetShapeResponse(Result<Vec<String>, String>),
    ContainsObjectResponse(Result<bool, String>),
    GetStringInternerResponse(Result<StringInterner, String>),
    ShutdownResult,
}

#[derive(Fail, Debug)]
pub enum ContextError {
    #[fail(display = "Context get object error: {}", reason)]
    GetValueError { reason: String },
    #[fail(display = "Context get shape error: {}", reason)]
    GetShapeError { reason: String },
    #[fail(display = "Context contains object error: {}", reason)]
    ContainsObjectError { reason: String },
    #[fail(display = "Context get hash id error: {}", reason)]
    GetContextHashIdError { reason: String },
    #[fail(display = "Context get hash error: {}", reason)]
    GetContextHashError { reason: String },
    #[fail(display = "Context get string interner error: {}", reason)]
    GetStringInternerError { reason: String },
}

#[derive(Fail, Debug)]
pub enum IpcContextError {
    #[fail(display = "Could not obtain a read lock to the TezEdge index")]
    TezedgeIndexReadLockError,
    #[fail(display = "IPC error: {}", reason)]
    IpcError { reason: IpcError },
}

impl From<TezedgeIndexError> for IpcContextError {
    fn from(_: TezedgeIndexError) -> Self {
        Self::TezedgeIndexReadLockError
    }
}

impl From<IpcError> for IpcContextError {
    fn from(error: IpcError) -> Self {
        IpcContextError::IpcError { reason: error }
    }
}

/// Errors generated by `protocol_runner`.
#[derive(Fail, Debug)]
pub enum ContextServiceError {
    /// Generic IPC communication error. See `reason` for more details.
    #[fail(display = "IPC error: {}", reason)]
    IpcError { reason: IpcError },
    /// Tezos protocol error.
    #[fail(display = "Protocol error: {}", reason)]
    ContextError { reason: ContextError },
    /// Unexpected message was received from IPC channel
    #[fail(display = "Received unexpected message: {}", message)]
    UnexpectedMessage { message: &'static str },
    /// Lock error
    #[fail(display = "Lock error: {:?}", message)]
    LockPoisonError { message: String },
}

impl<T> From<std::sync::PoisonError<T>> for ContextServiceError {
    fn from(source: std::sync::PoisonError<T>) -> Self {
        Self::LockPoisonError {
            message: source.to_string(),
        }
    }
}

impl slog::Value for ContextServiceError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

impl From<IpcError> for ContextServiceError {
    fn from(error: IpcError) -> Self {
        ContextServiceError::IpcError { reason: error }
    }
}

impl From<ContextError> for ContextServiceError {
    fn from(error: ContextError) -> Self {
        ContextServiceError::ContextError { reason: error }
    }
}

/// IPC context server that listens for new connections.
pub struct IpcContextListener(IpcServer<ContextRequest, ContextResponse>);

pub struct ContextIncoming<'a> {
    listener: &'a mut IpcContextListener,
}

struct IpcClientIO {
    rx: IpcReceiver<ContextResponse>,
    tx: IpcSender<ContextRequest>,
}

struct IpcServerIO {
    rx: IpcReceiver<ContextRequest>,
    tx: IpcSender<ContextResponse>,
}

/// Encapsulate IPC communication.
pub struct IpcContextClient {
    io: RefCell<IpcClientIO>,
}

pub struct IpcContextServer {
    io: RefCell<IpcServerIO>,
}

/// IPC context client for readers.
impl IpcContextClient {
    const TIMEOUT: Duration = Duration::from_secs(180);

    pub fn try_connect<P: AsRef<Path>>(socket_path: P) -> Result<Self, IpcError> {
        // TODO - TE-261: do this in a better way
        for _ in 0..5 {
            if socket_path.as_ref().exists() {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let ipc_client: IpcClient<ContextResponse, ContextRequest> = IpcClient::new(socket_path);
        let (rx, tx) = ipc_client.connect()?;
        let io = RefCell::new(IpcClientIO { rx, tx });
        Ok(Self { io })
    }

    /// Get object by hash id
    pub fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ContextRequest::GetValue(hash_id))?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::GetValueResponse(result) => result
                .map(|h| h.map(Cow::Owned))
                .map_err(|err| ContextError::GetValueError { reason: err }.into()),
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Get object by hash id
    pub fn get_string_interner(&self) -> Result<StringInterner, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ContextRequest::GetStringInterner)?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::GetStringInternerResponse(result) => result.map_err(|err| {
                eprintln!("Fail to get string interner {:?}", err);
                ContextError::GetStringInternerError { reason: err }.into()
            }),
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Check if object with hash id exists
    pub fn contains_object(&self, hash_id: HashId) -> Result<bool, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ContextRequest::ContainsObject(hash_id))?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::ContainsObjectResponse(result) => {
                result.map_err(|err| ContextError::ContainsObjectError { reason: err }.into())
            }
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Check if object with hash id exists
    pub fn get_context_hash_id(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<HashId>, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ContextRequest::GetContextHashId(context_hash.clone()))?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::GetContextHashIdResponse(result) => {
                result.map_err(|err| ContextError::GetContextHashIdError { reason: err }.into())
            }
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Check if object with hash id exists
    pub fn get_hash(
        &self,
        hash_id: HashId,
    ) -> Result<Option<Cow<ObjectHash>>, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ContextRequest::GetHash(hash_id))?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::GetContextHashResponse(result) => result
                .map(|h| h.map(Cow::Owned))
                .map_err(|err| ContextError::GetContextHashError { reason: err }.into()),
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Get object by hash id
    pub fn get_shape(&self, shape_id: ShapeId) -> Result<Vec<String>, ContextServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ContextRequest::GetShape(shape_id))?;

        // this might take a while, so we will use unusually long timeout
        match io
            .rx
            .try_receive(Some(Self::TIMEOUT), Some(IpcContextListener::IO_TIMEOUT))?
        {
            ContextResponse::GetShapeResponse(result) => result
                // .map()
                .map_err(|err| ContextError::GetShapeError { reason: err }.into()),
            message => Err(ContextServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }
}

impl<'a> Iterator for ContextIncoming<'a> {
    type Item = Result<IpcContextServer, IpcError>;
    fn next(&mut self) -> Option<Result<IpcContextServer, IpcError>> {
        Some(self.listener.accept())
    }
}

impl IpcContextListener {
    const IO_TIMEOUT: Duration = Duration::from_secs(180);

    /// Create new IPC endpoint
    pub fn try_new<P: AsRef<Path>>(socket_path: P) -> Result<Self, IpcError> {
        // Remove file first, otherwise bind will fail.
        std::fs::remove_file(&socket_path).ok();

        Ok(IpcContextListener(IpcServer::bind_path(socket_path)?))
    }

    /// Start accepting incoming IPC connections.
    ///
    /// Returns an [`ipc context server`](IpcContextServer) if new IPC channel is successfully created.
    /// This is a blocking operation.
    pub fn accept(&mut self) -> Result<IpcContextServer, IpcError> {
        let (rx, tx) = self.0.accept()?;

        Ok(IpcContextServer {
            io: RefCell::new(IpcServerIO { rx, tx }),
        })
    }

    /// Returns an iterator over the connections being received on this context IPC listener.
    pub fn incoming(&mut self) -> ContextIncoming<'_> {
        ContextIncoming { listener: self }
    }

    /// Starts accepting connections.
    ///
    /// A new thread is launched to serve each connection.
    pub fn handle_incoming_connections(&mut self, log: &Logger) {
        for connection in self.incoming() {
            match connection {
                Err(err) => {
                    error!(&log, "Error accepting IPC connection"; "reason" => format!("{:?}", err))
                }
                Ok(server) => {
                    info!(
                        &log,
                        "IpcContextServer accepted new IPC connection for context"
                    );
                    let log_inner = log.clone();
                    if let Err(spawn_error) = std::thread::Builder::new()
                        .name("ctx-ipc-server-thread".to_string())
                        .spawn(move || {
                            info!(&log_inner, "IpcContextServer start processing requests",);

                            if let Err(err) = server.process_context_requests(&log_inner) {
                                error!(
                                    &log_inner,
                                    "Error when processing context IPC requests";
                                    "reason" => format!("{:?}", err),
                                );
                            }
                        })
                    {
                        error!(
                            &log,
                            "Failed to spawn thread to IpcContextServer";
                            "reason" => spawn_error,
                        );
                    }

                    info!(&log, "IpcContextServer spawned new thread",);
                }
            }
        }
    }
}

impl IpcContextServer {
    /// Listen to new connections from context readers.
    /// Begin receiving commands from context readers until `ShutdownCall` command is received.
    pub fn process_context_requests(&self, log: &Logger) -> Result<(), IpcContextError> {
        let mut io = self.io.borrow_mut();
        loop {
            let cmd = io.rx.receive()?;

            let cmd_clone = cmd.clone();

            info!(log, "IpcContextServer processing {:?}", cmd,);

            match cmd {
                ContextRequest::GetValue(hash) => match crate::ffi::get_context_index()? {
                    None => io.tx.send(&ContextResponse::GetValueResponse(Err(
                        "Context index unavailable".to_owned(),
                    )))?,
                    Some(index) => {
                        let res = index
                            .fetch_object_bytes(hash)
                            .map_err(|err| format!("Context error: {:?}", err));
                        io.tx.send(&ContextResponse::GetValueResponse(res))?;
                    }
                },
                ContextRequest::GetShape(shape_id) => match crate::ffi::get_context_index()? {
                    None => io.tx.send(&ContextResponse::GetShapeResponse(Err(
                        "Context index unavailable".to_owned(),
                    )))?,
                    Some(index) => {
                        let res = index
                            .repository
                            .read()
                            .map_err(|_| ContextError::GetShapeError {
                                reason: "Fail to get repo".to_string(),
                            })
                            .and_then(|repo| {
                                let storage = index.storage.borrow();

                                let shape = repo.get_shape(shape_id).map_err(|_| {
                                    ContextError::GetStringInternerError {
                                        reason: "Fail to get string interner".to_string(),
                                    }
                                })?;

                                let shape = match shape {
                                    ShapeStrings::Ids(slice) => storage.string_to_owned(slice),
                                    ShapeStrings::Owned(_) => todo!(),
                                };

                                Ok(shape.unwrap())
                            })
                            .map_err(|err| format!("Context error: {:?}", err));

                        // let repo = index.repository.read().unwrap();
                        // let shape = repo.get_shape(shape_id).unwrap();

                        io.tx.send(&ContextResponse::GetShapeResponse(res))?;

                        // io.tx
                        //     .send(&ContextResponse::GetShapeResponse(Ok(shape.to_vec())))?;
                    }
                },
                ContextRequest::ContainsObject(hash) => match crate::ffi::get_context_index()? {
                    None => io.tx.send(&ContextResponse::GetValueResponse(Err(
                        "Context index unavailable".to_owned(),
                    )))?,
                    Some(index) => {
                        let res = index
                            .contains(hash)
                            .map_err(|err| format!("Context error: {:?}", err));
                        io.tx.send(&ContextResponse::ContainsObjectResponse(res))?;
                    }
                },

                ContextRequest::ShutdownCall => {
                    if let Err(e) = io.tx.send(&ContextResponse::ShutdownResult) {
                        warn!(log, "Failed to send shutdown response"; "reason" => format!("{}", e));
                    }

                    break;
                }
                ContextRequest::GetContextHashId(context_hash) => {
                    match crate::ffi::get_context_index()? {
                        None => io.tx.send(&ContextResponse::GetContextHashIdResponse(Err(
                            "Context index unavailable".to_owned(),
                        )))?,
                        Some(index) => {
                            let res = index
                                .fetch_context_hash_id(&context_hash)
                                .map_err(|err| format!("Context error: {:?}", err));

                            io.tx
                                .send(&ContextResponse::GetContextHashIdResponse(res))?;
                        }
                    }
                }
                ContextRequest::GetHash(hash_id) => match crate::ffi::get_context_index()? {
                    None => io.tx.send(&ContextResponse::GetContextHashResponse(Err(
                        "Context index unavailable".to_owned(),
                    )))?,
                    Some(index) => {
                        let res = index
                            .fetch_hash(hash_id)
                            .map_err(|err| format!("Context error: {:?}", err));

                        io.tx.send(&ContextResponse::GetContextHashResponse(res))?;
                    }
                },
                ContextRequest::GetStringInterner => match crate::ffi::get_context_index()? {
                    None => io.tx.send(&ContextResponse::GetStringInternerResponse(Err(
                        "Context index unavailable".to_owned(),
                    )))?,
                    Some(index) => {
                        let res = index
                            .repository
                            .read()
                            .map_err(|_| ContextError::GetStringInternerError {
                                reason: "Fail to get repo".to_string(),
                            })
                            .and_then(|repo| {
                                repo.clone_string_interner().ok_or(
                                    ContextError::GetStringInternerError {
                                        reason: "Fail to get string interner".to_string(),
                                    },
                                )
                            })
                            .map_err(|err| format!("Context error: {:?}", err));

                        io.tx
                            .send(&ContextResponse::GetStringInternerResponse(res))?;

                        // let repo = index.repository.read().unwrap();

                        // let string_interner = repo.clone_string_interner().unwrap();

                        // io.tx.send(&ContextResponse::GetStringInternerResponse(Ok(
                        //     string_interner,
                        // )))?;
                    }
                },
            }

            info!(log, "IpcContextServer processed {:?}", cmd_clone,);
        }

        Ok(())
    }
}
