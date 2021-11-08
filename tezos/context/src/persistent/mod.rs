// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    borrow::Cow,
    convert::TryFrom,
    fs::OpenOptions,
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use crypto::hash::ContextHash;
use thiserror::Error;

use tezos_timing::{RepositoryMemoryUsage, SerializeStats};

use crate::{
    kv_store::{readonly_ipc::ContextServiceError, HashId, HashIdError, VacantObjectHash},
    serialize::{persistent::AbsoluteOffset, DeserializationError},
    working_tree::{
        shape::{DirectoryShapeError, DirectoryShapeId, ShapeStrings},
        storage::{DirEntryId, Storage},
        string_interner::{StringId, StringInterner},
        working_tree::WorkingTree,
        Object, ObjectReference,
    },
    ContextError, ContextKeyValueStore, ObjectHash,
};

pub trait Flushable {
    fn flush(&self) -> Result<(), anyhow::Error>;
}

pub trait Persistable {
    fn is_persistent(&self) -> bool;
}

pub trait KeyValueStoreBackend {
    /// Check if database contains given hash id
    ///
    /// # Arguments
    /// * `hash_id` - HashId, to be checked for existence
    fn contains(&self, hash_id: HashId) -> Result<bool, DBError>;
    /// Mark the HashId as a ContextHash
    ///
    /// # Arguments
    /// * `hash_id` - HashId to mark
    fn put_context_hash(&mut self, object_ref: ObjectReference) -> Result<(), DBError>;
    /// Get the HashId corresponding to the ContextHash
    ///
    /// # Arguments
    /// * `context_hash` - ContextHash to find the HashId
    fn get_context_hash(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<ObjectReference>, DBError>;
    /// Read hash associated with given HashId, if exists.
    ///
    /// # Arguments
    /// * `hash_id` - HashId of the ObjectHash
    fn get_hash(&self, object_ref: ObjectReference) -> Result<Cow<ObjectHash>, DBError>;
    /// Find an object to insert a new ObjectHash
    /// Return the object
    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError>;
    /// Memory usage
    fn memory_usage(&self) -> RepositoryMemoryUsage;
    /// Returns the strings of the directory shape
    fn get_shape(&self, shape_id: DirectoryShapeId) -> Result<ShapeStrings, DBError>;
    /// Returns the `ShapeId` of this `dir`
    ///
    /// Create a new shape when it doesn't exist.
    /// This returns `None` when a shape cannot be made (currently if one of the
    /// string is > 30 bytes).
    fn make_shape(
        &mut self,
        dir: &[(StringId, DirEntryId)],
        storage: &Storage,
    ) -> Result<Option<DirectoryShapeId>, DBError>;
    /// Returns the string associated to this `string_id`.
    ///
    /// The string interner must have been updated with the `update_strings` method.
    fn get_str(&self, string_id: StringId) -> Option<&str>;
    /// Update the repository `StringInterner` to be in sync with `string_interner`.
    fn synchronize_strings_from(&mut self, string_interner: &StringInterner);
    /// Update `string_interner` to be in sync with the repository `StringInterner`.
    fn synchronize_strings_into(&self, string_interner: &mut StringInterner);

    fn get_object(
        &self,
        object_ref: ObjectReference,
        storage: &mut Storage,
        strings: &mut StringInterner,
    ) -> Result<Object, DBError>;

    fn get_object_bytes<'a>(
        &self,
        object_ref: ObjectReference,
        buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], DBError>;

    fn commit(
        &mut self,
        working_tree: &WorkingTree,
        parent_commit_ref: Option<ObjectReference>,
        author: String,
        message: String,
        date: u64,
    ) -> Result<(ContextHash, Box<SerializeStats>), DBError>;

    fn get_hash_id(&self, object_ref: ObjectReference) -> Result<HashId, DBError>;

    /// [test-only]
    #[cfg(test)]
    fn synchronize_data(
        &mut self,
        batch: &[(HashId, Arc<[u8]>)],
        output: &[u8],
    ) -> Result<Option<AbsoluteOffset>, DBError>;
}

/// Possible errors for schema
#[derive(Debug, Error)]
pub enum DBError {
    #[error("Database incompatibility {name}")]
    DatabaseIncompatibility { name: String },
    #[error("Value already exists {key}")]
    ValueExists { key: String },
    #[error("Found wrong structure. Was looking for {sought}, but found {found}")]
    FoundUnexpectedStructure { sought: String, found: String },
    #[error("Guard Poison {error} ")]
    GuardPoison { error: String },
    #[error("Mutex/lock lock error! Reason: {reason}")]
    LockError { reason: String },
    #[error("I/O error {error}")]
    IOError {
        #[from]
        error: io::Error,
    },
    #[error("MemoryStatisticsOverflow")]
    MemoryStatisticsOverflow,
    #[error("IPC Context access error: {reason:?}")]
    IpcAccessError { reason: ContextServiceError },
    #[error("Missing object: {object_ref:?}")]
    MissingObject { object_ref: ObjectReference },
    #[error("Conversion from/to HashId failed")]
    HashIdFailed,
    #[error("Deserialization error: {error:?}")]
    DeserializationError {
        #[from]
        error: DeserializationError,
    },
    #[error("Shape error: {error:?}")]
    ShapeError {
        #[from]
        error: DirectoryShapeError,
    },
    #[error("Context error: {error:?}")]
    ContextError {
        #[from]
        error: Box<ContextError>,
    },
    #[error("Hash not found: {object_ref:?}")]
    HashNotFound { object_ref: ObjectReference },
}

impl From<HashIdError> for DBError {
    fn from(_: HashIdError) -> Self {
        DBError::HashIdFailed
    }
}

impl<T> From<PoisonError<T>> for DBError {
    fn from(pe: PoisonError<T>) -> Self {
        DBError::LockError {
            reason: format!("{}", pe),
        }
    }
}

pub(crate) fn get_commit_hash(
    commit_ref: ObjectReference,
    repo: &ContextKeyValueStore,
) -> Result<ContextHash, ContextError> {
    let commit_hash = repo.get_hash(commit_ref)?;
    let commit_hash = ContextHash::try_from(&commit_hash[..])?;
    Ok(commit_hash)
}

#[derive(Debug)]
pub enum FileType {
    ShapeDirectories,
    ShapeDirectoriesIndex,
    CommitIndex,
    Data,
    Strings,
    BigStrings,
    BigStringsOffsets,
    Hashes,
}

const PERSISTENT_BASE_PATH: &str = "db_persistent";

impl FileType {
    fn get_path(&self) -> &Path {
        match self {
            FileType::ShapeDirectories => Path::new("shape_directories.db"),
            FileType::ShapeDirectoriesIndex => Path::new("shape_directories_index.db"),
            FileType::CommitIndex => Path::new("commit_index.db"),
            FileType::Data => Path::new("data.db"),
            FileType::Strings => Path::new("strings.db"),
            FileType::Hashes => Path::new("hashes.db"),
            FileType::BigStrings => Path::new("big_strings.db"),
            FileType::BigStringsOffsets => Path::new("big_strings_offsets.db"),
        }
    }
}

pub struct File {
    file: std::fs::File,
    offset: u64,
}

/// Absolute offset in the file
#[derive(Debug)]
pub struct FileOffset(pub u64);

// #[cfg(test)]
lazy_static::lazy_static! {
    static ref BASE_PATH_EXCLU: Arc<Mutex<()>> = {
        Arc::new(Mutex::new(()))
    };
}

// #[cfg(test)]
fn create_random_path() -> String {
    use rand::Rng;

    let mut rng = rand::thread_rng();

    // Avoid data races with `Path::exists` below
    let _guard = BASE_PATH_EXCLU.lock().unwrap();

    let mut path = format!("{}/{}", PERSISTENT_BASE_PATH, rng.gen::<u32>());

    while Path::new(&path).exists() {
        path = format!("{}/{}", PERSISTENT_BASE_PATH, rng.gen::<u32>());
    }

    path
}

pub fn get_persistent_base_path() -> String {
    match std::env::var("PERSISTENT_PATH").ok() {
        Some(path) => path,
        None => create_random_path(),
    }

    // #[cfg(not(test))]
    // return PERSISTENT_BASE_PATH.to_string();

    // #[cfg(test)]
    // return create_random_path();
}

impl File {
    pub fn new(base_path: &str, file_type: FileType) -> Self {
        println!(
            "FILE={:?}",
            PathBuf::from(base_path).join(file_type.get_path())
        );

        std::fs::create_dir_all(&base_path).unwrap();

        use std::os::unix::fs::OpenOptionsExt;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .append(true)
            .create(true)
            .custom_flags(libc::O_NOATIME)
            .open(PathBuf::from(base_path).join(file_type.get_path()))
            .unwrap();

        // We use seek, in cases metadatas were not synchronized
        let offset = file.seek(SeekFrom::End(0)).unwrap();

        Self { file, offset }
    }

    pub fn offset(&self) -> AbsoluteOffset {
        self.offset.into()
    }

    pub fn sync(&mut self) {
        self.file.sync_data().unwrap();
        // self.file.sync_all().unwrap();
    }

    pub fn append(&mut self, bytes: impl AsRef<[u8]>) {
        let bytes = bytes.as_ref();

        self.offset += bytes.len() as u64;
        self.file.write_all(bytes).unwrap();
    }

    pub fn read_at(&self, buffer: &mut Vec<u8>, offset: AbsoluteOffset) {
        use std::os::unix::prelude::FileExt;

        self.file.read_at(buffer, offset.as_u64()).unwrap();
    }

    pub fn read_exact_at(&self, buffer: &mut [u8], offset: AbsoluteOffset) {
        use std::os::unix::prelude::FileExt;

        self.file.read_exact_at(buffer, offset.as_u64()).unwrap();
    }

    pub fn try_read<'a>(&self, mut buffer: &'a mut [u8], offset: AbsoluteOffset) -> &'a [u8] {
        let buf_len = buffer.len();

        let eof = self.offset as usize;
        let end = offset.as_u64() as usize + buf_len;

        if eof < end {
            buffer = &mut buffer[..buf_len - (end - eof)];
        }

        self.read_exact_at(buffer, offset);

        buffer
    }
}
