use std::{array::TryFromSliceError, num::TryFromIntError, str::Utf8Error, string::FromUtf8Error};

use thiserror::Error;

use crate::{
    persistent::DBError,
    working_tree::{
        storage::{Blob, DirEntryIdError, Storage, StorageError},
        DirEntry, Object,
    },
};

pub mod in_memory;
pub mod persistent;

const ID_DIRECTORY: u8 = 0;
const ID_BLOB: u8 = 1;
const ID_COMMIT: u8 = 2;
const ID_INODE_POINTERS: u8 = 3;
const ID_SHAPED_DIRECTORY: u8 = 4;

const COMPACT_HASH_ID_BIT: u32 = 1 << 23;

const FULL_31_BITS: u32 = 0x7FFFFFFF;
const FULL_23_BITS: u32 = 0x7FFFFF;

#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("IOError {error}")]
    IOError {
        #[from]
        error: std::io::Error,
    },
    #[error("Directory not found")]
    DirNotFound,
    #[error("Directory entry not found")]
    DirEntryNotFound,
    #[error("Blob not found")]
    BlobNotFound,
    #[error("Conversion from int failed: {error}")]
    TryFromIntError {
        #[from]
        error: TryFromIntError,
    },
    #[error("StorageIdError: {error}")]
    StorageIdError {
        #[from]
        error: StorageError,
    },
    #[error("HashId too big")]
    HashIdTooBig,
    #[error("Missing HashId")]
    MissingHashId,
    #[error("DBError: {error}")]
    DBError {
        #[from]
        error: DBError,
    },
    #[error("Missing Offset")]
    MissingOffset,
}

#[derive(Debug, Error)]
pub enum DeserializationError {
    #[error("Unexpected end of file")]
    UnexpectedEOF,
    #[error("Conversion from slice to an array failed")]
    TryFromSliceError {
        #[from]
        error: TryFromSliceError,
    },
    #[error("Bytes are not valid utf-8: {error}")]
    Utf8Error {
        #[from]
        error: Utf8Error,
    },
    #[error("UnknownID")]
    UnknownID,
    #[error("Vector is not valid utf-8: {error}")]
    FromUtf8Error {
        #[from]
        error: FromUtf8Error,
    },
    #[error("Root hash is missing")]
    MissingRootHash,
    #[error("Hash is missing")]
    MissingHash,
    #[error("DirEntryIdError: {error}")]
    DirEntryIdError {
        #[from]
        error: DirEntryIdError,
    },
    #[error("StorageIdError: {error:?}")]
    StorageIdError {
        #[from]
        error: StorageError,
    },
    #[error("Inode not found in repository")]
    InodeNotFoundInRepository,
    #[error("Inode empty in repository")]
    InodeEmptyInRepository,
    #[error("DBError: {error:?}")]
    DBError {
        #[from]
        error: Box<DBError>,
    },
    #[error("Cannot find next shape")]
    CannotFindNextShape,
}

fn get_inline_blob<'a>(storage: &'a Storage, dir_entry: &DirEntry) -> Option<Blob<'a>> {
    if let Some(Object::Blob(blob_id)) = dir_entry.get_object() {
        if blob_id.is_inline() {
            return storage.get_blob(blob_id).ok();
        }
    }
    None
}
