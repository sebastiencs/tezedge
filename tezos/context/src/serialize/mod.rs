use std::{
    array::TryFromSliceError, num::TryFromIntError, str::Utf8Error, string::FromUtf8Error,
    sync::Arc,
};

use modular_bitfield::prelude::*;
use tezos_timing::SerializeStats;
use thiserror::Error;

use crate::{
    kv_store::HashId,
    persistent::DBError,
    working_tree::{
        shape::DirectoryShapeError,
        storage::{Blob, DirEntryIdError, Storage, StorageError},
        string_interner::StringInterner,
        DirEntry, Object,
    },
    ContextKeyValueStore,
};

use self::persistent::AbsoluteOffset;

pub mod in_memory;
pub mod persistent;

const COMPACT_HASH_ID_BIT: u32 = 1 << 23;

const FULL_31_BITS: u32 = 0x7FFFFFFF;
const FULL_23_BITS: u32 = 0x7FFFFF;

pub type SerializeObjectSignature = fn(
    &Object,                       // object
    HashId,                        // object_hash_id
    &mut Vec<u8>,                  // output
    &Storage,                      // storage
    &StringInterner,               // strings
    &mut SerializeStats,           // statistics
    &mut Vec<(HashId, Arc<[u8]>)>, // batch
    &mut Vec<HashId>,              // referenced_older_objects
    &mut ContextKeyValueStore,     // repository
    Option<AbsoluteOffset>,        // offset
) -> Result<Option<AbsoluteOffset>, SerializationError>;

#[derive(BitfieldSpecifier)]
#[bits = 2]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum ObjectLength {
    OneByte,
    TwoBytes,
    FourBytes,
}

#[derive(BitfieldSpecifier)]
#[bits = 3]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum ObjectTag {
    Directory,
    Blob,
    Commit,
    InodePointers,
    ShapedDirectory,
}

#[bitfield(bits = 8)]
#[derive(Debug)]
pub struct ObjectHeader {
    #[allow(dead_code)] // `bitfield` generates unused method on this `tag`
    tag: ObjectTag,
    length: ObjectLength,
    is_persistent: bool,
    #[skip]
    _unused: B2,
}

impl ObjectHeader {
    pub fn get_length(&self) -> ObjectLength {
        self.length()
    }

    pub fn get_persistent(&self) -> bool {
        self.is_persistent()
    }
}

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
    #[error("Offset is missing")]
    MissingOffset,
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
    #[error("Directory shape error: {error:?}")]
    DirectoryShapeError {
        #[from]
        error: DirectoryShapeError,
    },
    #[error("IOError: {error:?}")]
    IOError {
        #[from]
        error: std::io::Error,
    },
}

fn get_inline_blob<'a>(storage: &'a Storage, dir_entry: &DirEntry) -> Option<Blob<'a>> {
    if let Some(Object::Blob(blob_id)) = dir_entry.get_object() {
        if blob_id.is_inline() {
            return storage.get_blob(blob_id).ok();
        }
    }
    None
}
