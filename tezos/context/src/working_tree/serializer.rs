// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Serialization/deserialization for objects in the Working Tree so that they can be
//! saved/loaded to/from the repository.

use std::{
    array::TryFromSliceError, borrow::Cow, convert::TryInto, io::Write, num::TryFromIntError,
    str::Utf8Error, string::FromUtf8Error, sync::Arc,
};

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;
use tezos_timing::SerializeStats;
use thiserror::Error;

use crate::{
    kv_store::HashId,
    persistent::DBError,
    working_tree::{
        shape::ShapeStrings,
        storage::{DirectoryId, Inode, PointerToInode},
        Commit, DirEntryKind, ObjectReference,
    },
    ContextKeyValueStore,
};

use super::{
    shape::DirectoryShapeId,
    storage::{Blob, DirEntryId, DirEntryIdError, InodeId, Storage, StorageError},
    string_interner::StringId,
    DirEntry, Object,
};

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
}

fn get_inline_blob<'a>(storage: &'a Storage, dir_entry: &DirEntry) -> Option<Blob<'a>> {
    if let Some(Object::Blob(blob_id)) = dir_entry.get_object() {
        if blob_id.is_inline() {
            return storage.get_blob(blob_id).ok();
        }
    }
    None
}

#[bitfield(bits = 8)]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct DirEntryShapeHeader {
    kind: DirEntryKind,
    blob_inline_length: B3,
    offset_length: OffsetLength,
    #[skip]
    _unused: B2,
}

// Must fit in 1 byte
assert_eq_size!(DirEntryShapeHeader, u8);

#[bitfield(bits = 8)]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct DirEntryHeader {
    kind: DirEntryKind,
    blob_inline_length: B3,
    key_inline_length: B2,
    offset_length: OffsetLength,
}

// Must fit in 1 byte
assert_eq_size!(DirEntryHeader, u8);

#[derive(BitfieldSpecifier)]
#[bits = 5]
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
    tag: ObjectTag,
    length: ObjectLength,
}

impl ObjectHeader {
    pub fn get_length(&self) -> ObjectLength {
        self.length()
    }
}

#[derive(BitfieldSpecifier)]
#[bits = 2]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
enum OffsetLength {
    RelativeOneByte,
    RelativeTwoBytes,
    RelativeFourBytes,
    RelativeEightBytes,
}

fn get_relative_offset(current_offset: u64, target_offset: u64) -> (u64, OffsetLength) {
    assert!(current_offset >= target_offset);

    let relative_offset = current_offset - target_offset;
    // let negative_offset = -(relative_offset as i64);

    if relative_offset <= 0xFF {
        (relative_offset, OffsetLength::RelativeOneByte)
    } else if relative_offset <= 0xFFFF {
        (relative_offset, OffsetLength::RelativeTwoBytes)
    } else if relative_offset <= 0xFFFFFFFF {
        (relative_offset, OffsetLength::RelativeFourBytes)
    } else {
        (relative_offset, OffsetLength::RelativeEightBytes)
    }
}

fn serialize_offset(
    output: &mut Vec<u8>,
    relative_offset: u64,
    offset_length: OffsetLength,
    // current_offset: u64,
    // target_offset: u64,
) {
    // println!("SERIALIZING OFFSET {:?} {:?}", relative_offset, offset_length);
    match offset_length {
        OffsetLength::RelativeOneByte => {
            let offset: u8 = relative_offset as u8;
            output.write_all(&offset.to_le_bytes()).unwrap();
        }
        OffsetLength::RelativeTwoBytes => {
            let offset: u16 = relative_offset as u16;
            output.write_all(&offset.to_le_bytes()).unwrap();
        }
        OffsetLength::RelativeFourBytes => {
            let offset: u32 = relative_offset as u32;
            output.write_all(&offset.to_le_bytes()).unwrap();
        }
        OffsetLength::RelativeEightBytes => {
            output.write_all(&relative_offset.to_le_bytes()).unwrap();
        }
    }
}

fn serialize_shaped_directory(
    shape_id: DirectoryShapeId,
    dir: &[(StringId, DirEntryId)],
    offset: u64,
    output: &mut Vec<u8>,
    storage: &Storage,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    let start = output.len();

    // Replaced by ObjectHeader
    output.write_all(&[0, 0])?;

    let shape_id = shape_id.as_u32();
    output.write_all(&shape_id.to_ne_bytes())?;

    // Make sure that SHAPED_DIRECTORY_NBYTES_TO_HASHES is correct.
    debug_assert_eq!(output[start + 2..].len(), SHAPED_DIRECTORY_NBYTES_TO_HASHES);

    for (_, dir_entry_id) in dir {
        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let dir_entry_offset = dir_entry.get_offset();
        let (relative_offset, offset_length) = get_relative_offset(offset, dir_entry_offset);

        if dir_entry_offset != 0 {
            // println!("OFFSET={:?}", dir_entry_offset);
        }

        let hash_id: u32 = dir_entry.hash_id().map(|h| h.as_u32()).unwrap_or(0);
        let kind = dir_entry.dir_entry_kind();

        let blob_inline = get_inline_blob(storage, &dir_entry);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        let byte: [u8; 1] = DirEntryShapeHeader::new()
            .with_kind(kind)
            .with_offset_length(offset_length)
            // .with_key_inline_length(0)
            .with_blob_inline_length(blob_inline_length as u8)
            .into_bytes();
        output.write_all(&byte[..])?;

        if let Some(blob_inline) = blob_inline {
            output.write_all(&blob_inline)?;
        } else {
            let _nbytes = serialize_hash_id(hash_id, output)?;

            serialize_offset(output, relative_offset, offset_length);
            // output.write_all(&dir_entry_offset.to_ne_bytes())?;
        }
    }

    write_object_header(output, start, ObjectTag::ShapedDirectory);

    stats.nshapes = stats.nshapes.saturating_add(1);

    Ok(())
}

fn write_object_header(
    output: &mut Vec<u8>,
    start: usize,
    //length: usize,
    tag: ObjectTag,
) {
    let length = output.len() - start;

    // println!("WRITE_OBJECT_HEADER LENGTH={:?} TAG={:?} START={:?}", length, tag, start);

    if length <= 0xFF {
        let header: [u8; 1] = ObjectHeader::new()
            .with_tag(tag)
            .with_length(ObjectLength::OneByte)
            .into_bytes();

        output[start] = header[0];
        output[start + 1] = length as u8;
    } else if length <= (0xFFFF - 1) {
        output.push(0);

        let end = output.len();
        output.copy_within(start + 2..end - 1, start + 3);

        let header: [u8; 1] = ObjectHeader::new()
            .with_tag(tag)
            .with_length(ObjectLength::TwoBytes)
            .into_bytes();

        let length: u16 = length as u16 + 1;

        output[start] = header[0];
        output[start + 1..start + 3].copy_from_slice(&length.to_le_bytes());
    } else {
        output.extend_from_slice(&[0, 0, 0]);

        let end = output.len();
        output.copy_within(start + 2..end - 3, start + 5);

        let header: [u8; 1] = ObjectHeader::new()
            .with_tag(tag)
            .with_length(ObjectLength::FourBytes)
            .into_bytes();

        let length: u32 = length as u32 + 3;

        output[start] = header[0];
        output[start + 1..start + 5].copy_from_slice(&length.to_le_bytes());
    }
}

fn serialize_directory_or_shape(
    dir: &[(StringId, DirEntryId)],
    offset: u64,
    output: &mut Vec<u8>,
    storage: &Storage,
    repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    if let Some(shape_id) = repository.make_shape(dir, storage)? {
        serialize_shaped_directory(shape_id, dir, offset, output, storage, stats)
    } else {
        serialize_directory(dir, offset, output, storage, repository, stats)
    }
}

fn serialize_directory(
    dir: &[(StringId, DirEntryId)],
    offset: u64,
    output: &mut Vec<u8>,
    storage: &Storage,
    repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    let mut keys_length: usize = 0;
    let mut hash_ids_length: usize = 0;
    let mut highest_hash_id: u32 = 0;
    let mut nblobs_inlined: usize = 0;
    let mut blobs_length: usize = 0;

    let start = output.len();

    // Replaced by ObjectHeader
    output.write_all(&[0, 0])?;

    // output.write_all(&[ID_DIRECTORY])?;

    // // Replaced by the length
    // output.write_all(&[0, 0, 0, 0])?;

    for (key_id, dir_entry_id) in dir {
        let key = storage.get_str(*key_id)?;
        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let hash_id: u32 = dir_entry.hash_id().map(|h| h.as_u32()).unwrap_or(0);
        let kind = dir_entry.dir_entry_kind();

        let blob_inline = get_inline_blob(storage, &dir_entry);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        let dir_entry_offset = dir_entry.get_offset();
        let (relative_offset, offset_length) = get_relative_offset(offset, dir_entry_offset);

        match key.len() {
            len if len != 0 && len < 4 => {
                let byte: [u8; 1] = DirEntryHeader::new()
                    .with_kind(kind)
                    .with_key_inline_length(len as u8)
                    .with_blob_inline_length(blob_inline_length as u8)
                    .with_offset_length(offset_length)
                    .into_bytes();

                output.write_all(&byte[..])?;
                output.write_all(key.as_bytes())?;
                keys_length += len;
            }
            len => {
                let byte: [u8; 1] = DirEntryHeader::new()
                    .with_kind(kind)
                    .with_key_inline_length(0)
                    .with_blob_inline_length(blob_inline_length as u8)
                    .with_offset_length(offset_length)
                    .into_bytes();
                output.write_all(&byte[..])?;

                let key_length: u16 = len.try_into()?;
                output.write_all(&key_length.to_ne_bytes())?;
                output.write_all(key.as_bytes())?;
                keys_length += 2 + key.len();
            }
        }

        if let Some(blob_inline) = blob_inline {
            nblobs_inlined += 1;
            blobs_length += blob_inline.len();

            output.write_all(&blob_inline)?;
        } else {
            let nbytes = serialize_hash_id(hash_id, output)?;

            serialize_offset(output, relative_offset, offset_length);

            hash_ids_length += nbytes;
            highest_hash_id = highest_hash_id.max(hash_id);
        }
    }

    write_object_header(output, start, ObjectTag::Directory);

    // let length = output.len() as u32 - start as u32;
    // output[start as usize + 1..start as usize + 5].copy_from_slice(&length.to_ne_bytes());

    stats.add_directory(
        hash_ids_length,
        keys_length,
        highest_hash_id,
        nblobs_inlined,
        blobs_length,
    );

    Ok(())
}

pub fn serialize_object(
    object: &Object,
    object_hash_id: HashId,
    output: &mut Vec<u8>,
    storage: &Storage,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>,
    referenced_older_objects: &mut Vec<HashId>,
    repository: &mut ContextKeyValueStore,
    offset: u64,
) -> Result<u64, SerializationError> {
    let start = output.len();
    let mut offset = start as u64 + offset;

    // output.clear();

    match object {
        Object::Directory(dir_id) => {
            if let Some(inode_id) = dir_id.get_inode_id() {
                offset = serialize_inode(
                    inode_id,
                    output,
                    object_hash_id,
                    storage,
                    stats,
                    batch,
                    referenced_older_objects,
                    repository,
                    offset,
                )?;
            } else {
                let dir = storage.get_small_dir(*dir_id)?;

                serialize_directory_or_shape(dir, offset, output, storage, repository, stats)?;

                batch.push((object_hash_id, Arc::from(&output[start..])));
            }
        }
        Object::Blob(blob_id) => {
            debug_assert!(!blob_id.is_inline());

            let blob = storage.get_blob(*blob_id)?;

            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;

            // output.write_all(&[ID_BLOB])?;

            // let length: u32 = blob.len() as u32;
            // let length = length + 5;
            // output.write_all(&length.to_ne_bytes())?;

            output.write_all(blob.as_ref())?;

            write_object_header(output, start, ObjectTag::Blob);

            stats.add_blob(blob.len());

            batch.push((object_hash_id, Arc::from(&output[start..])));
        }
        Object::Commit(commit) => {
            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;

            // output.write_all(&[ID_COMMIT])?;

            // // Replaced by the length
            // output.write_all(&[0, 0, 0, 0])?;

            let parent_hash_id = commit.parent_commit_hash.map(|h| h.as_u32()).unwrap_or(0);
            serialize_hash_id(parent_hash_id, output)?;

            let root_hash_id = commit.root_hash_ref.hash_id().as_u32();
            serialize_hash_id(root_hash_id, output)?;

            let root_hash_offset: u64 = commit.root_hash_ref.offset();

            // TODO: Use smaller offsets, add a header here
            // let (relative_offset, offset_length) = get_relative_offset(offset, root_hash_offset);
            // serialize_offset(output, relative_offset, offset_length);

            output.write_all(&root_hash_offset.to_ne_bytes())?;

            output.write_all(&commit.time.to_ne_bytes())?;

            let author_length: u32 = commit.author.len().try_into()?;
            output.write_all(&author_length.to_ne_bytes())?;
            output.write_all(commit.author.as_bytes())?;

            // The message length is inferred.
            // It's until the end of the slice
            output.write_all(commit.message.as_bytes())?;

            write_object_header(output, start, ObjectTag::Commit);

            // let length = output.len() as u32 - start as u32;
            // output[start as usize + 1..start as usize + 5].copy_from_slice(&length.to_ne_bytes());

            batch.push((object_hash_id, Arc::from(&output[start..])));
        }
    };

    stats.total_bytes += output.len();

    Ok(offset)
}

fn serialize_hash_id(hash_id: u32, output: &mut Vec<u8>) -> Result<usize, SerializationError> {
    if hash_id & FULL_23_BITS == hash_id {
        // The HashId fits in 23 bits

        // Set `COMPACT_HASH_ID_BIT` so the deserializer knows the `HashId` is in 3 bytes
        let hash_id: u32 = hash_id | COMPACT_HASH_ID_BIT;
        let hash_id: [u8; 4] = hash_id.to_be_bytes();

        output.write_all(&hash_id[1..])?;
        Ok(3)
    } else if hash_id & FULL_31_BITS == hash_id {
        // HashId fits in 31 bits

        output.write_all(&hash_id.to_be_bytes())?;
        Ok(4)
    } else {
        // The HashId must not be 32 bits because we use the
        // MSB to determine if the HashId is compact or not
        Err(SerializationError::HashIdTooBig)
    }
}

/// Describes which pointers are set and at what index.
///
/// `Inode::Pointers` is an array of 32 pointers.
/// Each pointer is either set (`Some`) or not set (`None`).
///
/// Example:
/// Let's say that there are 2 pointers sets in the array, at the index
/// 1 and 7.
/// This would be represented in this bitfield as:
/// `0b01000001_00000000_00000000`
///
#[derive(Copy, Clone, Default, Debug)]
struct PointersHeader {
    bitfield: u32,
}

impl PointersHeader {
    /// Set bit at index in the bitfield
    fn set(&mut self, index: usize) {
        self.bitfield |= 1 << index;
    }

    /// Get bit at index in the bitfield
    fn get(&self, index: usize) -> bool {
        self.bitfield & 1 << index != 0
    }

    fn to_bytes(&self) -> [u8; 4] {
        self.bitfield.to_ne_bytes()
    }

    /// Iterates on all the bit sets in the bitfield.
    ///
    /// The iterator returns the index of the bit.
    fn iter(&self) -> PointersDescriptorIterator {
        PointersDescriptorIterator {
            bitfield: *self,
            current: 0,
        }
    }

    fn from_bytes(bytes: [u8; 4]) -> Self {
        Self {
            bitfield: u32::from_ne_bytes(bytes),
        }
    }

    /// Count number of bit set in the bitfield.
    fn count(&self) -> u8 {
        self.bitfield.count_ones() as u8
    }
}

impl From<&[Option<PointerToInode>; 32]> for PointersHeader {
    fn from(pointers: &[Option<PointerToInode>; 32]) -> Self {
        let mut bitfield = Self::default();

        for (index, pointer) in pointers.iter().enumerate() {
            if pointer.is_some() {
                bitfield.set(index);
            }
        }

        bitfield
    }
}

/// Iterates on all the bit sets in the bitfield.
///
/// The iterator returns the index of the bit.
struct PointersDescriptorIterator {
    bitfield: PointersHeader,
    current: usize,
}

impl Iterator for PointersDescriptorIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        for index in self.current..32 {
            if self.bitfield.get(index) {
                self.current = index + 1;
                return Some(index);
            }
        }

        None
    }
}

#[derive(Default, Debug)]
struct PointersOffsetsHeader {
    bitfield: u64,
}

impl PointersOffsetsHeader {
    fn set(&mut self, index: usize, offset_length: OffsetLength) {
        assert!(index < 32);

        let bits: u64 = match offset_length {
            OffsetLength::RelativeOneByte => 0,
            OffsetLength::RelativeTwoBytes => 1,
            OffsetLength::RelativeFourBytes => 2,
            OffsetLength::RelativeEightBytes => 3,
        };

        self.bitfield |= bits << (index * 2);

        assert_eq!(self.get(index), offset_length)
    }

    fn get(&self, index: usize) -> OffsetLength {
        assert!(index < 32);

        let bits = (self.bitfield >> (index * 2)) & 0b11;

        match bits {
            0 => OffsetLength::RelativeOneByte,
            1 => OffsetLength::RelativeTwoBytes,
            2 => OffsetLength::RelativeFourBytes,
            _ => OffsetLength::RelativeEightBytes,
        }
    }

    /// Sets bits to zero at `index`
    #[cfg(test)]
    fn clear(&mut self, index: usize) {
        self.bitfield = self.bitfield & !(0b11 << (index * 2));
    }

    fn from_pointers(object_offset: u64, pointers: &[Option<PointerToInode>; 32]) -> Self {
        let mut bitfield = Self::default();

        for (index, pointer) in pointers.iter().filter_map(|p| p.as_ref()).enumerate() {
            let p_offset = pointer.offset();

            let (_, offset_length) = get_relative_offset(object_offset, p_offset);

            bitfield.set(index, offset_length);
        }

        bitfield
    }

    fn to_bytes(&self) -> [u8; 8] {
        self.bitfield.to_le_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        Self {
            bitfield: u64::from_ne_bytes(bytes),
        }
    }
}

fn serialize_inode(
    inode_id: InodeId,
    output: &mut Vec<u8>,
    hash_id: HashId,
    storage: &Storage,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>,
    referenced_older_objects: &mut Vec<HashId>,
    repository: &mut ContextKeyValueStore,
    off: u64,
) -> Result<u64, SerializationError> {
    use SerializationError::*;

    let mut start = output.len();
    let mut offset = start as u64 + off;

    // output.clear();
    let inode = storage.get_inode(inode_id)?;

    match inode {
        Inode::Pointers {
            depth,
            nchildren,
            npointers: _,
            pointers,
        } => {
            // Recursively serialize all children
            for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                let hash_id = pointer.hash_id().ok_or(MissingHashId)?;

                if pointer.is_commited() {
                    // We only want to serialize new inodes.
                    // We skip inodes that were previously serialized and already
                    // in the repository.
                    // Add their hash_id to `referenced_older_objects` so the gargage
                    // collector won't collect them.
                    referenced_older_objects.push(hash_id);
                    continue;
                }

                let inode_id = pointer.inode_id();
                let offset = serialize_inode(
                    inode_id,
                    output,
                    hash_id,
                    storage,
                    stats,
                    batch,
                    referenced_older_objects,
                    repository,
                    off,
                )?;

                pointer.set_offset(offset);
            }

            start = output.len();
            offset = start as u64 + off;

            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;
            // output.write_all(&[ID_INODE_POINTERS])?;

            // // Replaced by the length
            // output.write_all(&[0, 0, 0, 0])?;

            output.write_all(&depth.to_ne_bytes())?;
            output.write_all(&nchildren.to_ne_bytes())?;

            let bitfield = PointersHeader::from(pointers);
            output.write_all(&bitfield.to_bytes())?;

            let bitfield_offsets = PointersOffsetsHeader::from_pointers(offset, pointers);
            output.write_all(&bitfield_offsets.to_bytes())?;

            // println!(
            //     "SER HEADER_OFFSET={:?} POINTERS={:?}",
            //     bitfield_offsets, pointers
            // );

            // Make sure that INODE_POINTERS_NBYTES_TO_HASHES is correct.
            debug_assert_eq!(output[start + 2..].len(), INODE_POINTERS_NBYTES_TO_HASHES);

            for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                let hash_id = pointer.hash_id().ok_or(MissingHashId)?;
                let hash_id = hash_id.as_u32();

                serialize_hash_id(hash_id, output)?;

                let (relative_offset, offset_length) =
                    get_relative_offset(offset, pointer.offset());

                // println!(
                //     "SER OFFSET={:?} REL_OFFSET={:?} LENGTH={:?}",
                //     pointer.offset(),
                //     relative_offset,
                //     offset_length
                // );

                serialize_offset(output, relative_offset, offset_length);

                // let offset = pointer.offset();
                // output.write_all(&offset.to_ne_bytes())?;
            }

            write_object_header(output, start, ObjectTag::InodePointers);

            // let length = output.len() as u32 - start as u32;
            // output[start as usize + 1..start as usize + 5].copy_from_slice(&length.to_ne_bytes());

            batch.push((hash_id, Arc::from(&output[start..])));

            // for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
            //     let hash_id = pointer.hash_id().ok_or(MissingHashId)?;

            //     if pointer.is_commited() {
            //         // We only want to serialize new inodes.
            //         // We skip inodes that were previously serialized and already
            //         // in the repository.
            //         // Add their hash_id to `referenced_older_objects` so the gargage
            //         // collector won't collect them.
            //         referenced_older_objects.push(hash_id);
            //         continue;
            //     }

            //     let inode_id = pointer.inode_id();
            //     serialize_inode(
            //         inode_id,
            //         output,
            //         hash_id,
            //         storage,
            //         stats,
            //         batch,
            //         referenced_older_objects,
            //         repository,
            //     )?;
            // }
        }
        Inode::Directory(dir_id) => {
            // We don't check if it's a new inode because the parent
            // caller (recursively) confirmed it's a new one.

            let dir = storage.get_small_dir(*dir_id)?;
            serialize_directory(dir, offset, output, storage, repository, stats)?;

            batch.push((hash_id, Arc::from(&output[start..])));
        }
    };

    Ok(offset)
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

fn deserialize_hash_id(data: &[u8]) -> Result<(Option<HashId>, usize), DeserializationError> {
    use DeserializationError::*;

    let byte_hash_id = data.get(0).copied().ok_or(UnexpectedEOF)?;

    if byte_hash_id & 1 << 7 != 0 {
        // The HashId is in 3 bytes
        let hash_id = data.get(0..3).ok_or(UnexpectedEOF)?;

        let hash_id: u32 = (hash_id[0] as u32) << 16 | (hash_id[1] as u32) << 8 | hash_id[2] as u32;

        // Clear `COMPACT_HASH_ID_BIT`
        let hash_id = hash_id & (COMPACT_HASH_ID_BIT - 1);
        let hash_id = HashId::new(hash_id);

        Ok((hash_id, 3))
    } else {
        // The HashId is in 4 bytes
        let hash_id = data.get(0..4).ok_or(UnexpectedEOF)?;
        let hash_id = u32::from_be_bytes(hash_id.try_into()?);
        let hash_id = HashId::new(hash_id);

        Ok((hash_id, 4))
    }
}

fn deserialize_offset_length(
    data: &[u8],
    offset_length: OffsetLength,
    object_offset: u64,
) -> (u64, usize) {
    match offset_length {
        OffsetLength::RelativeOneByte => {
            let byte = data.get(0).unwrap();
            let relative_offset: u8 = u8::from_le_bytes([*byte]);
            let offset = object_offset - relative_offset as u64;

            // println!("LAAA REL={:?} OFFSET={:?}", relative_offset, offset);

            (offset, 1)
        }
        OffsetLength::RelativeTwoBytes => {
            let bytes = data.get(..2).unwrap();
            let relative_offset: u16 = u16::from_le_bytes(bytes.try_into().unwrap());
            let offset = object_offset - relative_offset as u64;
            (offset, 2)
        }
        OffsetLength::RelativeFourBytes => {
            let bytes = data.get(..4).unwrap();
            let relative_offset: u32 = u32::from_le_bytes(bytes.try_into().unwrap());
            let offset = object_offset - relative_offset as u64;
            (offset, 4)
        }
        OffsetLength::RelativeEightBytes => {
            let bytes = data.get(..8).unwrap();
            let relative_offset: u64 = u64::from_le_bytes(bytes.try_into().unwrap());
            let offset = object_offset - relative_offset;
            (offset, 8)
        }
    }
}

fn deserialize_shaped_directory(
    data: &[u8],
    object_offset: u64,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
) -> Result<DirectoryId, DeserializationError> {
    use DeserializationError as Error;
    use DeserializationError::*;

    let mut pos = 0;
    let data_length = data.len();

    let shape_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let shape_id = u32::from_ne_bytes(shape_id.try_into()?);
    let shape_id = DirectoryShapeId::from(shape_id);

    let directory_shape = match repository.get_shape(shape_id).map_err(Box::new)? {
        ShapeStrings::SliceIds(slice_ids) => Cow::Borrowed(slice_ids),
        ShapeStrings::Owned(strings) => {
            // We are in the readonly protocol runner.
            // Store the `String` in the `StringInterner`.
            let string_ids: Vec<StringId> =
                strings.iter().map(|s| storage.get_string_id(s)).collect();
            Cow::Owned(string_ids)
        }
    };

    let mut directory_shape = directory_shape.as_ref().iter();

    pos += 4;

    let dir_id = storage.with_new_dir::<_, Result<_, Error>>(|storage, new_dir| {
        while pos < data_length {
            let dir_entry_header = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
            let dir_entry_header = DirEntryShapeHeader::from_bytes([dir_entry_header[0]; 1]);

            pos += 1;

            let key_id = directory_shape.next().copied().ok_or(CannotFindNextShape)?;

            let kind = dir_entry_header.kind();
            let blob_inline_length = dir_entry_header.blob_inline_length() as usize;
            let offset_length: OffsetLength = dir_entry_header.offset_length();

            let dir_entry = if blob_inline_length > 0 {
                // The blob is inlined

                let blob = data
                    .get(pos..pos + blob_inline_length)
                    .ok_or(UnexpectedEOF)?;
                let blob_id = storage.add_blob_by_ref(blob)?;

                pos += blob_inline_length;

                DirEntry::new_commited(kind, None, Some(Object::Blob(blob_id)))
            } else {
                let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
                let (hash_id, nbytes) = deserialize_hash_id(bytes)?;

                pos += nbytes;

                let (offset, nbytes) =
                    deserialize_offset_length(&data[pos..], offset_length, object_offset);

                // println!("DESERIALIZING OFFSET offset={:?} nbytes={:?} length={:?} object={:?}", offset, nbytes, offset_length, object_offset);

                // let offset = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
                // let offset = u64::from_ne_bytes(offset.try_into()?);

                pos += nbytes;

                let dir_entry =
                    DirEntry::new_commited(kind, Some(hash_id.ok_or(MissingHash)?), None);
                // let dir_entry =
                //     DirEntry::new_commited(kind, None, None);
                dir_entry.set_offset(offset);
                dir_entry
            };

            let dir_entry_id = storage.add_dir_entry(dir_entry)?;

            new_dir.push((key_id, dir_entry_id));
        }

        Ok(storage.append_to_directories(new_dir))
    })??;

    Ok(dir_id)
}

fn deserialize_directory(
    data: &[u8],
    object_offset: u64,
    storage: &mut Storage,
) -> Result<DirectoryId, DeserializationError> {
    use DeserializationError as Error;
    use DeserializationError::*;

    let mut pos = 0;
    let data_length = data.len();

    let dir_id = storage.with_new_dir::<_, Result<_, Error>>(|storage, new_dir| {
        while pos < data_length {
            let dir_entry_header = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
            let dir_entry_header = DirEntryHeader::from_bytes([dir_entry_header[0]; 1]);

            pos += 1;

            let key_id = match dir_entry_header.key_inline_length() as usize {
                len if len > 0 => {
                    // The key is in the next `len` bytes
                    let key_bytes = data.get(pos..pos + len).ok_or(UnexpectedEOF)?;
                    let key_str = std::str::from_utf8(key_bytes)?;
                    pos += len;
                    storage.get_string_id(key_str)
                }
                _ => {
                    // The key length is in 2 bytes, followed by the key itself
                    let key_length = data.get(pos..pos + 2).ok_or(UnexpectedEOF)?;
                    let key_length = u16::from_ne_bytes(key_length.try_into()?);
                    let key_length = key_length as usize;

                    let key_bytes = data
                        .get(pos + 2..pos + 2 + key_length)
                        .ok_or(UnexpectedEOF)?;
                    let key_str = std::str::from_utf8(key_bytes)?;
                    pos += 2 + key_length;
                    storage.get_string_id(key_str)
                }
            };

            let kind = dir_entry_header.kind();
            let blob_inline_length = dir_entry_header.blob_inline_length() as usize;
            let offset_length = dir_entry_header.offset_length();

            let dir_entry = if blob_inline_length > 0 {
                // The blob is inlined

                let blob = data
                    .get(pos..pos + blob_inline_length)
                    .ok_or(UnexpectedEOF)?;
                let blob_id = storage.add_blob_by_ref(blob)?;

                pos += blob_inline_length;

                DirEntry::new_commited(kind, None, Some(Object::Blob(blob_id)))
            } else {
                let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
                let (hash_id, nbytes) = deserialize_hash_id(bytes)?;

                pos += nbytes;

                let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
                let (offset, nbytes) =
                    deserialize_offset_length(bytes, offset_length, object_offset);

                pos += nbytes;

                // let offset = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
                // let offset = u64::from_ne_bytes(offset.try_into()?);

                // pos += 8;

                let dir_entry =
                    DirEntry::new_commited(kind, Some(hash_id.ok_or(MissingHash)?), None);
                dir_entry.set_offset(offset);
                dir_entry
            };

            let dir_entry_id = storage.add_dir_entry(dir_entry)?;

            new_dir.push((key_id, dir_entry_id));
        }

        Ok(storage.append_to_directories(new_dir))
    })??;

    Ok(dir_id)
}

pub fn read_object_length(data: &[u8], header: &ObjectHeader) -> (usize, usize) {
    match header.length() {
        ObjectLength::OneByte => {
            let length = data[1] as usize;
            (1 + 1, length)
        }
        ObjectLength::TwoBytes => {
            let length = &data[1..3];
            let length = u16::from_le_bytes(length.try_into().unwrap()) as usize;
            (1 + 2, length)
        }
        ObjectLength::FourBytes => {
            let length = &data[1..5];
            let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
            (1 + 4, length)
        }
    }
}

/// Extract values from `data` to store them in `storage`.
/// Return an `Object`, which can be ids (refering to data inside `storage`) or a `Commit`
pub fn deserialize_object(
    // data: &[u8],
    object_offset: u64,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
) -> Result<Object, DeserializationError> {
    use DeserializationError::*;

    let data = std::mem::take(&mut storage.data);
    let data = &data;

    let header = data.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    // println!("DESERIALIZE_OBJECT HEADER={:?}", header);

    let (header_nbytes, _) = read_object_length(data, &header);

    let mut pos = header_nbytes;

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::Directory => {
            let dir_id = deserialize_directory(&data[pos..], object_offset, storage)?;
            Ok(Object::Directory(dir_id))
        }
        ObjectTag::ShapedDirectory => {
            let dir_id =
                deserialize_shaped_directory(&data[pos..], object_offset, storage, repository)?;
            Ok(Object::Directory(dir_id))
        }
        ObjectTag::Blob => {
            let blob = data.get(pos..).ok_or(UnexpectedEOF)?;
            let blob_id = storage.add_blob_by_ref(blob)?;
            Ok(Object::Blob(blob_id))
        }
        ObjectTag::Commit => {
            let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
            let (parent_commit_hash, nbytes) = deserialize_hash_id(bytes)?;

            pos += nbytes;

            let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
            let (root_hash, nbytes) = deserialize_hash_id(bytes)?;

            pos += nbytes;

            let root_hash_offset = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
            let root_hash_offset = u64::from_ne_bytes(root_hash_offset.try_into()?);

            pos += 8;

            let time = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
            let time = u64::from_ne_bytes(time.try_into()?);

            let author_length = data.get(pos + 8..pos + 12).ok_or(UnexpectedEOF)?;
            let author_length = u32::from_ne_bytes(author_length.try_into()?) as usize;

            let author = data
                .get(pos + 12..pos + 12 + author_length)
                .ok_or(UnexpectedEOF)?;
            let author = author.to_vec();

            pos = pos + 12 + author_length;

            let message = data.get(pos..).ok_or(UnexpectedEOF)?;
            let message = message.to_vec();

            Ok(Object::Commit(Box::new(Commit {
                parent_commit_hash,
                root_hash_ref: ObjectReference::new(
                    Some(root_hash.ok_or(MissingRootHash)?),
                    root_hash_offset,
                ),
                // root_hash: root_hash.ok_or(MissingRootHash)?,
                // root_hash_offset,
                time,
                author: String::from_utf8(author)?,
                message: String::from_utf8(message)?,
            })))
        }
        ObjectTag::InodePointers => {
            let inode =
                deserialize_inode_pointers(&data[pos..], object_offset, storage, repository)?;
            let inode_id = storage.add_inode(inode)?;

            Ok(Object::Directory(inode_id.into()))
        } // _ => Err(UnknownID),
    }
}

fn deserialize_inode_pointers(
    data: &[u8],
    object_offset: u64,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
) -> Result<Inode, DeserializationError> {
    use DeserializationError::*;

    let mut pos = 0;

    let depth = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let depth = u32::from_ne_bytes(depth.try_into()?);

    let nchildren = data.get(pos + 4..pos + 8).ok_or(UnexpectedEOF)?;
    let nchildren = u32::from_ne_bytes(nchildren.try_into()?);

    pos += 8;

    let pointers_header = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let pointers_header = PointersHeader::from_bytes(pointers_header.try_into()?);

    let npointers = pointers_header.count();
    let indexes_iter = pointers_header.iter();

    pos += 4;

    let offsets_header = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
    let offsets_header = PointersOffsetsHeader::from_bytes(offsets_header.try_into()?);

    // println!(
    //     "DES START OBJECT_OFFSET={:?} HEADER_OFFSET={:?}",
    //     object_offset, header_offsets
    // );

    pos += 8;

    let mut pointers: [Option<PointerToInode>; 32] = Default::default();

    for (index, pointer_index) in indexes_iter.enumerate() {
        let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
        let (hash_id, nbytes) = deserialize_hash_id(bytes)?;

        pos += nbytes;

        let offset_length = offsets_header.get(index);
        let (offset, nbytes) =
            deserialize_offset_length(&data[pos..], offset_length, object_offset);

        // println!(
        //     "DES OFFSET={:?} NBYTES={:?} LENGTH={:?} INDEX={:?}",
        //     offset, nbytes, offset_length, index
        // );

        pos += nbytes;

        // let offset = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
        // let offset = u64::from_ne_bytes(offset.try_into()?);

        // pos += 8;

        // TODO: Use offset

        let mut output = Vec::with_capacity(1000);

        let object_ref = ObjectReference::new(None, offset);
        repository
            .get_value_from_offset(&mut output, object_ref)
            .unwrap();
        let inode_id = deserialize_inode(&output, offset, storage, repository).unwrap();

        // .map_err(|_| InodeNotFoundInRepository)
        // .and_then(|data| {
        //     let data = data.ok_or(InodeEmptyInRepository)?;
        //     deserialize_inode(data.as_ref(), storage, repository)
        // })?;

        // let inode_id = repository
        //     .get_value(hash_id.ok_or(MissingHash)?)
        //     .map_err(|_| InodeNotFoundInRepository)
        //     .and_then(|data| {
        //         let data = data.ok_or(InodeEmptyInRepository)?;
        //         deserialize_inode(data.as_ref(), storage, repository)
        //     })?;

        pointers[pointer_index as usize] = Some(PointerToInode::new_commited(
            Some(hash_id.ok_or(MissingHash)?),
            inode_id,
            offset,
        ));
    }

    Ok(Inode::Pointers {
        depth,
        nchildren,
        npointers,
        pointers,
    })
}

pub fn deserialize_inode(
    data: &[u8],
    object_offset: u64,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
) -> Result<InodeId, DeserializationError> {
    use DeserializationError::*;

    let header = data.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    let (header_nbytes, _) = read_object_length(data, &header);

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::InodePointers => {
            let inode = deserialize_inode_pointers(
                &data[header_nbytes..],
                object_offset,
                storage,
                repository,
            )?;
            storage.add_inode(inode).map_err(Into::into)
        }
        ObjectTag::Directory => {
            let dir_id = deserialize_directory(&data[header_nbytes..], object_offset, storage)?;
            storage
                .add_inode(Inode::Directory(dir_id))
                .map_err(Into::into)
        }
        ObjectTag::ShapedDirectory => {
            let dir_id = deserialize_shaped_directory(
                &data[header_nbytes..],
                object_offset,
                storage,
                repository,
            )?;
            storage
                .add_inode(Inode::Directory(dir_id))
                .map_err(Into::into)
        }
        _ => Err(UnknownID),
    }
}

/// Iterate HashIds in the serialized data
pub fn iter_hash_ids(data: &[u8]) -> HashIdIterator {
    HashIdIterator { data, pos: 0 }
}

pub struct HashIdIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

/// Number of bytes to reach the hashes when serializing a `Inode::Pointers`.
///
/// This skip `ID_INODE_POINTERS`, `depth`, `nchildren` and `PointersDescriptor`.
const INODE_POINTERS_NBYTES_TO_HASHES: usize = 20;

/// Number of bytes to reach the hashes when serializing a shaped directory.
///
/// This skip `ID_SHAPED_DIRECTORY` and the `ShapeId`
const SHAPED_DIRECTORY_NBYTES_TO_HASHES: usize = 4;

impl<'a> Iterator for HashIdIterator<'a> {
    type Item = HashId;

    fn next(&mut self) -> Option<Self::Item> {
        // let id = self.data.get(0).copied()?;

        let header = self.data.get(0).copied()?;
        let header: ObjectHeader = ObjectHeader::from_bytes([header]);
        let tag: ObjectTag = header.tag_or_err().ok()?;

        loop {
            let mut pos = self.pos;

            if pos == 0 {
                let (header_nbytes, length) = read_object_length(self.data, &header);

                match tag {
                    ObjectTag::Blob => {
                        // No HashId in Object::Blob
                        return None;
                    }
                    ObjectTag::Commit => {
                        // Deserialize the parent hash to know it's size
                        let (_, nbytes) =
                            deserialize_hash_id(self.data.get(header_nbytes..)?).ok()?;

                        // Object::Commit.root_hash
                        let (root_hash, _) =
                            deserialize_hash_id(self.data.get(header_nbytes + nbytes..)?).ok()?;
                        self.pos = self.data.len();

                        return root_hash;
                    }
                    ObjectTag::InodePointers => {
                        // We skip the first bytes (ID_INODE_POINTERS, depth, nchildren, ..) to reach
                        // the hashes
                        pos += header_nbytes + INODE_POINTERS_NBYTES_TO_HASHES;
                    }
                    ObjectTag::ShapedDirectory => {
                        pos += header_nbytes + SHAPED_DIRECTORY_NBYTES_TO_HASHES;
                    }
                    ObjectTag::Directory => {
                        // Skip the tag (ID_DIRECTORY)
                        // pos += 5;
                        pos += header_nbytes;
                    }
                }

                // if id == ID_BLOB {
                //     // No HashId in Object::Blob
                //     return None;
                // } else if id == ID_COMMIT {
                //     // Deserialize the parent hash to know it's size
                //     let (_, nbytes) = deserialize_hash_id(self.data.get(5..)?).ok()?;

                //     // Object::Commit.root_hash
                //     let (root_hash, _) = deserialize_hash_id(self.data.get(5 + nbytes..)?).ok()?;
                //     self.pos = self.data.len();

                //     return root_hash;
                // } else if id == ID_INODE_POINTERS {
                //     // We skip the first bytes (ID_INODE_POINTERS, depth, nchildren, ..) to reach
                //     // the hashes
                //     pos += INODE_POINTERS_NBYTES_TO_HASHES;
                // } else if id == ID_SHAPED_DIRECTORY {
                //     pos += SHAPED_DIRECTORY_NBYTES_TO_HASHES;
                // } else {
                //     debug_assert_eq!(ID_DIRECTORY, id);

                //     // Skip the tag (ID_DIRECTORY)
                //     pos += 5;
                // }
            }

            match tag {
                ObjectTag::InodePointers => {
                    let bytes = self.data.get(pos..)?;
                    let (hash_id, nbytes) = deserialize_hash_id(bytes).ok()?;

                    self.pos = pos + nbytes + 8;

                    return hash_id;
                }
                ObjectTag::Directory | ObjectTag::ShapedDirectory => {
                    let descriptor = self.data.get(pos..pos + 1)?;
                    let descriptor = DirEntryShapeHeader::from_bytes([descriptor[0]; 1]);

                    pos += 1;

                    if tag != ObjectTag::ShapedDirectory {
                        // if id != ID_SHAPED_DIRECTORY {
                        // ID_SHAPED_DIRECTORY do not contain the keys

                        // let offset = match descriptor.key_inline_length() as usize {
                        //     len if len > 0 => len,
                        //     _ => {
                        //         let key_length = self.data.get(pos..pos + 2)?;
                        //         let key_length = u16::from_ne_bytes(key_length.try_into().ok()?);
                        //         2 + key_length as usize
                        //     }
                        // };

                        // pos += offset;
                    }

                    let blob_inline_length = descriptor.blob_inline_length() as usize;

                    if blob_inline_length > 0 {
                        // No HashId when the blob is inlined, go to next dir entry
                        self.pos = pos + blob_inline_length;
                        continue;
                    }

                    let bytes = self.data.get(pos..)?;
                    let (hash_id, nbytes) = deserialize_hash_id(bytes).ok()?;

                    self.pos = pos + nbytes;

                    // let (_, nbytes) = deserialize_offset_length(&self.data[self.pos..], offset_length, object_offset);

                    self.pos += 8;

                    // let offset = self.data.get(pos..pos + 8).ok()?;
                    // let offset = u64::from_ne_bytes(offset.try_into()?);

                    return hash_id;
                }
                _ => return None,
            }

            // if id == ID_INODE_POINTERS {
            //     let bytes = self.data.get(pos..)?;
            //     let (hash_id, nbytes) = deserialize_hash_id(bytes).ok()?;

            //     self.pos = pos + nbytes + 8;

            //     return hash_id;
            // } else {
            //     // ID_DIRECTORY or ID_SHAPED_DIRECTORY

            //     let descriptor = self.data.get(pos..pos + 1)?;
            //     let descriptor = KeyDirEntryDescriptor::from_bytes([descriptor[0]; 1]);

            //     pos += 1;

            //     if id != ID_SHAPED_DIRECTORY {
            //         // ID_SHAPED_DIRECTORY do not contain the keys

            //         let offset = match descriptor.key_inline_length() as usize {
            //             len if len > 0 => len,
            //             _ => {
            //                 let key_length = self.data.get(pos..pos + 2)?;
            //                 let key_length = u16::from_ne_bytes(key_length.try_into().ok()?);
            //                 2 + key_length as usize
            //             }
            //         };

            //         pos += offset;
            //     }

            //     let blob_inline_length = descriptor.blob_inline_length() as usize;

            //     if blob_inline_length > 0 {
            //         // No HashId when the blob is inlined, go to next dir entry
            //         self.pos = pos + blob_inline_length;
            //         continue;
            //     }

            //     let bytes = self.data.get(pos..)?;
            //     let (hash_id, nbytes) = deserialize_hash_id(bytes).ok()?;

            //     self.pos = pos + nbytes;

            //     self.pos += 8;

            //     // let offset = self.data.get(pos..pos + 8).ok()?;
            //     // let offset = u64::from_ne_bytes(offset.try_into()?);

            //     return hash_id;
            // }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use tezos_timing::SerializeStats;

    use crate::persistent::KeyValueStoreBackend;
    use crate::{
        hash::hash_object,
        kv_store::{in_memory::InMemory, persistent::Persistent},
        working_tree::storage::DirectoryId,
    };

    use super::*;

    #[test]
    fn test_serialize() {
        let mut storage = Storage::new();
        let mut repo = Persistent::try_new().unwrap();
        //let mut repo = InMemory::try_new().unwrap();
        let mut stats = SerializeStats::default();
        let mut batch = Vec::new();
        let mut older_objects = Vec::new();
        let fake_hash_id = HashId::try_from(1).unwrap();

        // Test Object::Directory

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(1), None),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None),
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        let offset = serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            0,
        )
        .unwrap();

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(offset, &mut storage, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id).unwrap(),
                storage.get_owned_dir(object).unwrap()
            )
        } else {
            panic!();
        }

        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[3, 1, 2]);

        // Test Object::Directory (Shaped)

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(1), None),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None),
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            0,
        )
        .unwrap();

        let offset = data.len();

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(0, &mut storage, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id).unwrap(),
                storage.get_owned_dir(object).unwrap()
            )
        } else {
            panic!();
        }

        // Test Object::Directory (Shaped)

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a1",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(4), None).with_offset(1),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab1",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(5), None).with_offset(2),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aa1",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(6), None).with_offset(3),
            )
            .unwrap();

        println!("LAAAAA={:?}", storage.get_owned_dir(dir_id).unwrap(),);

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset as u64,
        )
        .unwrap();

        println!("OFFSET={:?}", offset);

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(offset as u64, &mut storage, &repo).unwrap();

        if let Object::Directory(object) = object {
            println!("RESULT={:?}", storage.get_owned_dir(object).unwrap());

            assert_eq!(
                storage.get_owned_dir(dir_id).unwrap(),
                storage.get_owned_dir(object).unwrap()
            )
        } else {
            panic!();
        }

        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[3, 1, 2]);

        // Test Object::Blob

        // Not inlined value
        let blob_id = storage.add_blob_by_ref(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Blob(blob_id),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            0,
        )
        .unwrap();

        let offset = data.len();

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(offset as u64, &mut storage, &repo).unwrap();
        if let Object::Blob(object) = object {
            let blob = storage.get_blob(object).unwrap();
            assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        } else {
            panic!();
        }
        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.count(), 0);

        // Test Object::Commit

        let mut data = Vec::with_capacity(1024);

        let commit = Commit {
            parent_commit_hash: HashId::new(9876),
            root_hash_ref: ObjectReference::new(HashId::new(12345), 0),
            // root_hash: HashId::new(12345).unwrap(),
            // root_hash_offset: 0,
            time: 12345,
            author: "123".to_string(),
            message: "abc".to_string(),
        };

        serialize_object(
            &Object::Commit(Box::new(commit.clone())),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            0,
        )
        .unwrap();

        let offset = data.len();

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(offset as u64, &mut storage, &repo).unwrap();
        if let Object::Commit(object) = object {
            assert_eq!(*object, commit);
        } else {
            panic!();
        }

        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[12345]);

        println!("OKKKK");

        // data.clear();
        // let mut data = Vec::with_capacity(1024);

        // data.extend_from_slice(&[1,2,3,4,5]);

        repo.append_serialized_data(&data).unwrap();
        let offset = repo.get_current_offset().unwrap().unwrap();

        data.clear();

        let mut offsets = Vec::with_capacity(32);

        // Test Inode::Directory

        let mut pointers: [Option<PointerToInode>; 32] = Default::default();

        println!("START POINTERS AT OFFSET {:?}", offset);

        for index in 0..pointers.len() {
            let inode_value = Inode::Directory(DirectoryId::empty());
            let inode_value_id = storage.add_inode(inode_value).unwrap();

            let hash_id = HashId::new((index + 1) as u32).unwrap();

            let off = data.len();
            let offset = serialize_inode(
                inode_value_id,
                &mut data,
                hash_id,
                &storage,
                &mut stats,
                &mut batch,
                &mut older_objects,
                &mut repo,
                offset,
            )
            .unwrap();

            offsets.push(offset);

            repo.write_batch(vec![(hash_id, Arc::new([ID_DIRECTORY]))])
                .unwrap();

            println!(
                "OFFSET={:?} DATA_LEN={:?} OFF={:?}",
                offset,
                data.len(),
                off
            );

            pointers[index] = Some(PointerToInode::new_commited(
                Some(hash_id),
                inode_value_id,
                offset,
            ));
        }

        println!("END POINTERS",);

        let inode = Inode::Pointers {
            depth: 100,
            nchildren: 200,
            npointers: 250,
            pointers,
        };

        println!("POINTERS={:?}", inode);

        // let offset = data.len();

        let inode_id = storage.add_inode(inode).unwrap();

        let hash_id = HashId::new(123).unwrap();
        batch.clear();
        let offset = serialize_inode(
            inode_id,
            &mut data,
            hash_id,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            //            5,
            offset as u64,
        )
        .unwrap();

        // let offset = data.len();

        println!("DATA LENGTH={:?}", data.len());
        repo.append_serialized_data(&data).unwrap();

        println!("ID={:?}", batch.last().unwrap().1[0]);
        let new_inode_id =
            deserialize_inode(&batch.last().unwrap().1, offset as u64, &mut storage, &repo)
                .unwrap();
        //let new_inode_id = deserialize_inode(&batch[0].1, &mut storage, &repo).unwrap();
        let new_inode = storage.get_inode(new_inode_id).unwrap();

        if let Inode::Pointers {
            depth,
            nchildren,
            npointers,
            pointers,
        } = new_inode
        {
            assert_eq!(*depth, 100);
            assert_eq!(*nchildren, 200);
            assert_eq!(*npointers, 32);

            for (index, pointer) in pointers.iter().enumerate() {
                let pointer = pointer.as_ref().unwrap();
                let hash_id = pointer.hash_id().unwrap();
                assert_eq!(hash_id.as_u32() as usize, index + 1);

                let inode = storage.get_inode(pointer.inode_id()).unwrap();
                match inode {
                    Inode::Directory(dir_id) => assert!(dir_id.is_empty()),
                    _ => panic!(),
                }

                assert_eq!(pointer.offset(), offsets[index]);
            }
        } else {
            println!("LAAA {:?}", new_inode);
            panic!()
        }

        // let iter = iter_hash_ids(&batch.last().unwrap().1);
        // assert_eq!(
        //     iter.map(|h| h.as_u32()).collect::<Vec<_>>(),
        //     (1..33).collect::<Vec<_>>()
        // );

        // Test Inode::Value

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(1), None).with_offset(1),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None).with_offset(2),
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None).with_offset(3),
            )
            .unwrap();

        let inode = Inode::Directory(dir_id);
        let inode_id = storage.add_inode(inode).unwrap();

        // let offset = data.len();

        batch.clear();
        let offset = serialize_inode(
            inode_id,
            &mut data,
            hash_id,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset as u64,
        )
        .unwrap();

        // let offset = data.len();
        repo.append_serialized_data(&data).unwrap();

        assert_eq!(batch.len(), 1);

        let new_inode_id =
            deserialize_inode(&batch.last().unwrap().1, offset as u64, &mut storage, &repo)
                .unwrap();
        let new_inode = storage.get_inode(new_inode_id).unwrap();

        if let Inode::Directory(new_dir_id) = new_inode {
            assert_eq!(
                storage.get_owned_dir(dir_id).unwrap(),
                storage.get_owned_dir(*new_dir_id).unwrap()
            )
        }

        // let iter = iter_hash_ids(&batch[0].1);
        // assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[3, 1, 2]);
    }

    #[test]
    fn test_serialize_empty_blob() {
        let mut repo = Persistent::try_new().expect("failed to create context");
        //let mut repo = InMemory::try_new().expect("failed to create context");
        let mut storage = Storage::new();
        let mut stats = SerializeStats::default();
        let mut batch = Vec::new();
        let mut older_objects = Vec::new();

        let fake_hash_id = HashId::try_from(1).unwrap();

        let blob_id = storage.add_blob_by_ref(&[]).unwrap();
        let blob = Object::Blob(blob_id);
        let blob_hash_id = hash_object(&blob, &mut repo, &storage).unwrap();

        assert!(blob_hash_id.is_some());

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, blob_hash_id, None),
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);

        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            0,
        )
        .unwrap();

        storage.data = data.clone(); // TODO: Do not do this
        let object = deserialize_object(0, &mut storage, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id).unwrap(),
                storage.get_owned_dir(object).unwrap()
            )
        } else {
            panic!();
        }
    }

    #[test]
    fn test_inode_headers() {
        let mut header = PointersOffsetsHeader::default();

        for index in 0..32 {
            header.set(index, OffsetLength::RelativeOneByte);
            assert_eq!(header.get(index), OffsetLength::RelativeOneByte);

            header.clear(index);
            header.set(index, OffsetLength::RelativeTwoBytes);
            assert_eq!(header.get(index), OffsetLength::RelativeTwoBytes);

            header.clear(index);
            header.set(index, OffsetLength::RelativeFourBytes);
            assert_eq!(header.get(index), OffsetLength::RelativeFourBytes);

            header.clear(index);
            header.set(index, OffsetLength::RelativeEightBytes);
            assert_eq!(header.get(index), OffsetLength::RelativeEightBytes);
        }
    }
}
