// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Serialization/deserialization for objects in the Working Tree so that they can be
//! saved/loaded to/from the repository.

use std::{borrow::Cow, convert::TryInto, io::Write, sync::Arc};

use modular_bitfield::prelude::*;
use serde::{Deserialize, Serialize};
use static_assertions::assert_eq_size;
use tezos_timing::SerializeStats;

use crate::{
    kv_store::HashId,
    serialize::{get_inline_blob, ObjectTag, COMPACT_HASH_ID_BIT},
    working_tree::{
        shape::ShapeStrings,
        storage::{DirectoryId, Inode, PointerToInode},
        string_interner::StringInterner,
        Commit, DirEntryKind, ObjectReference,
    },
    ContextKeyValueStore,
};

use crate::working_tree::{
    shape::DirectoryShapeId,
    storage::{DirEntryId, InodeId, Storage},
    string_interner::StringId,
    DirEntry, Object,
};

use super::{
    DeserializationError, ObjectHeader, ObjectLength, SerializationError, FULL_23_BITS,
    FULL_31_BITS,
};

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
#[bits = 2]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
enum OffsetLength {
    RelativeOneByte,
    RelativeTwoBytes,
    RelativeFourBytes,
    RelativeEightBytes,
}

#[bitfield(bits = 8)]
struct CommitHeader {
    parent_offset_length: OffsetLength,
    root_offset_length: OffsetLength,
    author_length: ObjectLength,
    is_parent_exist: bool,
    #[skip]
    _unused: B1,
}

// Must fit in 1 byte
assert_eq_size!(CommitHeader, u8);

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AbsoluteOffset(u64);
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct RelativeOffset(u64);

impl From<u64> for AbsoluteOffset {
    fn from(offset: u64) -> Self {
        Self(offset)
    }
}

impl From<u64> for RelativeOffset {
    fn from(offset: u64) -> Self {
        Self(offset)
    }
}

impl AbsoluteOffset {
    pub fn as_u64(self) -> u64 {
        self.0
    }
    pub fn add(self, add: u64) -> Self {
        Self(self.0 + add)
    }
}

impl RelativeOffset {
    fn as_u64(self) -> u64 {
        self.0
    }
}

fn get_relative_offset(
    current_offset: AbsoluteOffset,
    target_offset: AbsoluteOffset,
) -> (RelativeOffset, OffsetLength) {
    let current_offset = current_offset.as_u64();
    let target_offset = target_offset.as_u64();

    assert!(current_offset >= target_offset);

    let relative_offset = current_offset - target_offset;

    if relative_offset <= 0xFF {
        (
            RelativeOffset(relative_offset),
            OffsetLength::RelativeOneByte,
        )
    } else if relative_offset <= 0xFFFF {
        (
            RelativeOffset(relative_offset),
            OffsetLength::RelativeTwoBytes,
        )
    } else if relative_offset <= 0xFFFFFFFF {
        (
            RelativeOffset(relative_offset),
            OffsetLength::RelativeFourBytes,
        )
    } else {
        (
            RelativeOffset(relative_offset),
            OffsetLength::RelativeEightBytes,
        )
    }
}

fn serialize_offset(
    output: &mut Vec<u8>,
    relative_offset: RelativeOffset,
    offset_length: OffsetLength,
) -> Result<(), SerializationError> {
    let relative_offset = relative_offset.as_u64();

    match offset_length {
        OffsetLength::RelativeOneByte => {
            let offset: u8 = relative_offset as u8;
            output.write_all(&offset.to_le_bytes())?;
        }
        OffsetLength::RelativeTwoBytes => {
            let offset: u16 = relative_offset as u16;
            output.write_all(&offset.to_le_bytes())?;
        }
        OffsetLength::RelativeFourBytes => {
            let offset: u32 = relative_offset as u32;
            output.write_all(&offset.to_le_bytes())?;
        }
        OffsetLength::RelativeEightBytes => {
            output.write_all(&relative_offset.to_le_bytes())?;
        }
    }

    Ok(())
}

fn serialize_shaped_directory(
    shape_id: DirectoryShapeId,
    dir: &[(StringId, DirEntryId)],
    object_hash_id: HashId,
    offset: AbsoluteOffset,
    output: &mut Vec<u8>,
    storage: &Storage,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    use SerializationError::*;

    let start = output.len();

    // Replaced by ObjectHeader
    output.write_all(&[0, 0])?;

    let _nbytes = serialize_hash_id(object_hash_id.as_u32(), output)?;

    let shape_id = shape_id.as_u32();
    output.write_all(&shape_id.to_le_bytes())?;

    for (_, dir_entry_id) in dir {
        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let kind = dir_entry.dir_entry_kind();

        let blob_inline = get_inline_blob(storage, dir_entry);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        if let Some(blob_inline) = blob_inline {
            let byte: [u8; 1] = DirEntryShapeHeader::new()
                .with_kind(kind)
                .with_offset_length(OffsetLength::RelativeOneByte) // Ignored on deserialization
                .with_blob_inline_length(blob_inline_length as u8)
                .into_bytes();

            output.write_all(&byte[..])?;
            output.write_all(&blob_inline)?;
        } else {
            let dir_entry_offset = dir_entry.get_offset().ok_or(MissingOffset)?;
            let (relative_offset, offset_length) = get_relative_offset(offset, dir_entry_offset);

            let byte: [u8; 1] = DirEntryShapeHeader::new()
                .with_kind(kind)
                .with_offset_length(offset_length)
                .with_blob_inline_length(blob_inline_length as u8)
                .into_bytes();

            output.write_all(&byte[..])?;

            serialize_offset(output, relative_offset, offset_length)?;
        }
    }

    write_object_header(output, start, ObjectTag::ShapedDirectory);

    stats.nshapes = stats.nshapes.saturating_add(1);

    Ok(())
}

fn write_object_header(output: &mut Vec<u8>, start: usize, tag: ObjectTag) {
    let length = output.len() - start;

    if length <= 0xFF {
        let header: [u8; 1] = ObjectHeader::new()
            .with_tag(tag)
            .with_length(ObjectLength::OneByte)
            .with_is_persistent(true)
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
            .with_is_persistent(true)
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
            .with_is_persistent(true)
            .into_bytes();

        let length: u32 = length as u32 + 3;

        output[start] = header[0];
        output[start + 1..start + 5].copy_from_slice(&length.to_le_bytes());
    }
}

fn serialize_directory_or_shape(
    dir: &[(StringId, DirEntryId)],
    object_hash_id: HashId,
    offset: AbsoluteOffset,
    output: &mut Vec<u8>,
    storage: &Storage,
    repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
    strings: &StringInterner,
) -> Result<(), SerializationError> {
    if let Some(shape_id) = repository.make_shape(dir, storage)? {
        serialize_shaped_directory(
            shape_id,
            dir,
            object_hash_id,
            offset,
            output,
            storage,
            stats,
        )
    } else {
        serialize_directory(
            dir,
            object_hash_id,
            offset,
            output,
            storage,
            repository,
            stats,
            strings,
        )
    }
}

fn serialize_directory(
    dir: &[(StringId, DirEntryId)],
    object_hash_id: HashId,
    offset: AbsoluteOffset,
    output: &mut Vec<u8>,
    storage: &Storage,
    _repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
    strings: &StringInterner,
) -> Result<(), SerializationError> {
    use SerializationError::*;

    let mut keys_length: usize = 0;
    let hash_ids_length: usize = 0;
    let highest_hash_id: u32 = 0;
    let mut nblobs_inlined: usize = 0;
    let mut blobs_length: usize = 0;

    let start = output.len();

    // Replaced by ObjectHeader
    output.write_all(&[0, 0])?;

    serialize_hash_id(object_hash_id.as_u32(), output)?;

    for (key_id, dir_entry_id) in dir {
        let key = strings.get(*key_id).unwrap();
        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let kind = dir_entry.dir_entry_kind();

        let blob_inline = get_inline_blob(storage, dir_entry);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        let (relative_offset, offset_length) = match dir_entry
            .get_offset()
            .map(|dir_entry_offset| get_relative_offset(offset, dir_entry_offset))
        {
            Some((relative_offset, offset_length)) => (Some(relative_offset), Some(offset_length)),
            None => (None, None),
        };

        match key.len() {
            len if len != 0 && len < 4 => {
                let byte: [u8; 1] = DirEntryHeader::new()
                    .with_kind(kind)
                    .with_key_inline_length(len as u8)
                    .with_blob_inline_length(blob_inline_length as u8)
                    .with_offset_length(offset_length.unwrap_or(OffsetLength::RelativeOneByte))
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
                    .with_offset_length(offset_length.unwrap_or(OffsetLength::RelativeOneByte))
                    .into_bytes();
                output.write_all(&byte[..])?;

                let key_length: u16 = len.try_into()?;
                output.write_all(&key_length.to_le_bytes())?;
                output.write_all(key.as_bytes())?;
                keys_length += 2 + key.len();
            }
        }

        if let Some(blob_inline) = blob_inline {
            nblobs_inlined += 1;
            blobs_length += blob_inline.len();

            output.write_all(&blob_inline)?;
        } else {
            let relative_offset = relative_offset.ok_or(MissingOffset)?;
            let offset_length = offset_length.ok_or(MissingOffset)?;

            serialize_offset(output, relative_offset, offset_length)?;
        }
    }

    write_object_header(output, start, ObjectTag::Directory);

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
    strings: &StringInterner,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>, // TODO: Unused this
    _referenced_older_objects: &mut Vec<HashId>,
    repository: &mut ContextKeyValueStore,
    offset: Option<AbsoluteOffset>,
) -> Result<Option<AbsoluteOffset>, SerializationError> {
    let start = output.len();

    let offset = offset.ok_or(SerializationError::MissingOffset)?;
    let mut offset: AbsoluteOffset = offset.add(start as u64);

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
                    repository,
                    offset,
                    strings,
                )?;
            } else {
                let dir = storage.get_small_dir(*dir_id)?;

                serialize_directory_or_shape(
                    dir,
                    object_hash_id,
                    offset,
                    output,
                    storage,
                    repository,
                    stats,
                    strings,
                )?;
            }
        }
        Object::Blob(blob_id) => {
            debug_assert!(!blob_id.is_inline());

            let blob = storage.get_blob(*blob_id)?;

            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;

            serialize_hash_id(object_hash_id.as_u32(), output)?;

            output.write_all(blob.as_ref())?;

            write_object_header(output, start, ObjectTag::Blob);

            stats.add_blob(blob.len());
        }
        Object::Commit(commit) => {
            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;

            serialize_hash_id(object_hash_id.as_u32(), output)?;

            let author_length = match commit.author.len() {
                length if length <= 0xFF => ObjectLength::OneByte,
                length if length <= 0xFFFF => ObjectLength::TwoBytes,
                _ => ObjectLength::FourBytes,
            };

            let (root_relative_offset, root_offset_length) =
                get_relative_offset(offset, commit.root_ref.offset());

            let (is_parent_exist, (parent_relative_offset, parent_offset_length)) =
                match commit.parent_commit_ref {
                    Some(parent) => (true, get_relative_offset(offset, parent.offset())),
                    None => (false, (0.into(), OffsetLength::RelativeOneByte)),
                };

            let header: [u8; 1] = CommitHeader::new()
                .with_is_parent_exist(is_parent_exist)
                .with_parent_offset_length(parent_offset_length)
                .with_root_offset_length(root_offset_length)
                .with_author_length(author_length)
                .into_bytes();

            output.write_all(&header)?;

            if let Some(parent) = commit.parent_commit_ref {
                serialize_hash_id(parent.hash_id().as_u32(), output)?;
                serialize_offset(output, parent_relative_offset, parent_offset_length)?;
            };

            let root_hash_id = commit.root_ref.hash_id().as_u32();
            serialize_hash_id(root_hash_id, output)?;

            serialize_offset(output, root_relative_offset, root_offset_length)?;

            output.write_all(&commit.time.to_le_bytes())?;

            match author_length {
                ObjectLength::OneByte => {
                    let author_length: u8 = commit.author.len() as u8;
                    output.write_all(&author_length.to_le_bytes())?;
                }
                ObjectLength::TwoBytes => {
                    let author_length: u16 = commit.author.len() as u16;
                    output.write_all(&author_length.to_le_bytes())?;
                }
                ObjectLength::FourBytes => {
                    let author_length: u32 = commit.author.len() as u32;
                    output.write_all(&author_length.to_le_bytes())?;
                }
            }

            output.write_all(commit.author.as_bytes())?;

            // The message length is inferred.
            // It's until the end of the slice
            output.write_all(commit.message.as_bytes())?;

            write_object_header(output, start, ObjectTag::Commit);

            batch.push((object_hash_id, Arc::from(&output[start..])));
        }
    };

    stats.total_bytes += output.len();

    Ok(Some(offset))
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

    fn to_bytes(self) -> [u8; 4] {
        self.bitfield.to_le_bytes()
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
            bitfield: u32::from_le_bytes(bytes),
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

    fn from_pointers(
        object_offset: AbsoluteOffset,
        pointers: &[Option<PointerToInode>; 32],
    ) -> Self {
        let mut bitfield = Self::default();

        for (index, pointer) in pointers.iter().filter_map(|p| p.as_ref()).enumerate() {
            let p_offset = pointer.offset();

            let (_, offset_length) = get_relative_offset(object_offset, p_offset);

            bitfield.set(index, offset_length);
        }

        bitfield
    }

    fn to_bytes(self) -> [u8; 8] {
        self.bitfield.to_le_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        Self {
            bitfield: u64::from_le_bytes(bytes),
        }
    }
}

fn serialize_inode(
    inode_id: InodeId,
    output: &mut Vec<u8>,
    object_hash_id: HashId,
    storage: &Storage,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>,
    repository: &mut ContextKeyValueStore,
    off: AbsoluteOffset,
    strings: &StringInterner,
) -> Result<AbsoluteOffset, SerializationError> {
    use SerializationError::*;

    let mut start = output.len();
    let mut offset = off.add(start as u64);

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
                    continue;
                }

                let inode_id = pointer.inode_id();
                let offset = serialize_inode(
                    inode_id, output, hash_id, storage, stats, batch, repository, off, strings,
                )?;

                pointer.set_offset(offset);
            }

            start = output.len();
            offset = off.add(start as u64);

            // Replaced by ObjectHeader
            output.write_all(&[0, 0])?;

            let _nbytes = serialize_hash_id(object_hash_id.as_u32(), output)?;

            output.write_all(&depth.to_le_bytes())?;
            output.write_all(&nchildren.to_le_bytes())?;

            let bitfield = PointersHeader::from(pointers);
            output.write_all(&bitfield.to_bytes())?;

            let bitfield_offsets = PointersOffsetsHeader::from_pointers(offset, pointers);
            output.write_all(&bitfield_offsets.to_bytes())?;

            for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                let (relative_offset, offset_length) =
                    get_relative_offset(offset, pointer.offset());

                serialize_offset(output, relative_offset, offset_length)?;
            }

            write_object_header(output, start, ObjectTag::InodePointers);

            batch.push((object_hash_id, Arc::from(&output[start..])));
        }
        Inode::Directory(dir_id) => {
            // We don't check if it's a new inode because the parent
            // caller (recursively) confirmed it's a new one.

            let dir = storage.get_small_dir(*dir_id)?;
            serialize_directory_or_shape(
                dir,
                object_hash_id,
                offset,
                output,
                storage,
                repository,
                stats,
                strings,
            )?;

            batch.push((object_hash_id, Arc::from(&output[start..])));
        }
    };

    Ok(offset)
}

pub fn deserialize_hash_id(data: &[u8]) -> Result<(Option<HashId>, usize), DeserializationError> {
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

fn deserialize_offset(
    data: &[u8],
    offset_length: OffsetLength,
    object_offset: AbsoluteOffset,
) -> Result<(AbsoluteOffset, usize), DeserializationError> {
    use DeserializationError::*;

    let object_offset = object_offset.as_u64();

    match offset_length {
        OffsetLength::RelativeOneByte => {
            let byte = data.get(0).ok_or(UnexpectedEOF)?;
            let relative_offset: u8 = u8::from_le_bytes([*byte]);
            let absolute_offset: u64 = object_offset - relative_offset as u64;
            Ok((absolute_offset.into(), 1))
        }
        OffsetLength::RelativeTwoBytes => {
            let bytes = data.get(..2).ok_or(UnexpectedEOF)?;
            let relative_offset: u16 = u16::from_le_bytes(bytes.try_into()?);
            let absolute_offset: u64 = object_offset - relative_offset as u64;
            Ok((absolute_offset.into(), 2))
        }
        OffsetLength::RelativeFourBytes => {
            let bytes = data.get(..4).ok_or(UnexpectedEOF)?;
            let relative_offset: u32 = u32::from_le_bytes(bytes.try_into()?);
            let absolute_offset: u64 = object_offset - relative_offset as u64;
            Ok((absolute_offset.into(), 4))
        }
        OffsetLength::RelativeEightBytes => {
            let bytes = data.get(..8).ok_or(UnexpectedEOF)?;
            let relative_offset: u64 = u64::from_le_bytes(bytes.try_into()?);
            let absolute_offset: u64 = object_offset - relative_offset;
            Ok((absolute_offset.into(), 8))
        }
    }
}

fn deserialize_shaped_directory(
    data: &[u8],
    object_offset: AbsoluteOffset,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
    strings: &mut StringInterner,
) -> Result<DirectoryId, DeserializationError> {
    use DeserializationError as Error;
    use DeserializationError::*;

    let mut pos = 0;
    let data_length = data.len();

    let shape_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let shape_id = u32::from_le_bytes(shape_id.try_into()?);
    let shape_id = DirectoryShapeId::from(shape_id);

    let directory_shape = match repository.get_shape(shape_id).map_err(Box::new)? {
        ShapeStrings::SliceIds(slice_ids) => Cow::Borrowed(slice_ids),
        ShapeStrings::Owned(slice_strings) => {
            // We are in the readonly protocol runner.
            // Store the `String` in the `StringInterner`.
            let string_ids: Vec<StringId> = slice_strings
                .iter()
                .map(|s| strings.get_string_id(s))
                .collect();
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
                let (absolute_offset, nbytes) =
                    deserialize_offset(&data[pos..], offset_length, object_offset)?;

                pos += nbytes;

                let dir_entry = DirEntry::new_commited(kind, None, None);
                dir_entry.set_offset(absolute_offset);
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
    object_offset: AbsoluteOffset,
    storage: &mut Storage,
    strings: &mut StringInterner,
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
                    strings.get_string_id(key_str)
                }
                _ => {
                    // The key length is in 2 bytes, followed by the key itself
                    let key_length = data.get(pos..pos + 2).ok_or(UnexpectedEOF)?;
                    let key_length = u16::from_le_bytes(key_length.try_into()?);
                    let key_length = key_length as usize;

                    let key_bytes = data
                        .get(pos + 2..pos + 2 + key_length)
                        .ok_or(UnexpectedEOF)?;
                    let key_str = std::str::from_utf8(key_bytes)?;
                    pos += 2 + key_length;
                    strings.get_string_id(key_str)
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
                let (absolute_offset, nbytes) =
                    deserialize_offset(bytes, offset_length, object_offset)?;

                pos += nbytes;

                let dir_entry = DirEntry::new_commited(kind, None, None);
                dir_entry.set_offset(absolute_offset);
                dir_entry
            };

            let dir_entry_id = storage.add_dir_entry(dir_entry)?;

            new_dir.push((key_id, dir_entry_id));
        }

        Ok(storage.append_to_directories(new_dir))
    })??;

    Ok(dir_id)
}

pub fn read_object_length(
    data: &[u8],
    header: &ObjectHeader,
) -> Result<(usize, usize), DeserializationError> {
    use DeserializationError::*;

    match header.length() {
        ObjectLength::OneByte => {
            let length = data.get(1).copied().ok_or(UnexpectedEOF)? as usize;
            Ok((1 + 1, length))
        }
        ObjectLength::TwoBytes => {
            let length = data.get(1..3).ok_or(UnexpectedEOF)?;
            let length = u16::from_le_bytes(length.try_into()?) as usize;
            Ok((1 + 2, length))
        }
        ObjectLength::FourBytes => {
            let length = data.get(1..5).ok_or(UnexpectedEOF)?;
            let length = u32::from_le_bytes(length.try_into()?) as usize;
            Ok((1 + 4, length))
        }
    }
}

/// Extract values from `data` to store them in `storage`.
/// Return an `Object`, which can be ids (refering to data inside `storage`) or a `Commit`
pub fn deserialize_object(
    bytes: &[u8],
    object_offset: AbsoluteOffset,
    storage: &mut Storage,
    strings: &mut StringInterner,
    repository: &ContextKeyValueStore,
) -> Result<Object, DeserializationError> {
    use DeserializationError::*;

    let header = bytes.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    let (header_nbytes, object_length) = read_object_length(bytes, &header)?;

    let bytes = bytes
        .get(header_nbytes..object_length)
        .ok_or(UnexpectedEOF)?;

    let (object_hash_id, nbytes) = deserialize_hash_id(bytes)?;

    storage
        .offsets_to_hash_id
        .insert(object_offset, object_hash_id.ok_or(MissingOffset)?);

    let bytes = bytes.get(nbytes..).ok_or(UnexpectedEOF)?;
    let mut pos = 0;

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::Directory => {
            deserialize_directory(bytes, object_offset, storage, strings).map(Object::Directory)
        }
        ObjectTag::ShapedDirectory => {
            deserialize_shaped_directory(bytes, object_offset, storage, repository, strings)
                .map(Object::Directory)
        }
        ObjectTag::Blob => storage
            .add_blob_by_ref(bytes)
            .map(Object::Blob)
            .map_err(Into::into),
        ObjectTag::InodePointers => {
            let inode =
                deserialize_inode_pointers(bytes, object_offset, storage, repository, strings)?;
            let inode_id = storage.add_inode(inode)?;

            Ok(Object::Directory(inode_id.into()))
        }
        ObjectTag::Commit => {
            let header = bytes.get(pos).ok_or(UnexpectedEOF)?;
            let header = CommitHeader::from_bytes([*header; 1]);

            pos += 1;

            let parent_commit_ref = if header.is_parent_exist() {
                let data = bytes.get(pos..).ok_or(UnexpectedEOF)?;
                let (parent_commit_hash, nbytes) = deserialize_hash_id(data)?;

                pos += nbytes;

                let data = bytes.get(pos..).ok_or(UnexpectedEOF)?;
                let (parent_absolute_offset, nbytes) =
                    deserialize_offset(data, header.parent_offset_length(), object_offset)?;

                pos += nbytes;

                Some(ObjectReference::new(
                    parent_commit_hash,
                    Some(parent_absolute_offset),
                ))
            } else {
                None
            };

            let data = bytes.get(pos..).ok_or(UnexpectedEOF)?;
            let (root_hash, nbytes) = deserialize_hash_id(data)?;

            pos += nbytes;

            let data = bytes.get(pos..).ok_or(UnexpectedEOF)?;
            let (root_absolute_offset, nbytes) =
                deserialize_offset(data, header.root_offset_length(), object_offset)?;

            let root_ref = ObjectReference::new(
                Some(root_hash.ok_or(MissingRootHash)?),
                Some(root_absolute_offset),
            );

            pos += nbytes;

            let time = bytes.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
            let time = u64::from_le_bytes(time.try_into()?);

            pos += 8;

            let author_length: usize = match header.author_length() {
                ObjectLength::OneByte => {
                    let author_length: u8 = *bytes.get(pos).ok_or(UnexpectedEOF)?;
                    pos += 1;
                    author_length as usize
                }
                ObjectLength::TwoBytes => {
                    let author_length = bytes.get(pos..pos + 2).ok_or(UnexpectedEOF)?;
                    pos += 2;
                    u16::from_le_bytes(author_length.try_into()?) as usize
                }
                ObjectLength::FourBytes => {
                    let author_length = bytes.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
                    pos += 4;
                    u32::from_le_bytes(author_length.try_into()?) as usize
                }
            };

            let author = bytes.get(pos..pos + author_length).ok_or(UnexpectedEOF)?;
            let author = author.to_vec();

            pos += author_length;

            let message = bytes.get(pos..).ok_or(UnexpectedEOF)?;
            let message = message.to_vec();

            Ok(Object::Commit(Box::new(Commit {
                parent_commit_ref,
                root_ref,
                time,
                author: String::from_utf8(author)?,
                message: String::from_utf8(message)?,
            })))
        }
    }
}

fn deserialize_inode_pointers(
    data: &[u8],
    object_offset: AbsoluteOffset,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
    strings: &mut StringInterner,
) -> Result<Inode, DeserializationError> {
    use DeserializationError::*;

    let mut pos = 0;

    let depth = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let depth = u32::from_le_bytes(depth.try_into()?);

    let nchildren = data.get(pos + 4..pos + 8).ok_or(UnexpectedEOF)?;
    let nchildren = u32::from_le_bytes(nchildren.try_into()?);

    pos += 8;

    let pointers_header = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let pointers_header = PointersHeader::from_bytes(pointers_header.try_into()?);

    let npointers = pointers_header.count();
    let indexes_iter = pointers_header.iter();

    pos += 4;

    let offsets_header = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
    let offsets_header = PointersOffsetsHeader::from_bytes(offsets_header.try_into()?);

    pos += 8;

    let mut pointers: [Option<PointerToInode>; 32] = Default::default();

    for (index, pointer_index) in indexes_iter.enumerate() {
        let offset_length = offsets_header.get(index);
        let (absolute_offset, nbytes) =
            deserialize_offset(&data[pos..], offset_length, object_offset)?;

        pos += nbytes;

        // TODO: Move this outside the loop
        let mut output = Vec::with_capacity(1000);

        let object_ref = ObjectReference::new(None, Some(absolute_offset));
        let data = repository
            .get_object_bytes(object_ref, &mut output)
            .map_err(Box::new)?;

        let inode_id = deserialize_inode(data, absolute_offset, storage, repository, strings)?;

        pointers[pointer_index as usize] = Some(PointerToInode::new_commited(
            None,
            inode_id,
            Some(absolute_offset),
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
    object_offset: AbsoluteOffset,
    storage: &mut Storage,
    repository: &ContextKeyValueStore,
    strings: &mut StringInterner,
) -> Result<InodeId, DeserializationError> {
    use DeserializationError::*;

    let header = data.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    let (header_nbytes, _) = read_object_length(data, &header)?;
    let data = data.get(header_nbytes..).ok_or(UnexpectedEOF)?;

    let (_, nbytes) = deserialize_hash_id(data)?;
    let data = data.get(nbytes..).ok_or(UnexpectedEOF)?;

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::InodePointers => {
            let inode =
                deserialize_inode_pointers(data, object_offset, storage, repository, strings)?;
            storage.add_inode(inode).map_err(Into::into)
        }
        ObjectTag::Directory => {
            let dir_id = deserialize_directory(data, object_offset, storage, strings)?;
            storage
                .add_inode(Inode::Directory(dir_id))
                .map_err(Into::into)
        }
        ObjectTag::ShapedDirectory => {
            let dir_id =
                deserialize_shaped_directory(data, object_offset, storage, repository, strings)?;
            storage
                .add_inode(Inode::Directory(dir_id))
                .map_err(Into::into)
        }
        _ => Err(UnknownID),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use tezos_timing::SerializeStats;

    use crate::persistent::KeyValueStoreBackend;
    use crate::working_tree::string_interner::StringInterner;
    use crate::{
        hash::hash_object, kv_store::persistent::Persistent, working_tree::storage::DirectoryId,
    };

    use super::*;

    #[test]
    fn test_serialize() {
        let mut storage = Storage::new();
        let mut strings = StringInterner::default();
        let mut repo = Persistent::try_new().unwrap();
        let mut stats = SerializeStats::default();
        let mut batch = Vec::new();
        let mut older_objects = Vec::new();
        let fake_hash_id = HashId::try_from(1).unwrap();

        let offset = repo.synchronize_data(&[], &[0, 0, 0, 0, 0, 0]).unwrap();

        // Test Object::Directory

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(0.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(0.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(0.into()),
                &mut strings,
            )
            .unwrap();

        let mut bytes = Vec::with_capacity(1024);
        let offset = serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset,
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &mut strings).unwrap(),
                storage.get_owned_dir(object, &mut strings).unwrap()
            )
        } else {
            panic!();
        }

        // Test Object::Directory (Shaped)

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(1.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(2.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aa",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(3.into()),
                &mut strings,
            )
            .unwrap();

        let offset = repo.synchronize_data(&[], &bytes).unwrap();
        bytes.clear();

        let mut bytes = Vec::with_capacity(1024);
        let offset = serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset,
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &mut strings).unwrap(),
                storage.get_owned_dir(object, &mut strings).unwrap()
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
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(1.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab1",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(2.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aa1",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(3.into()),
                &mut strings,
            )
            .unwrap();

        let offset = repo.synchronize_data(&[], &bytes).unwrap();
        bytes.clear();

        let mut bytes = Vec::with_capacity(1024);
        let offset = serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset,
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &mut strings).unwrap(),
                storage.get_owned_dir(object, &mut strings).unwrap()
            )
        } else {
            panic!();
        }

        // Test Object::Blob

        // Not inlined value
        let blob_id = storage.add_blob_by_ref(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let mut bytes = Vec::with_capacity(1024);
        let offset = serialize_object(
            &Object::Blob(blob_id),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            Some(0.into()),
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();
        if let Object::Blob(object) = object {
            let blob = storage.get_blob(object).unwrap();
            assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        } else {
            panic!();
        }

        // Test Object::Commit

        let offset = repo.synchronize_data(&[], &bytes).unwrap();
        bytes.clear();

        let commit = Commit {
            parent_commit_ref: Some(ObjectReference::new(HashId::new(9876), Some(1.into()))),
            root_ref: ObjectReference::new(HashId::new(12345), Some(2.into())),
            time: 123456,
            author: "123".to_string(),
            message: "abc".to_string(),
        };

        let offset = serialize_object(
            &Object::Commit(Box::new(commit.clone())),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset,
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();
        if let Object::Commit(object) = object {
            assert_eq!(*object, commit);
        } else {
            panic!();
        }

        // Test Object::Commit with no parent

        let offset = repo.synchronize_data(&[], &bytes).unwrap();
        bytes.clear();

        let commit = Commit {
            parent_commit_ref: None,
            root_ref: ObjectReference::new(HashId::new(12), Some(3.into())),
            time: 1234567,
            author: "123456".repeat(100),
            message: "abcd".repeat(100),
        };

        let offset = serialize_object(
            &Object::Commit(Box::new(commit.clone())),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            offset,
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();
        if let Object::Commit(object) = object {
            assert_eq!(*object, commit);
        } else {
            panic!();
        }

        let offset = repo.synchronize_data(&[], &bytes).unwrap();

        bytes.clear();

        let mut offsets = Vec::with_capacity(32);

        // Test Inode::Directory

        let mut pointers: [Option<PointerToInode>; 32] = Default::default();

        for index in 0..pointers.len() {
            let inode_value = Inode::Directory(DirectoryId::empty());
            let inode_value_id = storage.add_inode(inode_value).unwrap();

            let hash_id = HashId::new((index + 1) as u32).unwrap();

            let offset = serialize_inode(
                inode_value_id,
                &mut bytes,
                hash_id,
                &storage,
                &mut stats,
                &mut batch,
                &mut repo,
                offset.unwrap(),
                &strings,
            )
            .unwrap();

            offsets.push(offset);

            pointers[index] = Some(PointerToInode::new_commited(
                Some(hash_id),
                inode_value_id,
                Some(offset),
            ));
        }

        let inode = Inode::Pointers {
            depth: 100,
            nchildren: 200,
            npointers: 250,
            pointers,
        };

        let inode_id = storage.add_inode(inode).unwrap();

        let hash_id = HashId::new(123).unwrap();
        batch.clear();
        let offset = serialize_inode(
            inode_id,
            &mut bytes,
            hash_id,
            &storage,
            &mut stats,
            &mut batch,
            &mut repo,
            //            5,
            offset.unwrap(),
            &strings,
        )
        .unwrap();

        repo.synchronize_data(&[], &bytes).unwrap();

        let new_inode_id = deserialize_inode(
            &batch.last().unwrap().1,
            offset,
            &mut storage,
            &repo,
            &mut strings,
        )
        .unwrap();
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

                let inode = storage.get_inode(pointer.inode_id()).unwrap();
                match inode {
                    Inode::Directory(dir_id) => assert!(dir_id.is_empty()),
                    _ => panic!(),
                }

                assert_eq!(pointer.offset(), offsets[index]);
            }
        } else {
            panic!()
        }

        // Test Inode::Value

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(1.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(2.into()),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(3.into()),
                &mut strings,
            )
            .unwrap();

        let inode = Inode::Directory(dir_id);
        let inode_id = storage.add_inode(inode).unwrap();

        batch.clear();
        let offset = serialize_inode(
            inode_id, &mut bytes, hash_id, &storage, &mut stats, &mut batch, &mut repo, offset,
            &strings,
        )
        .unwrap();

        repo.synchronize_data(&[], &bytes).unwrap();

        assert_eq!(batch.len(), 1);

        let new_inode_id = deserialize_inode(
            &batch.last().unwrap().1,
            offset,
            &mut storage,
            &repo,
            &mut strings,
        )
        .unwrap();
        let new_inode = storage.get_inode(new_inode_id).unwrap();

        if let Inode::Directory(new_dir_id) = new_inode {
            assert_eq!(
                storage.get_owned_dir(dir_id, &mut strings).unwrap(),
                storage.get_owned_dir(*new_dir_id, &mut strings).unwrap()
            )
        }
    }

    #[test]
    fn test_serialize_empty_blob() {
        let mut repo = Persistent::try_new().expect("failed to create context");
        let mut storage = Storage::new();
        let mut strings = StringInterner::default();
        let mut stats = SerializeStats::default();
        let mut batch = Vec::new();
        let mut older_objects = Vec::new();

        let fake_hash_id = HashId::try_from(1).unwrap();

        let blob_id = storage.add_blob_by_ref(&[]).unwrap();
        let blob = Object::Blob(blob_id);
        let blob_hash_id = hash_object(&blob, &mut repo, &storage, &strings).unwrap();

        assert!(blob_hash_id.is_some());

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, None, None).with_offset(0.into()),
                &mut strings,
            )
            .unwrap();

        let mut bytes = Vec::with_capacity(1024);

        let offset = serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut bytes,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            Some(0.into()),
        )
        .unwrap();

        let object =
            deserialize_object(&bytes, offset.unwrap(), &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &strings).unwrap(),
                storage.get_owned_dir(object, &strings).unwrap()
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
