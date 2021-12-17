// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Serialization/deserialization for objects in the Working Tree so that they can be
//! saved/loaded to/from the repository.

use std::{borrow::Cow, convert::TryInto, io::Write, sync::Arc};

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;
use tezos_timing::SerializeStats;

use crate::{
    kv_store::HashId,
    serialize::{deserialize_hash_id, ObjectHeader, ObjectTag, PointersHeader},
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
    persistent::AbsoluteOffset, serialize_hash_id, DeserializationError, SerializationError,
};

#[bitfield(bits = 8)]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct KeyDirEntryDescriptor {
    kind: DirEntryKind,
    blob_inline_length: B3,
    key_inline_length: B4,
}

// Must fit in 1 byte
assert_eq_size!(KeyDirEntryDescriptor, u8);

fn serialize_shaped_directory(
    shape_id: DirectoryShapeId,
    dir: &[(StringId, DirEntryId)],
    output: &mut Vec<u8>,
    storage: &Storage,
    repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    let mut nblobs_inlined: usize = 0;
    let mut blobs_length: usize = 0;

    let header: [u8; 1] = ObjectHeader::new()
        .with_tag(ObjectTag::ShapedDirectory)
        .with_is_persistent(false)
        .into_bytes();
    output.write_all(&header)?;

    let shape_id = shape_id.as_u32();
    output.write_all(&shape_id.to_ne_bytes())?;

    // Make sure that SHAPED_DIRECTORY_NBYTES_TO_HASHES is correct.
    debug_assert_eq!(output.len(), SHAPED_DIRECTORY_NBYTES_TO_HASHES);

    for (_, dir_entry_id) in dir {
        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let hash_id = dir_entry.hash_id();
        let kind = dir_entry.dir_entry_kind();

        let blob_inline = dir_entry.get_inlined_blob(storage);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        let byte: [u8; 1] = KeyDirEntryDescriptor::new()
            .with_kind(kind)
            .with_key_inline_length(0)
            .with_blob_inline_length(blob_inline_length as u8)
            .into_bytes();
        output.write_all(&byte[..])?;

        if let Some(blob_inline) = blob_inline {
            nblobs_inlined += 1;
            blobs_length += blob_inline.len();

            output.write_all(&blob_inline)?;
        } else {
            serialize_hash_id(hash_id, output, repository, stats)?;
        }
    }

    stats.add_shape(nblobs_inlined, blobs_length);

    Ok(())
}

fn serialize_directory(
    dir: &[(StringId, DirEntryId)],
    output: &mut Vec<u8>,
    storage: &Storage,
    strings: &StringInterner,
    repository: &mut ContextKeyValueStore,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    let mut keys_length: usize = 0;
    let mut nblobs_inlined: usize = 0;
    let mut blobs_length: usize = 0;

    if let Some(shape_id) = repository.make_shape(dir)? {
        return serialize_shaped_directory(shape_id, dir, output, storage, repository, stats);
    };

    let header: [u8; 1] = ObjectHeader::new()
        .with_tag(ObjectTag::Directory)
        .with_is_persistent(false)
        .into_bytes();
    output.write_all(&header)?;

    for (key_id, dir_entry_id) in dir {
        let key = strings.get_str(*key_id)?;

        let dir_entry = storage.get_dir_entry(*dir_entry_id)?;

        let hash_id = dir_entry.hash_id();
        let kind = dir_entry.dir_entry_kind();

        let blob_inline = dir_entry.get_inlined_blob(storage);
        let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

        match key.len() {
            len if len != 0 && len < 16 => {
                let byte: [u8; 1] = KeyDirEntryDescriptor::new()
                    .with_kind(kind)
                    .with_key_inline_length(len as u8)
                    .with_blob_inline_length(blob_inline_length as u8)
                    .into_bytes();
                output.write_all(&byte[..])?;
                output.write_all(key.as_bytes())?;
                keys_length += len;
            }
            len => {
                let byte: [u8; 1] = KeyDirEntryDescriptor::new()
                    .with_kind(kind)
                    .with_key_inline_length(0)
                    .with_blob_inline_length(blob_inline_length as u8)
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
            serialize_hash_id(hash_id, output, repository, stats)?;
        }
    }

    stats.add_directory(keys_length, nblobs_inlined, blobs_length);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn serialize_object(
    object: &Object,
    object_hash_id: HashId,
    output: &mut Vec<u8>,
    storage: &Storage,
    strings: &StringInterner,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>,
    referenced_older_objects: &mut Vec<HashId>,
    repository: &mut ContextKeyValueStore,
    _object_offset: Option<AbsoluteOffset>,
) -> Result<Option<AbsoluteOffset>, SerializationError> {
    output.clear();

    match object {
        Object::Directory(dir_id) => {
            if let Some(inode_id) = dir_id.get_inode_id() {
                serialize_inode(
                    inode_id,
                    output,
                    object_hash_id,
                    storage,
                    strings,
                    stats,
                    batch,
                    referenced_older_objects,
                    repository,
                )?;
            } else {
                let dir = storage.get_small_dir(*dir_id)?;

                serialize_directory(dir.as_ref(), output, storage, strings, repository, stats)?;

                batch.push((object_hash_id, Arc::from(output.as_slice())));
            }
        }
        Object::Blob(blob_id) => {
            debug_assert!(!blob_id.is_inline());

            let blob = storage.get_blob(*blob_id)?;

            let header: [u8; 1] = ObjectHeader::new()
                .with_tag(ObjectTag::Blob)
                .with_is_persistent(false)
                .into_bytes();
            output.write_all(&header)?;

            output.write_all(blob.as_ref())?;

            stats.add_blob(blob.len());

            batch.push((object_hash_id, Arc::from(output.as_slice())));
        }
        Object::Commit(commit) => {
            let header: [u8; 1] = ObjectHeader::new()
                .with_tag(ObjectTag::Commit)
                .with_is_persistent(false)
                .into_bytes();
            output.write_all(&header)?;

            let parent_hash_id = commit.parent_commit_ref.and_then(|p| p.hash_id_opt());
            serialize_hash_id(parent_hash_id, output, repository, stats)?;

            let root_hash_id = commit.root_ref.hash_id();
            serialize_hash_id(root_hash_id, output, repository, stats)?;

            output.write_all(&commit.time.to_ne_bytes())?;

            let author_length: u32 = commit.author.len().try_into()?;
            output.write_all(&author_length.to_ne_bytes())?;
            output.write_all(commit.author.as_bytes())?;

            // The message length is inferred.
            // It's until the end of the slice
            output.write_all(commit.message.as_bytes())?;

            batch.push((object_hash_id, Arc::from(output.as_slice())));
        }
    }

    stats.total_bytes += output.len();

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn serialize_inode(
    inode_id: InodeId,
    output: &mut Vec<u8>,
    hash_id: HashId,
    storage: &Storage,
    strings: &StringInterner,
    stats: &mut SerializeStats,
    batch: &mut Vec<(HashId, Arc<[u8]>)>,
    referenced_older_objects: &mut Vec<HashId>,
    repository: &mut ContextKeyValueStore,
) -> Result<(), SerializationError> {
    use SerializationError::*;

    output.clear();
    let inode = storage.get_inode(inode_id)?;

    match inode {
        Inode::Pointers {
            depth,
            nchildren,
            npointers: _,
            pointers,
        } => {
            stats.add_inode_pointers();

            let header: [u8; 1] = ObjectHeader::new()
                .with_tag(ObjectTag::InodePointers)
                .with_is_persistent(false)
                .into_bytes();
            output.write_all(&header)?;

            output.write_all(&depth.to_ne_bytes())?;
            output.write_all(&nchildren.to_ne_bytes())?;

            let bitfield = PointersHeader::from(pointers);
            output.write_all(&bitfield.to_bytes())?;

            // Make sure that INODE_POINTERS_NBYTES_TO_HASHES is correct.
            debug_assert_eq!(output.len(), INODE_POINTERS_NBYTES_TO_HASHES);

            for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                let hash_id = pointer
                    .hash_id(storage, repository)
                    .unwrap()
                    .ok_or(MissingHashId)?;

                serialize_hash_id(hash_id, output, repository, stats)?;
            }

            batch.push((hash_id, Arc::from(output.as_slice())));

            // Recursively serialize all children
            for pointer in pointers.iter().filter_map(|p| p.as_ref()) {
                let hash_id = pointer
                    .hash_id(storage, repository)
                    .unwrap()
                    .ok_or(MissingHashId)?;

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
                serialize_inode(
                    inode_id,
                    output,
                    hash_id,
                    storage,
                    strings,
                    stats,
                    batch,
                    referenced_older_objects,
                    repository,
                )?;
            }
        }
        Inode::Directory(dir_id) => {
            // We don't check if it's a new inode because the parent
            // caller (recursively) confirmed it's a new one.

            let dir = storage.get_small_dir(*dir_id)?;
            serialize_directory(dir.as_ref(), output, storage, strings, repository, stats)?;

            batch.push((hash_id, Arc::from(output.as_slice())));
        }
    };

    Ok(())
}

fn deserialize_shaped_directory(
    data: &[u8],
    storage: &mut Storage,
    strings: &mut StringInterner,
    repository: &ContextKeyValueStore,
) -> Result<DirectoryId, DeserializationError> {
    use DeserializationError as Error;
    use DeserializationError::*;

    let mut pos = 1;
    let data_length = data.len();

    let shape_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let shape_id = u32::from_ne_bytes(shape_id.try_into()?);
    let shape_id = DirectoryShapeId::from(shape_id);

    let directory_shape = match repository.get_shape(shape_id).map_err(Box::new)? {
        ShapeStrings::SliceIds(slice_ids) => slice_ids,
        ShapeStrings::Owned(strings_slice) => {
            // We are in the readonly protocol runner.
            // Store the `String` in the `StringInterner`.
            let string_ids: Vec<StringId> = strings_slice
                .iter()
                .map(|s| strings.make_string_id(s))
                .collect();
            Cow::Owned(string_ids)
        }
    };

    let mut directory_shape = directory_shape.as_ref().iter();

    pos += 4;

    let dir_id = storage.with_new_dir::<_, Result<_, Error>>(|storage, new_dir| {
        while pos < data_length {
            let descriptor = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
            let descriptor = KeyDirEntryDescriptor::from_bytes([descriptor[0]; 1]);

            pos += 1;

            let key_id = directory_shape.next().copied().ok_or(CannotFindNextShape)?;

            let kind = descriptor.kind();
            let blob_inline_length = descriptor.blob_inline_length() as usize;

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

                DirEntry::new_commited(kind, Some(hash_id.ok_or(MissingHash)?), None)
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
    storage: &mut Storage,
    strings: &mut StringInterner,
) -> Result<DirectoryId, DeserializationError> {
    use DeserializationError as Error;
    use DeserializationError::*;

    let mut pos = 1;
    let data_length = data.len();

    let dir_id = storage.with_new_dir::<_, Result<_, Error>>(|storage, new_dir| {
        while pos < data_length {
            let descriptor = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
            let descriptor = KeyDirEntryDescriptor::from_bytes([descriptor[0]; 1]);

            pos += 1;

            let key_id = match descriptor.key_inline_length() as usize {
                len if len > 0 => {
                    // The key is in the next `len` bytes
                    let key_bytes = data.get(pos..pos + len).ok_or(UnexpectedEOF)?;
                    let key_str = std::str::from_utf8(key_bytes)?;
                    pos += len;
                    strings.make_string_id(key_str)
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
                    strings.make_string_id(key_str)
                }
            };

            let kind = descriptor.kind();
            let blob_inline_length = descriptor.blob_inline_length() as usize;

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

                DirEntry::new_commited(kind, Some(hash_id.ok_or(MissingHash)?), None)
            };

            let dir_entry_id = storage.add_dir_entry(dir_entry)?;

            new_dir.push((key_id, dir_entry_id));
        }

        Ok(storage.append_to_directories(new_dir))
    })??;

    Ok(dir_id)
}

/// Extract values from `data` to store them in `storage`.
/// Return an `Object`, which can be ids (refering to data inside `storage`) or a `Commit`
pub fn deserialize_object(
    data: &[u8],
    storage: &mut Storage,
    strings: &mut StringInterner,
    repository: &ContextKeyValueStore,
) -> Result<Object, DeserializationError> {
    use DeserializationError::*;

    let header = data.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    let mut pos = 1;

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::Directory => {
            let dir_id = deserialize_directory(data, storage, strings)?;
            Ok(Object::Directory(dir_id))
        }
        ObjectTag::ShapedDirectory => {
            let dir_id = deserialize_shaped_directory(data, storage, strings, repository)?;
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
                parent_commit_ref: parent_commit_hash.map(|p| ObjectReference::new(Some(p), None)),
                root_ref: ObjectReference::new(Some(root_hash.ok_or(MissingRootHash)?), None),
                time,
                author: String::from_utf8(author)?,
                message: String::from_utf8(message)?,
            })))
        }
        ObjectTag::InodePointers => {
            let inode = deserialize_inode_pointers(&data[1..], storage, strings, repository)?;
            let inode_id = storage.add_inode(inode)?;

            Ok(Object::Directory(inode_id.into()))
        }
    }
}

fn deserialize_inode_pointers(
    data: &[u8],
    storage: &mut Storage,
    strings: &mut StringInterner,
    repository: &ContextKeyValueStore,
) -> Result<Inode, DeserializationError> {
    use DeserializationError::*;

    let mut pos = 0;

    let depth = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let depth = u32::from_ne_bytes(depth.try_into()?);

    let nchildren = data.get(pos + 4..pos + 8).ok_or(UnexpectedEOF)?;
    let nchildren = u32::from_ne_bytes(nchildren.try_into()?);

    pos += 8;

    let descriptor = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
    let descriptor = PointersHeader::from_bytes(descriptor.try_into()?);

    let npointers = descriptor.count();
    let indexes_iter = descriptor.iter();

    pos += 4;

    let mut pointers: [Option<PointerToInode>; 32] = Default::default();

    for index in indexes_iter {
        let bytes = data.get(pos..).ok_or(UnexpectedEOF)?;
        let (hash_id, nbytes) = deserialize_hash_id(bytes)?;

        pos += nbytes;

        let mut output = Vec::with_capacity(1000);

        let object_ref = ObjectReference::new(Some(hash_id.ok_or(MissingHash)?), None);
        let data = repository
            .get_object_bytes(object_ref, &mut output)
            .map_err(Box::new)?;
        let inode_id = deserialize_inode(data, storage, strings, repository)?;

        pointers[index as usize] = Some(PointerToInode::new_commited(
            Some(hash_id.ok_or(MissingHash)?),
            inode_id,
            None,
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
    storage: &mut Storage,
    strings: &mut StringInterner,
    repository: &ContextKeyValueStore,
) -> Result<InodeId, DeserializationError> {
    use DeserializationError::*;

    let header = data.get(0).copied().ok_or(UnexpectedEOF)?;
    let header: ObjectHeader = ObjectHeader::from_bytes([header]);

    match header.tag_or_err().map_err(|_| UnknownID)? {
        ObjectTag::InodePointers => {
            let inode = deserialize_inode_pointers(&data[1..], storage, strings, repository)?;
            storage.add_inode(inode).map_err(Into::into)
        }
        ObjectTag::Directory => {
            let dir_id = deserialize_directory(data, storage, strings)?;
            storage
                .add_inode(Inode::Directory(dir_id))
                .map_err(Into::into)
        }
        ObjectTag::ShapedDirectory => {
            let dir_id = deserialize_shaped_directory(data, storage, strings, repository)?;
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
const INODE_POINTERS_NBYTES_TO_HASHES: usize = 13;

/// Number of bytes to reach the hashes when serializing a shaped directory.
///
/// This skip `ID_SHAPED_DIRECTORY` and the `ShapeId`
const SHAPED_DIRECTORY_NBYTES_TO_HASHES: usize = 5;

impl<'a> Iterator for HashIdIterator<'a> {
    type Item = HashId;

    fn next(&mut self) -> Option<Self::Item> {
        let header = self.data.get(0).copied()?;
        let header: ObjectHeader = ObjectHeader::from_bytes([header]);

        let tag = header.tag_or_err().ok()?;

        loop {
            let mut pos = self.pos;

            if pos == 0 {
                match tag {
                    ObjectTag::Blob => {
                        // No HashId in Object::Blob
                        return None;
                    }
                    ObjectTag::Commit => {
                        // Deserialize the parent hash to know it's size
                        let (_, nbytes) = deserialize_hash_id(self.data.get(1..)?).ok()?;

                        // Object::Commit.root_hash
                        let (root_hash, _) =
                            deserialize_hash_id(self.data.get(1 + nbytes..)?).ok()?;
                        self.pos = self.data.len();

                        return root_hash;
                    }
                    ObjectTag::InodePointers => {
                        // We skip the first bytes (ID_INODE_POINTERS, depth, nchildren, ..) to reach
                        // the hashes
                        pos += INODE_POINTERS_NBYTES_TO_HASHES;
                    }
                    ObjectTag::ShapedDirectory => {
                        pos += SHAPED_DIRECTORY_NBYTES_TO_HASHES;
                    }
                    ObjectTag::Directory => {
                        // Skip the tag (ID_DIRECTORY)
                        pos += 1;
                    }
                }
            }

            if tag == ObjectTag::InodePointers {
                let bytes = self.data.get(pos..)?;
                let (hash_id, nbytes) = deserialize_hash_id(bytes).ok()?;

                self.pos = pos + nbytes;

                return hash_id;
            } else {
                // ID_DIRECTORY or ID_SHAPED_DIRECTORY

                let descriptor = self.data.get(pos..pos + 1)?;
                let descriptor = KeyDirEntryDescriptor::from_bytes([descriptor[0]; 1]);

                pos += 1;

                if tag != ObjectTag::ShapedDirectory {
                    // ID_SHAPED_DIRECTORY do not contain the keys

                    let offset = match descriptor.key_inline_length() as usize {
                        len if len > 0 => len,
                        _ => {
                            let key_length = self.data.get(pos..pos + 2)?;
                            let key_length = u16::from_ne_bytes(key_length.try_into().ok()?);
                            2 + key_length as usize
                        }
                    };

                    pos += offset;
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

                return hash_id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use tezos_timing::SerializeStats;

    use crate::{
        hash::hash_object, kv_store::in_memory::InMemory, working_tree::storage::DirectoryId,
    };

    use super::*;

    #[test]
    fn test_serialize() {
        let mut storage = Storage::new();
        let mut strings = StringInterner::default();
        let mut repo = InMemory::try_new().unwrap();
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
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None),
                &mut strings,
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            None,
        )
        .unwrap();

        let object = deserialize_object(&data, &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &strings).unwrap(),
                storage.get_owned_dir(object, &strings).unwrap()
            )
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_u64()).collect::<Vec<_>>(), &[3, 1, 2]);

        // Test Object::Directory (Shaped)

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(1), None),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None),
                &mut strings,
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            None,
        )
        .unwrap();

        let object = deserialize_object(&data, &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &strings).unwrap(),
                storage.get_owned_dir(object, &strings).unwrap()
            )
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_u64()).collect::<Vec<_>>(), &[3, 1, 2]);

        // Test Object::Blob

        // Not inlined value
        let blob_id = storage.add_blob_by_ref(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_object(
            &Object::Blob(blob_id),
            fake_hash_id,
            &mut data,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            None,
        )
        .unwrap();
        let object = deserialize_object(&data, &mut storage, &mut strings, &repo).unwrap();
        if let Object::Blob(object) = object {
            let blob = storage.get_blob(object).unwrap();
            assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        } else {
            panic!();
        }
        let iter = iter_hash_ids(&data);
        assert_eq!(iter.count(), 0);
        // Test Object::Commit

        let mut data = Vec::with_capacity(1024);

        let commit = Commit {
            parent_commit_ref: Some(ObjectReference::new(HashId::new(9876), None)),
            root_ref: ObjectReference::new(HashId::new(12345), None),
            time: 12345,
            author: "123".to_string(),
            message: "abc".to_string(),
        };

        serialize_object(
            &Object::Commit(Box::new(commit.clone())),
            fake_hash_id,
            &mut data,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            None,
        )
        .unwrap();
        let object = deserialize_object(&data, &mut storage, &mut strings, &repo).unwrap();
        if let Object::Commit(object) = object {
            assert_eq!(*object, commit);
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_u64()).collect::<Vec<_>>(), &[12345]);

        // Test Inode::Directory

        let mut pointers: [Option<PointerToInode>; 32] = Default::default();

        for index in 0..pointers.len() {
            let inode_value = Inode::Directory(DirectoryId::empty());
            let inode_value_id = storage.add_inode(inode_value).unwrap();

            let hash_id = HashId::new((index + 1) as u64).unwrap();

            repo.write_batch(vec![(hash_id, Arc::new(ObjectHeader::new().into_bytes()))])
                .unwrap();

            pointers[index] = Some(PointerToInode::new(Some(hash_id), inode_value_id));
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
        serialize_inode(
            inode_id,
            &mut data,
            hash_id,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
        )
        .unwrap();

        let new_inode_id =
            deserialize_inode(&batch[0].1, &mut storage, &mut strings, &repo).unwrap();
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
                let hash_id = pointer.hash_id(&storage, &repo).unwrap().unwrap();
                assert_eq!(hash_id.as_u64() as usize, index + 1);

                let inode = storage.get_inode(pointer.inode_id()).unwrap();
                match inode {
                    Inode::Directory(dir_id) => assert!(dir_id.is_empty()),
                    _ => panic!(),
                }
            }
        } else {
            panic!()
        }

        let iter = iter_hash_ids(&batch[0].1);
        assert_eq!(
            iter.map(|h| h.as_u64()).collect::<Vec<_>>(),
            (1..33).collect::<Vec<_>>()
        );

        // Test Inode::Value

        let dir_id = DirectoryId::empty();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "a",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(1), None),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "bab",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(2), None),
                &mut strings,
            )
            .unwrap();
        let dir_id = storage
            .dir_insert(
                dir_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                DirEntry::new_commited(DirEntryKind::Blob, HashId::new(3), None),
                &mut strings,
            )
            .unwrap();

        let inode = Inode::Directory(dir_id);
        let inode_id = storage.add_inode(inode).unwrap();

        batch.clear();
        serialize_inode(
            inode_id,
            &mut data,
            hash_id,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
        )
        .unwrap();

        let new_inode_id =
            deserialize_inode(&batch[0].1, &mut storage, &mut strings, &repo).unwrap();
        let new_inode = storage.get_inode(new_inode_id).unwrap();

        if let Inode::Directory(new_dir_id) = new_inode {
            assert_eq!(
                storage.get_owned_dir(dir_id, &strings).unwrap(),
                storage.get_owned_dir(*new_dir_id, &strings).unwrap()
            )
        }

        let iter = iter_hash_ids(&batch[0].1);
        assert_eq!(iter.map(|h| h.as_u64()).collect::<Vec<_>>(), &[3, 1, 2]);
    }

    #[test]
    fn test_serialize_empty_blob() {
        let mut repo = InMemory::try_new().expect("failed to create context");
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
                DirEntry::new_commited(DirEntryKind::Blob, blob_hash_id, None),
                &mut strings,
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);

        serialize_object(
            &Object::Directory(dir_id),
            fake_hash_id,
            &mut data,
            &storage,
            &strings,
            &mut stats,
            &mut batch,
            &mut older_objects,
            &mut repo,
            None,
        )
        .unwrap();

        let object = deserialize_object(&data, &mut storage, &mut strings, &repo).unwrap();

        if let Object::Directory(object) = object {
            assert_eq!(
                storage.get_owned_dir(dir_id, &strings).unwrap(),
                storage.get_owned_dir(object, &strings).unwrap()
            )
        } else {
            panic!();
        }
    }
}
