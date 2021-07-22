// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    array::TryFromSliceError, convert::TryInto, io::Write, num::TryFromIntError, str::Utf8Error,
    string::FromUtf8Error,
};

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;
use tezos_timing::SerializeStats;

use crate::{
    kv_store::HashId,
    working_tree::{Commit, NodeKind},
};

use super::{
    storage::{Blob, NodeIdError, Storage, StorageIdError},
    Entry, Node,
};

const ID_TREE: u8 = 0;
const ID_BLOB: u8 = 1;
const ID_COMMIT: u8 = 2;
const ID_INODE: u8 = 3;

const COMPACT_HASH_ID_BIT: u32 = 1 << 23;

#[derive(Debug)]
pub enum SerializationError {
    IOError,
    TreeNotFound,
    NodeNotFound,
    BlobNotFound,
    TryFromIntError,
    StorageIdError { error: StorageIdError },
    HashIdTooBig,
}

impl From<std::io::Error> for SerializationError {
    fn from(_: std::io::Error) -> Self {
        Self::IOError
    }
}

impl From<TryFromIntError> for SerializationError {
    fn from(_: TryFromIntError) -> Self {
        Self::TryFromIntError
    }
}

impl From<StorageIdError> for SerializationError {
    fn from(error: StorageIdError) -> Self {
        Self::StorageIdError { error }
    }
}

fn get_inline_blob<'a>(storage: &'a Storage, node: &Node) -> Option<Blob<'a>> {
    if let Some(Entry::Blob(blob_id)) = node.get_entry() {
        if blob_id.is_inline() {
            return storage.get_blob(blob_id).ok();
        }
    }
    None
}

#[bitfield(bits = 8)]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct KeyNodeDescriptor {
    kind: NodeKind,
    blob_inline_length: B3,
    key_inline_length: B4,
}

// Must fit in 1 byte
assert_eq_size!(KeyNodeDescriptor, u8);

pub fn serialize_entry(
    entry: &Entry,
    output: &mut Vec<u8>,
    storage: &Storage,
    stats: &mut SerializeStats,
) -> Result<(), SerializationError> {
    output.clear();

    match entry {
        Entry::Tree(tree) if !tree.is_inode() => {
            output.write_all(&[ID_TREE])?;
            let tree = storage.get_tree(*tree)?;

            let mut keys_length: usize = 0;
            let mut hash_ids_length: usize = 0;
            let mut highest_hash_id: u32 = 0;
            let mut nblobs_inlined: usize = 0;
            let mut blobs_length: usize = 0;

            for (key_id, node_id) in tree {
                let key = storage.get_str(*key_id)?;

                let node = storage.get_node(*node_id)?;

                let hash_id: u32 = node.hash_id().map(|h| h.as_u32()).unwrap_or(0);
                let kind = node.node_kind();

                let blob_inline = get_inline_blob(storage, &node);
                let blob_inline_length = blob_inline.as_ref().map(|b| b.len()).unwrap_or(0);

                match key.len() {
                    len if len != 0 && len < 16 => {
                        let byte: [u8; 1] = KeyNodeDescriptor::new()
                            .with_kind(kind)
                            .with_key_inline_length(len as u8)
                            .with_blob_inline_length(blob_inline_length as u8)
                            .into_bytes();
                        output.write_all(&byte[..])?;
                        output.write_all(key.as_bytes())?;
                        keys_length += len;
                    }
                    len => {
                        let byte: [u8; 1] = KeyNodeDescriptor::new()
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

                hash_ids_length += 4;
                highest_hash_id = highest_hash_id.max(hash_id);

                if let Some(blob_inline) = blob_inline {
                    nblobs_inlined += 1;
                    blobs_length += blob_inline.len();

                    output.write_all(&blob_inline)?;
                } else if hash_id & 0x7FFFFF == hash_id {
                    // The HashId fits in 23 bits
                    hash_ids_length += 3;
                    highest_hash_id = highest_hash_id.max(hash_id);

                    // Set `COMPACT_HASH_ID_BIT` so the deserializer knows the `HashId` is in 3 bytes
                    let hash_id: u32 = hash_id | COMPACT_HASH_ID_BIT;
                    let hash_id: [u8; 4] = hash_id.to_be_bytes();

                    output.write_all(&hash_id[1..])?;
                } else if hash_id & 0x7FFFFFFF == hash_id {
                    // HashId fits in 31 bits
                    hash_ids_length += 4;
                    highest_hash_id = highest_hash_id.max(hash_id);

                    output.write_all(&hash_id.to_be_bytes())?;
                } else {
                    // The HashId must not be 32 bits because we use the
                    // MSB to determine if the HashId is compact or not
                    return Err(SerializationError::HashIdTooBig);
                }
            }

            stats.add_tree(
                hash_ids_length,
                keys_length,
                highest_hash_id,
                nblobs_inlined,
                blobs_length,
            );
        }
        Entry::Tree(tree) => {
            assert!(tree.is_inode());

            use crate::working_tree::storage::Inode;

            let inode = storage.get_inode(tree.as_inode_id()).unwrap();

            let (depth, children, pointers) = match inode {
                Inode::Tree { depth, children, pointers } => (*depth, *children, pointers),
                _ => unreachable!("The root of an Inode is always a Inode::Tree"),
            };

            let children: u32 = children.try_into()?;

            output.write_all(&[ID_INODE])?;
            output.write_all(&depth.to_ne_bytes())?;
            output.write_all(&children.to_ne_bytes())?;

            for pointer in pointers {
                let hash_id = pointer.hash_id.get();
                let hash_id = hash_id.map(|h| h.as_u32()).unwrap_or(0);

                output.write_all(&pointer.index.to_ne_bytes())?;
                output.write_all(&hash_id.to_ne_bytes())?;
            }
        }
        Entry::Blob(blob_id) => {
            debug_assert!(!blob_id.is_inline());

            let blob = storage.get_blob(*blob_id)?;
            output.write_all(&[ID_BLOB])?;
            output.write_all(blob.as_ref())?;

            stats.add_blob(blob.len());
        }
        Entry::Commit(commit) => {
            output.write_all(&[ID_COMMIT])?;
            output.write_all(
                &commit
                    .parent_commit_hash
                    .map(|h| h.as_u32())
                    .unwrap_or(0)
                    .to_ne_bytes(),
            )?;
            output.write_all(&commit.root_hash.as_u32().to_ne_bytes())?;
            output.write_all(&commit.time.to_ne_bytes())?;
            let author_length: u32 = commit.author.len().try_into()?;
            output.write_all(&author_length.to_ne_bytes())?;
            output.write_all(commit.author.as_bytes())?;
            let message_length: u32 = commit.message.len().try_into()?;
            output.write_all(&message_length.to_ne_bytes())?;
            output.write_all(commit.message.as_bytes())?;
        }
    }

    stats.total_bytes += output.len();

    Ok(())
}

#[derive(Debug)]
pub enum DeserializationError {
    UnexpectedEOF,
    TryFromSliceError,
    Utf8Error,
    UnknownID,
    FromUtf8Error,
    MissingRootHash,
    MissingHash,
    NodeIdError,
    StorageIdError { error: StorageIdError },
}

impl From<TryFromSliceError> for DeserializationError {
    fn from(_: TryFromSliceError) -> Self {
        Self::TryFromSliceError
    }
}

impl From<Utf8Error> for DeserializationError {
    fn from(_: Utf8Error) -> Self {
        Self::Utf8Error
    }
}

impl From<FromUtf8Error> for DeserializationError {
    fn from(_: FromUtf8Error) -> Self {
        Self::FromUtf8Error
    }
}

impl From<NodeIdError> for DeserializationError {
    fn from(_: NodeIdError) -> Self {
        Self::NodeIdError
    }
}

impl From<StorageIdError> for DeserializationError {
    fn from(error: StorageIdError) -> Self {
        Self::StorageIdError { error }
    }
}

/// Extract values from `data` to store them in `storage`.
/// Return an `Entry`, which can be ids (refering to data inside `storage`) or a `Commit`
pub fn deserialize(data: &[u8], storage: &mut Storage) -> Result<Entry, DeserializationError> {
    let mut pos = 1;

    use DeserializationError as Error;
    use DeserializationError::*;

    match data.get(0).copied().ok_or(UnexpectedEOF)? {
        ID_TREE => {
            let data_length = data.len();

            let tree_id =
                storage.with_new_tree::<_, Result<_, Error>>(|storage, new_tree| {
                    while pos < data_length {
                        let descriptor = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
                        let descriptor = KeyNodeDescriptor::from_bytes([descriptor[0]; 1]);

                        pos += 1;

                        let key_id = match descriptor.key_inline_length() as usize {
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

                        let kind = descriptor.kind();
                        let blob_inline_length = descriptor.blob_inline_length() as usize;

                        let node = if blob_inline_length > 0 {
                            // The blob is inlined

                            let blob = data
                                .get(pos..pos + blob_inline_length)
                                .ok_or(UnexpectedEOF)?;
                            let blob_id = storage.add_blob_by_ref(blob)?;

                            pos += blob_inline_length;

                            Node::new_commited(kind, None, Some(Entry::Blob(blob_id)))
                        } else {
                            let byte_hash_id = data.get(pos).copied().ok_or(UnexpectedEOF)?;

                            if byte_hash_id & 1 << 7 != 0 {
                                // The HashId is in 3 bytes
                                let hash_id = data.get(pos..pos + 3).ok_or(UnexpectedEOF)?;

                                let hash_id: u32 = (hash_id[0] as u32) << 16
                                    | (hash_id[1] as u32) << 8
                                    | hash_id[2] as u32;

                                // Clear `COMPACT_HASH_ID_BIT`
                                let hash_id = hash_id & (COMPACT_HASH_ID_BIT - 1);
                                let hash_id = HashId::new(hash_id).ok_or(MissingHash)?;

                                pos += 3;

                                Node::new_commited(kind, Some(hash_id), None)
                            } else {
                                // The HashId is in 4 bytes
                                let hash_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
                                let hash_id = u32::from_be_bytes(hash_id.try_into()?);
                                let hash_id = HashId::new(hash_id).ok_or(MissingHash)?;

                                pos += 4;

                                Node::new_commited(kind, Some(hash_id), None)
                            }
                        };

                        let node_id = storage.add_node(node)?;

                        new_tree.push((key_id, node_id));
                    }

                    Ok(storage.add_tree(new_tree))
                })??;

            Ok(Entry::Tree(tree_id))
        }
        ID_BLOB => {
            let blob = data.get(pos..).ok_or(UnexpectedEOF)?;
            let blob_id = storage.add_blob_by_ref(blob)?;
            Ok(Entry::Blob(blob_id))
        }
        ID_COMMIT => {
            let parent_commit_hash = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
            let parent_commit_hash = u32::from_ne_bytes(parent_commit_hash.try_into()?);

            let root_hash = data.get(pos + 4..pos + 8).ok_or(UnexpectedEOF)?;
            let root_hash = u32::from_ne_bytes(root_hash.try_into()?);

            let time = data.get(pos + 8..pos + 16).ok_or(UnexpectedEOF)?;
            let time = u64::from_ne_bytes(time.try_into()?);

            let author_length = data.get(pos + 16..pos + 20).ok_or(UnexpectedEOF)?;
            let author_length = u32::from_ne_bytes(author_length.try_into()?) as usize;

            let author = data
                .get(pos + 20..pos + 20 + author_length)
                .ok_or(UnexpectedEOF)?;
            let author = author.to_vec();

            pos = pos + 20 + author_length;

            let message_length = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
            let message_length = u32::from_ne_bytes(message_length.try_into()?) as usize;

            let message = data
                .get(pos + 4..pos + 4 + message_length)
                .ok_or(UnexpectedEOF)?;
            let message = message.to_vec();

            Ok(Entry::Commit(Box::new(Commit {
                parent_commit_hash: HashId::new(parent_commit_hash),
                root_hash: HashId::new(root_hash).ok_or(MissingRootHash)?,
                time,
                author: String::from_utf8(author)?,
                message: String::from_utf8(message)?,
            })))
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

impl<'a> Iterator for HashIdIterator<'a> {
    type Item = HashId;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut pos = self.pos;

            if pos == 0 {
                let id = self.data.get(0).copied()?;
                if id == ID_BLOB {
                    // No HashId in Entry::Blob
                    return None;
                } else if id == ID_COMMIT {
                    // Entry::Commit.root_hash
                    let root_hash = self.data.get(9..9 + 4)?;
                    let root_hash = u32::from_ne_bytes(root_hash.try_into().ok()?);
                    self.pos = self.data.len();

                    return HashId::new(root_hash);
                }
                pos += 1;
            }

            let descriptor = self.data.get(pos..pos + 1)?;
            let descriptor = KeyNodeDescriptor::from_bytes([descriptor[0]; 1]);

            pos += 1;

            let offset = match descriptor.key_inline_length() as usize {
                len if len > 0 => len,
                _ => {
                    let key_length = self.data.get(pos..pos + 2)?;
                    let key_length = u16::from_ne_bytes(key_length.try_into().ok()?);
                    2 + key_length as usize
                }
            };

            pos += offset;

            let blob_inline_length = descriptor.blob_inline_length() as usize;

            if blob_inline_length > 0 {
                // No HashId when the blob is inlined, go to next entry
                self.pos = pos + blob_inline_length;
                continue;
            }

            let byte_hash_id = self.data.get(pos)?;

            let hash_id = if byte_hash_id & 1 << 7 != 0 {
                // HashId in 3 bytes
                let hash_id = self.data.get(pos..pos + 3)?;
                self.pos = pos + 3;

                let hash_id: u32 =
                    (hash_id[0] as u32) << 16 | (hash_id[1] as u32) << 8 | hash_id[2] as u32;
                hash_id & (COMPACT_HASH_ID_BIT - 1)
            } else {
                // HashId in 4 bytes
                let hash_id = self.data.get(pos..pos + 4)?;

                self.pos = pos + 4;
                u32::from_be_bytes(hash_id[..].try_into().ok()?)
            };

            return HashId::new(hash_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use tezos_timing::SerializeStats;

    use crate::{
        hash::hash_entry, kv_store::in_memory::InMemory, working_tree::storage::TreeStorageId,
    };

    use super::*;

    #[test]
    fn test_serialize() {
        let mut storage = Storage::new();
        let mut stats = SerializeStats::default();

        let tree_id = TreeStorageId::empty();
        let tree_id = storage
            .insert(
                tree_id,
                "a",
                Node::new_commited(NodeKind::Leaf, HashId::new(1), None),
            )
            .unwrap();
        let tree_id = storage
            .insert(
                tree_id,
                "bab",
                Node::new_commited(NodeKind::Leaf, HashId::new(2), None),
            )
            .unwrap();
        let tree_id = storage
            .insert(
                tree_id,
                "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Node::new_commited(NodeKind::Leaf, HashId::new(3), None),
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_entry(&Entry::Tree(tree_id), &mut data, &storage, &mut stats).unwrap();

        let entry = deserialize(&data, &mut storage).unwrap();

        if let Entry::Tree(entry) = entry {
            assert_eq!(
                storage.get_owned_tree(tree_id).unwrap(),
                storage.get_owned_tree(entry).unwrap()
            )
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[3, 1, 2]);

        // Not inlined value
        let blob_id = storage.add_blob_by_ref(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_entry(&Entry::Blob(blob_id), &mut data, &storage, &mut stats).unwrap();
        let entry = deserialize(&data, &mut storage).unwrap();
        if let Entry::Blob(entry) = entry {
            let blob = storage.get_blob(entry).unwrap();
            assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        } else {
            panic!();
        }
        let iter = iter_hash_ids(&data);
        assert_eq!(iter.count(), 0);

        let mut data = Vec::with_capacity(1024);

        let commit = Commit {
            parent_commit_hash: HashId::new(9876),
            root_hash: HashId::new(12345).unwrap(),
            time: 12345,
            author: "123".to_string(),
            message: "abc".to_string(),
        };

        serialize_entry(
            &Entry::Commit(Box::new(commit.clone())),
            &mut data,
            &storage,
            &mut stats,
        )
        .unwrap();
        let entry = deserialize(&data, &mut storage).unwrap();
        if let Entry::Commit(entry) = entry {
            assert_eq!(*entry, commit);
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_u32()).collect::<Vec<_>>(), &[12345]);
    }

    #[test]
    fn test_serialize_empty_blob() {
        let mut repo = InMemory::try_new().expect("failed to create context");
        let mut storage = Storage::new();
        let mut stats = SerializeStats::default();

        let blob_id = storage.add_blob_by_ref(&[]).unwrap();
        let blob = Entry::Blob(blob_id);
        let blob_hash_id = hash_entry(&blob, &mut repo, &storage).unwrap();

        assert!(blob_hash_id.is_some());

        let tree_id = TreeStorageId::empty();
        let tree_id = storage
            .insert(
                tree_id,
                "a",
                Node::new_commited(NodeKind::Leaf, blob_hash_id, None),
            )
            .unwrap();

        let mut data = Vec::with_capacity(1024);
        serialize_entry(&Entry::Tree(tree_id), &mut data, &storage, &mut stats).unwrap();

        let entry = deserialize(&data, &mut storage).unwrap();

        if let Entry::Tree(entry) = entry {
            assert_eq!(
                storage.get_owned_tree(tree_id).unwrap(),
                storage.get_owned_tree(entry).unwrap()
            )
        } else {
            panic!();
        }
    }
}
