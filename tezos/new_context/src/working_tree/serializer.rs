use std::{
    array::TryFromSliceError, cell::Cell, convert::TryInto, io::Write, num::TryFromIntError,
    str::Utf8Error, string::FromUtf8Error,
};

use modular_bitfield::prelude::*;

use crate::{kv_store::HashId, working_tree::{Commit, NodeEntryKind, NodeKind, string_interner::{STRING_INTERN_THRESHOLD, StringId}, tree_storage::BlobStorageId}};

use super::{Entry, Node, NodeBitfield, NodeEntry, tree_storage::{Blob, TreeStorage}};

const ID_TREE: u8 = 0;
const ID_BLOB: u8 = 1;
const ID_COMMIT: u8 = 2;

#[derive(Debug)]
pub enum SerializationError {
    IOError,
    TreeNotFound,
    TryFromIntError,
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

fn get_inline_blob(storage: &TreeStorage, node_entry: NodeEntry) -> Option<Blob> {
    if let Some(Entry::Blob(blob_id)) = node_entry.get_entry() {
        if let Some(blob) = storage.get_blob(blob_id) {
            if blob.len() < 8 {
                return Some(blob)
            }
        }
    }
    None
}

fn get_blob(storage: &TreeStorage, node_entry: NodeEntry) -> Option<Blob> {
    if let Some(Entry::Blob(blob_id)) = node_entry.get_entry() {
        return storage.get_blob(blob_id);
    }
    None
}

#[derive(BitfieldSpecifier)]
#[bits = 4]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
enum BlobLength {
    None,
    Inlined,
    OneByte,
    TwoBytes,
    FourBytes,
}

#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct NodeDescriptor {
    kind: NodeKind,
    blob_length: BlobLength,
    inline_length: B3,
}

#[derive(BitfieldSpecifier)]
#[bits = 2]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
enum KeyLength {
    Inlined,
    KeyId,
    TwoBytes,
}

#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct KeyDescriptor {
    length: KeyLength,
    kind: NodeKind,
    inline_length: B5
}

pub fn serialize_entry(
    entry: &Entry,
    output: &mut Vec<u8>,
    storage: &TreeStorage,
) -> Result<(usize, u32, usize, usize), SerializationError> {
    use SerializationError::*;

    output.clear();

    let mut hash_ids_len = 0;
    let mut keys_len = 0;
    let mut highest_hash_id = 0;
    let mut nchild = 0;

    match entry {
        Entry::Tree(tree) => {
            output.write(&[ID_TREE])?;
            let tree = storage.get_tree(*tree).ok_or(TreeNotFound)?;

            nchild = tree.len();

            for (key_id, node_id) in tree {
                let key = storage.get_str(*key_id);

                let node = storage.get_node(*node_id).unwrap();
                let node_bitfield = node.bitfield.get();

                let hash_id: u32 = node_bitfield.entry_hash_id();
                let kind = node_bitfield.node_kind();

                match key.len() {
                    len if len < 4 => {
                        let byte: [u8; 1] = KeyDescriptor::new()
                            .with_length(KeyLength::Inlined)
                            .with_kind(kind)
                            .with_inline_length(len as u8)
                            .into_bytes();
                        output.write(&byte[..])?;
                        output.write(key.as_bytes())?;
                        keys_len += len;
                    }
                    len if len < STRING_INTERN_THRESHOLD => {
                        let byte: [u8; 1] = KeyDescriptor::new()
                            .with_length(KeyLength::KeyId)
                            .with_kind(kind)
                            .with_inline_length(0)
                            .into_bytes();
                        output.write(&byte[..])?;

                        let key_id: u32 = key_id.as_u32();
                        output.write(&key_id.to_ne_bytes())?;
                        keys_len += 4;
                    }
                    _ => {
                        let byte: [u8; 1] = KeyDescriptor::new()
                            .with_length(KeyLength::TwoBytes)
                            .with_kind(kind)
                            .with_inline_length(0)
                            .into_bytes();
                        output.write(&byte[..])?;

                        let key_length: u16 = key.len().try_into()?;
                        output.write(&key_length.to_ne_bytes())?;
                        output.write(key.as_bytes())?;
                        keys_len += 2 + key.len();
                    }
                }

                hash_ids_len += 4;

                if hash_id > highest_hash_id {
                    highest_hash_id = hash_id;
                }

                // if let Some(blob) = get_blob(storage, node.entry.get()) {

                //     match blob.len() {
                //         len if len < 8 => {
                //             let byte: [u8; 1] = NodeDescriptor::new()
                //                 .with_kind(kind)
                //                 .with_blob_length(BlobLength::Inlined)
                //                 .with_inline_length(len as u8)
                //                 .into_bytes();

                //             output.write(&byte[..])?;
                //             output.write(&hash_id.to_ne_bytes())?;
                //             output.write(blob.as_ref())?;
                //         },
                //         len if len <= 0xFF => {
                //             let byte: [u8; 1] = NodeDescriptor::new()
                //                 .with_kind(kind)
                //                 .with_blob_length(BlobLength::OneByte)
                //                 .with_inline_length(0)
                //                 .into_bytes();

                //             let length: u8 = len.try_into().unwrap();

                //             output.write(&byte[..])?;
                //             output.write(&hash_id.to_ne_bytes())?;
                //             output.write(&length.to_ne_bytes())?;
                //             output.write(blob.as_ref())?;
                //         },
                //         len if len <= 0xFFFF => {
                //             let byte: [u8; 1] = NodeDescriptor::new()
                //                 .with_kind(kind)
                //                 .with_blob_length(BlobLength::TwoBytes)
                //                 .with_inline_length(0)
                //                 .into_bytes();

                //             let length: u16 = len.try_into().unwrap();

                //             output.write(&byte[..])?;
                //             output.write(&hash_id.to_ne_bytes())?;
                //             output.write(&length.to_ne_bytes())?;
                //             output.write(blob.as_ref())?;
                //         },
                //         len if len <= 0xFFFFFFFF => {
                //             let byte: [u8; 1] = NodeDescriptor::new()
                //                 .with_kind(kind)
                //                 .with_blob_length(BlobLength::FourBytes)
                //                 .with_inline_length(0)
                //                 .into_bytes();

                //             let length: u32 = len.try_into().unwrap();

                //             output.write(&byte[..])?;
                //             output.write(&hash_id.to_ne_bytes())?;
                //             output.write(&length.to_ne_bytes())?;
                //             output.write(blob.as_ref())?;
                //         },
                //         _ => {
                //             panic!()
                //         }
                //     }
                // } else {
                    // let byte: [u8; 1] = NodeDescriptor::new()
                    //     .with_kind(kind)
                    //     .with_blob_length(BlobLength::None)
                    //     .with_inline_length(0)
                    //     .into_bytes();
                    // output.write(&byte[..])?;

                    // let kind: u8 = node_bitfield.node_kind().into();

                    // output.write(&[kind])?;
                    // output.write(&hash_id.to_ne_bytes())?;
                // }

                if hash_id & 0x7FFFFF == hash_id {
                    let hash_id = hash_id.to_be_bytes();
                    let slice: [u8; 3] = [(1 << 7) | hash_id[1], hash_id[2], hash_id[3]];

                    output.write(&slice[..])?;
                } else {
                    output.write(&hash_id.to_be_bytes())?;
                }

                // if let Some(blob) = get_inline_blob(storage, node.entry.get()) {
                //     let kind: u8 = node_bitfield.node_kind().into();
                //     let hash_id: u32 = node_bitfield.entry_hash_id();

                //     let blob_len: u8 = blob.len().try_into().unwrap();
                //     let kind_len: u8 = (1 << 7) | (kind << 6) | blob_len;

                //     output.write(&[kind_len])?;
                //     output.write(&hash_id.to_ne_bytes())?;
                //     output.write(blob.as_ref())?;
                // } else {
                //     let kind: u8 = node_bitfield.node_kind().into();

                //     output.write(&[kind])?;
                //     output.write(&hash_id.to_ne_bytes())?;
                // }
            }
        }
        Entry::Blob(blob_id) => {
            // panic!()
            output.write(&[ID_BLOB])?;
            let blob = storage.get_blob(*blob_id).unwrap();
            // println!("SERIALIZE ID={:?} BLOB={:?}", blob_id, blob.as_ref());
            output.write(blob.as_ref())?;
        }
        Entry::Commit(commit) => {
            output.write(&[ID_COMMIT])?;
            output.write(
                &commit
                    .parent_commit_hash
                    .map(|h| h.as_usize())
                    .unwrap_or(0)
                    .to_ne_bytes(),
            )?;
            output.write(&commit.root_hash.as_usize().to_ne_bytes())?;
            output.write(&commit.time.to_ne_bytes())?;
            output.write(&commit.author.len().to_ne_bytes())?;
            output.write(commit.author.as_bytes())?;
            output.write(&commit.message.len().to_ne_bytes())?;
            output.write(commit.message.as_bytes())?;
        }
    }
    Ok((keys_len, highest_hash_id, nchild, hash_ids_len))
}

#[derive(Debug)]
pub enum DeserializationError {
    UnexpectedEOF,
    TryFromSliceError,
    Utf8Error,
    UnknownID,
    FromUtf8Error,
    MissingRootHash,
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

pub fn deserialize(
    data: &[u8],
    //strings: &mut StringInterner,
    tree_storage: &mut TreeStorage,
) -> Result<Entry, DeserializationError> {
    let mut pos = 1;

    use DeserializationError as Error;
    use DeserializationError::*;

    match data.get(0).copied().ok_or(UnexpectedEOF)? {
        ID_TREE => {
            let data_length = data.len();

            // todo!()
            let tree_id = tree_storage.add_tree_with_result::<_, Error>(|storage, trees| {
                while pos < data_length {
                    let key_descriptor = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
                    let key_descriptor = KeyDescriptor::from_bytes([key_descriptor[0]; 1]);

                    pos += 1;

                    let key_id = match key_descriptor.length() {
                        KeyLength::Inlined => {
                            let length = key_descriptor.inline_length() as usize;
                            let key_bytes = data.get(pos..pos + length).ok_or(UnexpectedEOF)?;
                            let key_str = std::str::from_utf8(key_bytes)?;
                            pos += length;
                            storage.get_string_id(key_str)
                        }
                        KeyLength::KeyId => {
                            let key_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
                            pos += 4;
                            let key_id = u32::from_ne_bytes(key_id.try_into()?);
                            StringId::new(key_id)
                        }
                        KeyLength::TwoBytes => {
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

                    let kind = key_descriptor.kind();
                    let hash_id = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;

                    let bitfield = if hash_id[0] & 1 << 7 != 0 {
                        let hash_id = data.get(pos..pos + 3).ok_or(UnexpectedEOF)?;

                        let hash_id: u32 = (hash_id[0] as u32) << 16 as u32 | (hash_id[1] as u32) << 8 | hash_id[2] as u32;
                        let hash_id = hash_id & 0x7FFFFF;

                        assert_ne!(hash_id, 0);

                        pos += 3;

                        NodeBitfield::new_with(kind, HashId::new_u32(hash_id).unwrap())
                            .with_commited(true)
                    } else {
                        let hash_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
                        let hash_id = u32::from_be_bytes(hash_id[..].try_into()?);
                        assert_ne!(hash_id, 0);

                        pos += 4;

                        NodeBitfield::new_with(kind, HashId::new_u32(hash_id).unwrap())
                            .with_commited(true)
                    };

                    // let hash_id = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;

                    // // let descriptor = NodeDescriptor::from_bytes([kind_hash_id[0]; 1]);
                    // // let kind = descriptor.kind();
                    // let hash_id = u32::from_ne_bytes(hash_id[..].try_into()?);
                    // assert_ne!(hash_id, 0);

                    // let bitfield = NodeBitfield::new_with(kind, HashId::new_u32(hash_id).unwrap())
                    //     .with_commited(true);

                    // pos += 4;

                // if hash_id & 0x7FFFFFFF == hash_id {
                //     let hash_id = hash_id.to_ne_bytes();
                //     let slice: [u8; 3] = [(1 << 7) | hash_id[1], hash_id[2], hash_id[3]];
                //     output.write(&slice[..])?;
                // } else {
                //     output.write(&hash_id.to_ne_bytes())?;
                // }

                    // let entry = match kind {
                    //     NodeKind::Leaf => {
                    //         match descriptor.blob_length() {
                    //             BlobLength::None => {
                    //                 NodeEntry::new_none()
                    //             }
                    //             BlobLength::Inlined => {
                    //                 let blob_length = descriptor.inline_length() as usize;
                    //                 let blob = data.get(pos..pos + blob_length).ok_or(UnexpectedEOF)?;
                    //                 pos = pos + blob_length;
                    //                 let blob_id = BlobStorageId::new_inline(blob);
                    //                 NodeEntry::new()
                    //                     .with_entry_kind(NodeEntryKind::Blob)
                    //                     .with_entry_id(blob_id.into())
                    //             },
                    //             BlobLength::OneByte => {
                    //                 let blob_length = data.get(pos..pos + 1).ok_or(UnexpectedEOF)?;
                    //                 let blob_length = u8::from_ne_bytes(blob_length.try_into()?);
                    //                 let blob_length = blob_length as usize;
                    //                 let blob = data.get(pos + 1..pos + 1 + blob_length).ok_or(UnexpectedEOF)?;
                    //                 pos = pos + 1 + blob_length;
                    //                 let blob_id = storage.add_blob_by_ref(blob);
                    //                 NodeEntry::new()
                    //                     .with_entry_kind(NodeEntryKind::Blob)
                    //                     .with_entry_id(blob_id.into())
                    //             },
                    //             BlobLength::TwoBytes => {
                    //                 let blob_length = data.get(pos..pos + 2).ok_or(UnexpectedEOF)?;
                    //                 let blob_length = u16::from_ne_bytes(blob_length.try_into()?);
                    //                 let blob_length = blob_length as usize;
                    //                 let blob = data.get(pos + 2..pos + 2 + blob_length).ok_or(UnexpectedEOF)?;
                    //                 pos = pos + 2 + blob_length;
                    //                 let blob_id = storage.add_blob_by_ref(blob);
                    //                 NodeEntry::new()
                    //                     .with_entry_kind(NodeEntryKind::Blob)
                    //                     .with_entry_id(blob_id.into())
                    //             },
                    //             BlobLength::FourBytes => {
                    //                 let blob_length = data.get(pos..pos + 4).ok_or(UnexpectedEOF)?;
                    //                 let blob_length = u32::from_ne_bytes(blob_length.try_into()?);
                    //                 let blob_length = blob_length as usize;
                    //                 let blob = data.get(pos + 4..pos + 4 + blob_length).ok_or(UnexpectedEOF)?;
                    //                 pos = pos + 4 + blob_length;
                    //                 let blob_id = storage.add_blob_by_ref(blob);
                    //                 NodeEntry::new()
                    //                     .with_entry_kind(NodeEntryKind::Blob)
                    //                     .with_entry_id(blob_id.into())
                    //             },
                    //         }
                    //     },
                    //     NodeKind::NonLeaf => {
                    //         NodeEntry::new_none()
                    //     }
                    // };

                    let entry = NodeEntry::new_none();

                    trees.push((
                        key_id,
                        Node {
                            bitfield: Cell::new(bitfield),
                            entry: Cell::new(entry),
                        },
                    ));

                    // let kind = kind_hash_id[0];
                    // let hash_id = u32::from_ne_bytes(kind_hash_id[1..].try_into()?);
                    // assert_ne!(hash_id, 0);

                    // if kind >> 7 != 0 {
                    //     let blob_len = (kind & 0b11111) as usize;
                    //     let kind = NodeKind::from((kind >> 6) & 1);

                    //     let blob = data.get(pos + 5..pos + 5 + blob_len).ok_or(UnexpectedEOF)?;
                    //     let blob_id = BlobStorageId::new_inline(blob);

                    //     let bitfield = NodeBitfield::new_with(kind, HashId::new_u32(hash_id).unwrap())
                    //         .with_commited(true);

                    //     pos += 5 + blob_len;

                    //     trees.push((
                    //         key,
                    //         Node {
                    //             bitfield: Cell::new(bitfield),
                    //             entry: Cell::new(
                    //                 NodeEntry::new()
                    //                     .with_entry_kind(NodeEntryKind::Blob)
                    //                     .with_entry_id(blob_id.into())
                    //             ),
                    //         },
                    //     ));

                    // } else {
                    //     let kind = NodeKind::from(kind_hash_id[0]);

                    //     let bitfield = NodeBitfield::new_with(kind, HashId::new_u32(hash_id).unwrap())
                    //         .with_commited(true);

                    //     pos += 5;

                    //     trees.push((
                    //         key,
                    //         Node {
                    //             bitfield: Cell::new(bitfield),
                    //             entry: Default::default(),
                    //         },
                    //     ));
                    // }
                }
                Ok(())
            })?;

            Ok(Entry::Tree(tree_id))
        }
        ID_BLOB => {
            // panic!()
            let blob = data.get(pos..).ok_or(UnexpectedEOF)?;
            let blob_id = tree_storage.add_blob_by_ref(blob);
            // println!("DESERIALIZE ID={:?} BLOB={:?}", blob_id, tree_storage.get_blob(blob_id));
            Ok(Entry::Blob(blob_id))
        },
        ID_COMMIT => {
            let parent_commit_hash = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
            let parent_commit_hash = usize::from_ne_bytes(parent_commit_hash.try_into()?);

            let root_hash = data.get(pos + 8..pos + 16).ok_or(UnexpectedEOF)?;
            let root_hash = usize::from_ne_bytes(root_hash.try_into()?);

            let time = data.get(pos + 16..pos + 24).ok_or(UnexpectedEOF)?;
            let time = u64::from_ne_bytes(time.try_into()?);

            let author_length = data.get(pos + 24..pos + 32).ok_or(UnexpectedEOF)?;
            let author_length = usize::from_ne_bytes(author_length.try_into()?);

            let author = data
                .get(pos + 32..pos + 32 + author_length)
                .ok_or(UnexpectedEOF)?;
            let author = author.to_vec();

            pos = pos + 32 + author_length;

            let message_length = data.get(pos..pos + 8).ok_or(UnexpectedEOF)?;
            let message_length = usize::from_ne_bytes(message_length.try_into()?);

            let message = data
                .get(pos + 8..pos + 8 + message_length)
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
pub fn iter_hash_ids<'a>(data: &'a [u8]) -> HashIdIterator<'a> {
    HashIdIterator { data, pos: 0 }
}

pub struct HashIdIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for HashIdIterator<'a> {
    type Item = HashId;

    fn next(&mut self) -> Option<Self::Item> {
        let mut pos = self.pos;

        if pos == 0 {
            let id = self.data.get(0).copied()?;
            if id == ID_BLOB {
                // No HashId in Entry::Blob
                return None;
            } else if id == ID_COMMIT {
                // Entry::Commit.root_hash
                let root_hash = self.data.get(9..9 + 8)?;
                let root_hash = usize::from_ne_bytes(root_hash.try_into().ok()?);
                self.pos = self.data.len();

                return HashId::new(root_hash);
            }
            pos += 1;
        }

        let key_descriptor = self.data.get(pos..pos + 1)?;
        let key_descriptor = KeyDescriptor::from_bytes([key_descriptor[0]; 1]);

        pos += 1;

        let offset = match key_descriptor.length() {
            KeyLength::Inlined => {
                key_descriptor.inline_length() as usize
            },
            KeyLength::KeyId => 4,
            KeyLength::TwoBytes => {
                let key_length = self.data.get(pos..pos + 2)?;
                let key_length = u16::from_ne_bytes(key_length.try_into().ok()?);
                2 + (key_length as usize)
            }
        };

        pos += offset;

        let hash_id = self.data.get(pos..pos + 1)?;

        let hash_id = if hash_id[0] & 1 << 7 != 0 {
            let hash_id = self.data.get(pos..pos + 3)?;

            self.pos = pos + 3;
            let hash_id: u32 = (hash_id[0] as u32) << 16 as u32 | (hash_id[1] as u32) << 8 | hash_id[2] as u32;
            hash_id & 0x7FFFFF
        } else {
            let hash_id = self.data.get(pos..pos + 4)?;

            self.pos = pos + 4;
            u32::from_be_bytes(hash_id[..].try_into().ok()?)
        };

        // let descriptor = NodeDescriptor::from_bytes([kind_hash_id[0]; 1]);
        // let hash_id = u32::from_ne_bytes(hash_id.try_into().ok()?);
        // let kind = descriptor.kind();


        // let offset = match kind {
        //     NodeKind::Leaf => {
        //         match descriptor.blob_length() {
        //             BlobLength::None => 0,
        //             BlobLength::Inlined => descriptor.inline_length() as usize,
        //             BlobLength::OneByte => {
        //                 let blob_length = self.data.get(pos..pos + 1)?;
        //                 let blob_length = u8::from_ne_bytes(blob_length.try_into().ok()?);
        //                 1 + (blob_length as usize)
        //             },
        //             BlobLength::TwoBytes => {
        //                 let blob_length = self.data.get(pos..pos + 2)?;
        //                 let blob_length = u16::from_ne_bytes(blob_length.try_into().ok()?);
        //                 2 + (blob_length as usize)
        //             },
        //             BlobLength::FourBytes => {
        //                 let blob_length = self.data.get(pos..pos + 4)?;
        //                 let blob_length = u32::from_ne_bytes(blob_length.try_into().ok()?);
        //                 4 + (blob_length as usize)
        //             },
        //         }
        //     },
        //     NodeKind::NonLeaf => 0
        // };

        // self.pos = pos + offset;

        HashId::new(hash_id as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use crate::working_tree::{NodeEntry, string_interner::StringInterner};

    use super::*;

    #[test]
    fn test_serialize() {
        let mut tree_storage = TreeStorage::new();

        let tree_id = tree_storage.new_tree();
        let tree_id = tree_storage.insert(
            tree_id,
            "a",
            Node {
                bitfield: Cell::new(
                    NodeBitfield::new()
                        .with_entry_hash_id(1)
                        .with_commited(true),
                ),
                entry: Cell::new(NodeEntry::new_none()),
            },
        );
        let tree_id = tree_storage.insert(
            tree_id,
            "b",
            Node {
                bitfield: Cell::new(
                    NodeBitfield::new()
                        .with_entry_hash_id(2)
                        .with_commited(true),
                ),
                entry: Cell::new(NodeEntry::new_none()),
            },
        );
        let tree_id = tree_storage.insert(
            tree_id,
            "0",
            Node {
                bitfield: Cell::new(
                    NodeBitfield::new()
                        .with_entry_hash_id(3)
                        .with_commited(true),
                ),
                entry: Cell::new(NodeEntry::new_none()),
            },
        );

        let mut data = Vec::with_capacity(1024);
        serialize_entry(&Entry::Tree(tree_id), &mut data, &tree_storage).unwrap();

        let entry = deserialize(&data, &mut tree_storage).unwrap();

        if let Entry::Tree(entry) = entry {
            assert_eq!(
                tree_storage.get_own_tree(tree_id).unwrap(),
                tree_storage.get_own_tree(entry).unwrap()
            )
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_usize()).collect::<Vec<_>>(), &[3, 1, 2]);

        // let blob_id = tree_storage.add_blob_by_ref(&[1, 2, 3, 4, 5]);

        // let mut data = Vec::with_capacity(1024);
        // serialize_entry(&Entry::Blob(blob_id), &mut data, &tree_storage).unwrap();
        // let entry = deserialize(&data, &mut tree_storage).unwrap();
        // if let Entry::Blob(entry) = entry {
        //     let blob = tree_storage.get_blob(entry).unwrap();
        //     assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5]);
        // } else {
        //     panic!();
        // }
        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.count(), 0);

        // // Not inlined value
        // let blob_id = tree_storage.add_blob_by_ref(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // let mut data = Vec::with_capacity(1024);
        // serialize_entry(&Entry::Blob(blob_id), &mut data, &tree_storage).unwrap();
        // let entry = deserialize(&data, &mut tree_storage).unwrap();
        // if let Entry::Blob(entry) = entry {
        //     let blob = tree_storage.get_blob(entry).unwrap();
        //     assert_eq!(blob.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        // } else {
        //     panic!();
        // }
        // let iter = iter_hash_ids(&data);
        // assert_eq!(iter.count(), 0);

        let mut data = Vec::with_capacity(1024);

        let commit = Commit {
            parent_commit_hash: HashId::new(9876),
            root_hash: HashId::new(12345).unwrap(),
            time: 12345,
            author: "seb".to_string(),
            message: "abc".to_string(),
        };

        serialize_entry(
            &Entry::Commit(Box::new(commit.clone())),
            &mut data,
            &tree_storage,
        )
        .unwrap();
        let entry = deserialize(&data, &mut tree_storage).unwrap();
        if let Entry::Commit(entry) = entry {
            assert_eq!(*entry, commit);
        } else {
            panic!();
        }

        let iter = iter_hash_ids(&data);
        assert_eq!(iter.map(|h| h.as_usize()).collect::<Vec<_>>(), &[12345]);
    }
}
