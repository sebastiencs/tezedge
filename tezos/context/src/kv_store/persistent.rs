use std::{
    borrow::Cow,
    collections::hash_map::DefaultHasher,
    convert::{TryFrom, TryInto},
    hash::Hasher,
    io::Write,
};

#[cfg(test)]
use std::sync::Arc;

use crypto::hash::ContextHash;
use tezos_timing::{RepositoryMemoryUsage, SerializeStats};

use crate::{
    gc::{GarbageCollectionError, GarbageCollector},
    initializer::IndexInitializationError,
    persistent::{
        get_commit_hash, get_persistent_base_path, DBError, File, FileType, Flushable,
        KeyValueStoreBackend, Persistable,
    },
    serialize::{
        persistent::{self, deserialize_hash_id, read_object_length, AbsoluteOffset},
        DeserializationError, ObjectHeader,
    },
    working_tree::{
        shape::{DirectoryShapeId, DirectoryShapes, ShapeStrings},
        storage::{DirEntryId, Storage},
        string_interner::{StringId, StringInterner},
        working_tree::{PostCommitData, WorkingTree},
        Object, ObjectReference,
    },
    Map, ObjectHash,
};

use super::{HashId, VacantObjectHash};

pub struct Persistent {
    data_file: File,
    shape_file: File,
    shape_index_file: File,
    commit_index_file: File,
    strings_file: File,
    big_strings_file: File,
    big_strings_offsets_file: File,

    hashes: Hashes,
    // hashes_file: File,

    // hashes_file_index: usize,
    shapes: DirectoryShapes,
    string_interner: StringInterner,

    // hashes: Hashes,
    pub context_hashes: Map<u64, ObjectReference>,
}

impl GarbageCollector for Persistent {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        // self.new_cycle_started();
        Ok(())
    }

    fn block_applied(
        &mut self,
        _referenced_older_objects: Vec<HashId>,
    ) -> Result<(), GarbageCollectionError> {
        // self.block_applied(referenced_older_objects);
        Ok(())
    }
}

impl Flushable for Persistent {
    fn flush(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Persistable for Persistent {
    fn is_persistent(&self) -> bool {
        false
    }
}

struct Hashes {
    list: Vec<ObjectHash>,
    list_first_index: usize,
    hashes_file: File,

    bytes: Vec<u8>,
}

impl Hashes {
    fn try_new(hashes_file: File) -> Self {
        let list_first_index =
            (hashes_file.offset().as_u64() as usize) / crate::hash::OBJECT_HASH_LEN;

        Self {
            list: Vec::with_capacity(1000),
            hashes_file,
            list_first_index,
            bytes: Vec::with_capacity(1000),
        }
    }

    fn get_hash(&self, hash_id: HashId) -> Result<Cow<ObjectHash>, DBError> {
        let hash_id_index: usize = hash_id.try_into()?;

        let is_in_file = hash_id_index < self.list_first_index;

        if is_in_file {
            let offset = hash_id_index * std::mem::size_of::<ObjectHash>();

            let mut hash: ObjectHash = Default::default();

            self.hashes_file
                .read_exact_at(&mut hash, (offset as u64).into())?;

            Ok(Cow::Owned(hash))
        } else {
            let index = hash_id_index - self.list_first_index;

            match self.list.get(index) {
                Some(hash) => Ok(Cow::Borrowed(hash)),
                None => Err(DBError::HashNotFound {
                    object_ref: hash_id.into(),
                }),
            }
        }
    }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        let list_length = self.list.len();
        let index = self.list_first_index + list_length;
        self.list.push(Default::default());

        Ok(VacantObjectHash {
            entry: Some(&mut self.list[list_length]),
            hash_id: HashId::try_from(index)?,
        })
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        let hash_id: usize = hash_id.try_into()?;

        Ok(hash_id < self.list_first_index + self.list.len())
    }

    fn commit(&mut self) -> Result<(), std::io::Error> {
        if self.list.is_empty() {
            return Ok(());
        }

        self.bytes.clear();
        for h in &self.list {
            self.bytes.extend_from_slice(h);
        }
        self.list_first_index += self.list.len();

        self.hashes_file.append(&self.bytes)?;
        self.list.clear();

        Ok(())
    }
}

impl Persistent {
    pub fn try_new() -> Result<Persistent, IndexInitializationError> {
        let base_path = get_persistent_base_path();

        let data_file = File::try_new(&base_path, FileType::Data)?;
        let mut shape_file = File::try_new(&base_path, FileType::ShapeDirectories)?;
        let mut shape_index_file = File::try_new(&base_path, FileType::ShapeDirectoriesIndex)?;
        let commit_index_file = File::try_new(&base_path, FileType::CommitIndex)?;
        let mut strings_file = File::try_new(&base_path, FileType::Strings)?;
        let mut big_strings_file = File::try_new(&base_path, FileType::BigStrings)?;
        let mut big_strings_offsets_file = File::try_new(&base_path, FileType::BigStringsOffsets)?;

        let hashes_file = File::try_new(&base_path, FileType::Hashes)?;

        let shapes = DirectoryShapes::deserialize(&mut shape_file, &mut shape_index_file)?;
        let string_interner = StringInterner::deserialize(
            &mut strings_file,
            &mut big_strings_file,
            &mut big_strings_offsets_file,
        )?;
        let (hashes, context_hashes) = deserialize_hashes(hashes_file, &commit_index_file)?;

        Ok(Self {
            data_file,
            shape_file,
            shape_index_file,
            commit_index_file,
            strings_file,
            hashes,
            big_strings_file,
            big_strings_offsets_file,
            shapes,
            string_interner,
            context_hashes,
        })
    }

    pub fn get_object_bytes<'a>(
        &self,
        object_ref: ObjectReference,
        buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], DBError> {
        let offset = object_ref.offset();

        if buffer.len() < 4096 {
            buffer.resize(4096, 0);
        }

        let buffer_length = buffer.len();

        // We attempt to read 4096 bytes, if it's not enough we will read more later
        let buffer_slice = self.data_file.try_read(&mut buffer[..4096], offset)?;

        let object_header: ObjectHeader = ObjectHeader::from_bytes([buffer_slice[0]]);
        let (_, object_length) = read_object_length(buffer_slice, &object_header)?;

        if buffer_slice.len() < object_length {
            if buffer_length < object_length {
                buffer.resize(object_length, 0);
            }

            // Read the rest of the object
            self.data_file
                .read_exact_at(&mut buffer[4096..object_length], offset.add(4096))?;
        }

        Ok(&buffer[..object_length])
    }

    fn get_hash_id_from_offset<'a>(&self, object_ref: ObjectReference) -> Result<HashId, DBError> {
        let offset = object_ref.offset();

        let mut buffer: [u8; 10] = Default::default();

        self.data_file.try_read(&mut buffer, offset)?;

        let object_header: ObjectHeader = ObjectHeader::from_bytes([buffer[0]]);
        let (header_nbytes, _) = read_object_length(&buffer, &object_header)?;

        let hash_id = deserialize_hash_id(&buffer[header_nbytes..])?
            .0
            .ok_or(DeserializationError::MissingHash)?;

        Ok(hash_id)
    }

    fn commit_to_disk(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        self.data_file.append(data)?;

        let strings = self.string_interner.serialize();

        self.strings_file.append(&strings.strings)?;
        self.big_strings_file.append(&strings.big_strings)?;

        let shapes = self.shapes.serialize();
        self.shape_file.append(&shapes.shapes)?;
        self.shape_index_file.append(&shapes.index)?;

        self.hashes.commit()?;

        self.data_file.sync()?;
        self.strings_file.sync()?;
        self.big_strings_file.sync()?;
        self.big_strings_offsets_file.sync()?;
        self.hashes.hashes_file.sync()?;
        self.commit_index_file.sync()?;

        Ok(())
    }
}

fn deserialize_hashes(
    hashes_file: File,
    commit_index_file: &File,
) -> Result<(Hashes, Map<u64, ObjectReference>), DeserializationError> {
    let hashes = Hashes::try_new(hashes_file);
    let mut context_hashes: Map<u64, ObjectReference> = Default::default();

    let mut offset = 0u64;
    let end = commit_index_file.offset().as_u64();

    let mut hash_id_bytes = [0u8; 4];
    let mut hash_offset_bytes = [0u8; 8];
    let mut commit_hash: ObjectHash = Default::default();

    while offset < end {
        // commit index file is a sequence of entries that look like:
        // [hash_id u32 ne bytes | offset u64 ne bytes | hash <HASH_LEN> bytes]
        commit_index_file.read_exact_at(&mut hash_id_bytes, offset.into())?;
        offset += hash_id_bytes.len() as u64;
        let hash_id = u32::from_le_bytes(hash_id_bytes);

        commit_index_file.read_exact_at(&mut hash_offset_bytes, offset.into())?;
        offset += hash_offset_bytes.len() as u64;
        let hash_offset = u64::from_le_bytes(hash_offset_bytes);

        commit_index_file.read_exact_at(&mut commit_hash, offset.into())?;
        offset += commit_hash.len() as u64;

        let object_reference = ObjectReference::new(HashId::new(hash_id), Some(hash_offset.into()));

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash);
        let hashed = hasher.finish();

        context_hashes.insert(hashed, object_reference);
    }

    Ok((hashes, context_hashes))
}

fn serialize_context_hash(
    hash_id: HashId,
    offset: AbsoluteOffset,
    hash: &[u8],
) -> Result<Vec<u8>, DBError> {
    let mut output = Vec::<u8>::with_capacity(100);

    let offset: u64 = offset.as_u64();
    let hash_id: u32 = hash_id.as_u32();

    output.write_all(&hash_id.to_le_bytes())?;
    output.write_all(&offset.to_le_bytes())?;
    output.write_all(hash)?;

    Ok(output)
}

impl KeyValueStoreBackend for Persistent {
    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.hashes.contains(hash_id)
    }

    fn put_context_hash(&mut self, object_ref: ObjectReference) -> Result<(), DBError> {
        let commit_hash = self.get_hash(object_ref)?;

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash[..]);
        let hashed = hasher.finish();

        let output = serialize_context_hash(
            object_ref.hash_id(),
            object_ref.offset(),
            commit_hash.as_ref(),
        )?;
        self.commit_index_file.append(&output)?;

        self.context_hashes.insert(hashed, object_ref);

        Ok(())
    }

    fn get_context_hash(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<ObjectReference>, DBError> {
        let mut hasher = DefaultHasher::new();
        hasher.write(context_hash.as_ref());
        let hashed = hasher.finish();

        Ok(self.context_hashes.get(&hashed).cloned())
    }

    fn get_hash(&self, object_ref: ObjectReference) -> Result<Cow<ObjectHash>, DBError> {
        let hash_id = match object_ref.hash_id_opt() {
            Some(hash_id) => hash_id,
            None => self.get_hash_id(object_ref)?,
        };

        // let hash_id = self.get_hash_id(object_ref)?;
        self.hashes.get_hash(hash_id)
    }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        self.hashes.get_vacant_object_hash()
    }

    fn memory_usage(&self) -> RepositoryMemoryUsage {
        RepositoryMemoryUsage::default()
    }

    fn get_shape(&self, shape_id: DirectoryShapeId) -> Result<ShapeStrings, DBError> {
        self.shapes
            .get_shape(shape_id)
            .map(ShapeStrings::SliceIds)
            .map_err(Into::into)
    }

    fn make_shape(
        &mut self,
        dir: &[(StringId, DirEntryId)],
        storage: &Storage,
    ) -> Result<Option<DirectoryShapeId>, DBError> {
        self.shapes.make_shape(dir, storage).map_err(Into::into)
    }

    fn get_str(&self, string_id: StringId) -> Option<&str> {
        self.string_interner.get(string_id)
    }

    fn synchronize_strings_from(&mut self, string_interner: &StringInterner) {
        self.string_interner.extend_from(string_interner);
    }

    fn synchronize_strings_into(&self, string_interner: &mut StringInterner) {
        string_interner.clone_after_reload(&self.string_interner);
    }

    fn get_object(
        &self,
        object_ref: ObjectReference,
        storage: &mut Storage,
        strings: &mut StringInterner,
    ) -> Result<Object, DBError> {
        self.get_object_bytes(object_ref, &mut storage.data)?;

        let object_bytes = std::mem::take(&mut storage.data);
        let result = persistent::deserialize_object(
            &object_bytes,
            object_ref.offset(),
            storage,
            strings,
            self,
        );
        storage.data = object_bytes;

        result.map_err(Into::into)
    }

    fn get_object_bytes<'a>(
        &self,
        object_ref: ObjectReference,
        buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], DBError> {
        self.get_object_bytes(object_ref, buffer)
            .map_err(Into::into)
    }

    fn commit(
        &mut self,
        working_tree: &WorkingTree,
        parent_commit_ref: Option<ObjectReference>,
        author: String,
        message: String,
        date: u64,
    ) -> Result<(ContextHash, Box<SerializeStats>), DBError> {
        let offset = self.data_file.offset();

        let PostCommitData {
            commit_ref,
            serialize_stats,
            output,
            ..
        } = working_tree
            .prepare_commit(
                date,
                author,
                message,
                parent_commit_ref,
                self,
                Some(persistent::serialize_object),
                Some(offset),
            )
            .map_err(Box::new)?;

        let commit_hash = get_commit_hash(commit_ref, self).map_err(Box::new)?;

        self.commit_to_disk(&output)
            .map_err(|err| DBError::CommitToDiskError { err })?;

        self.put_context_hash(commit_ref)?;

        Ok((commit_hash, serialize_stats))
    }

    fn get_hash_id(&self, object_ref: ObjectReference) -> Result<HashId, DBError> {
        match object_ref.hash_id_opt() {
            Some(hash_id) => Ok(hash_id),
            None => self.get_hash_id_from_offset(object_ref),
        }
    }

    #[cfg(test)]
    fn synchronize_data(
        &mut self,
        _batch: &[(HashId, Arc<[u8]>)],
        output: &[u8],
    ) -> Result<Option<AbsoluteOffset>, DBError> {
        self.commit_to_disk(output)?;
        Ok(Some(self.data_file.offset()))
    }
}
