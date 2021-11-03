use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{hash_map::DefaultHasher, VecDeque},
    convert::{TryFrom, TryInto},
    hash::Hasher,
    io::Write,
    sync::Arc,
};

use crypto::hash::ContextHash;
use tezos_timing::{RepositoryMemoryUsage, SerializeStats};

use crate::{
    gc::{worker::PRESERVE_CYCLE_COUNT, GarbageCollectionError, GarbageCollector},
    persistent::{
        get_commit_hash, get_persistent_base_path, DBError, File, FileOffset, FileType, Flushable,
        KeyValueStoreBackend, Persistable,
    },
    serialize::{
        persistent::{self, deserialize_hash_id, read_object_length, AbsoluteOffset},
        ObjectHeader, ObjectLength,
    },
    working_tree::{
        // serializer::{
        //     deserialize_object, read_object_length, serialize_object, AbsoluteOffset, ObjectHeader,
        //     ObjectLength,
        // },
        shape::{DirectoryShapeId, DirectoryShapes, ShapeStrings},
        storage::{DirEntryId, Storage},
        string_interner::{StringId, StringInterner},
        working_tree::{MerkleError, PostCommitData, WorkingTree},
        Object,
        ObjectReference,
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
    context_hashes_cycles: VecDeque<Vec<u64>>,
}

impl GarbageCollector for Persistent {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        // self.new_cycle_started();
        Ok(())
    }

    fn block_applied(
        &mut self,
        referenced_older_objects: Vec<HashId>,
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
    // hashes_file_index: usize,
}

impl Hashes {
    fn try_new(mut hashes_file: File) -> Self {
        let list = Self::deserialize_hashes(&mut hashes_file);
        let list_first_index = list.len();

        Self {
            list,
            hashes_file,
            // hashes_file_index: 0,
            list_first_index,
            bytes: Vec::with_capacity(1000),
        }
    }

    fn deserialize_hashes(hashes_file: &mut File) -> Vec<ObjectHash> {
        let mut list = Vec::with_capacity(1000);

        let mut offset = 0u64;
        let end = hashes_file.offset().as_u64();

        while offset < end {
            // TODO: it is probably better to preallocate list with capacity all at once (plus extra),
            // then write directly on the elements with `read_exact_at`.
            let mut hash_bytes: ObjectHash = Default::default();
            hashes_file.read_exact_at(&mut hash_bytes, offset.into());
            offset += hash_bytes.len() as u64;

            list.push(hash_bytes)
        }

        list
    }

    fn get_hash(&self, hash_id: HashId) -> Result<Option<Cow<ObjectHash>>, DBError> {
        let hash_id_index: usize = hash_id.try_into().unwrap();

        let is_in_file = hash_id_index < self.list_first_index;

        if is_in_file {
            let offset = hash_id_index * std::mem::size_of::<ObjectHash>();

            let mut hash: ObjectHash = Default::default();

            self.hashes_file
                .read_exact_at(&mut hash, (offset as u64).into());

            Ok(Some(Cow::Owned(hash)))
        } else {
            let index = hash_id_index - self.list_first_index;

            match self.list.get(index) {
                Some(hash) => Ok(Some(Cow::Borrowed(hash))),
                None => Ok(None),
            }
        }
    }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        let list_length = self.list.len();
        let index = self.list_first_index + list_length;
        self.list.push(Default::default());

        Ok(VacantObjectHash {
            entry: Some(&mut self.list[list_length]),
            // entry: Some(&mut self.hashes_file),
            hash_id: HashId::try_from(index).unwrap(),
            // data: Default::default(),
        })
    }

    fn contains(&self, hash_id: HashId) -> bool {
        let hash_id: usize = hash_id.try_into().unwrap();

        hash_id < self.list_first_index + self.list.len()
    }

    fn commit(&mut self) {
        if self.list.is_empty() {
            return;
        }

        self.bytes.clear();
        for h in &self.list {
            self.bytes.extend_from_slice(h);
        }
        self.list_first_index += self.list.len();

        self.hashes_file.append(&self.bytes);
        self.list.clear();
    }
}

impl Persistent {
    pub fn try_new() -> Result<Persistent, std::io::Error> {
        let base_path = get_persistent_base_path();

        let data_file = File::new(&base_path, FileType::Data);
        let mut shape_file = File::new(&base_path, FileType::ShapeDirectories);
        let mut shape_index_file = File::new(&base_path, FileType::ShapeDirectoriesIndex);
        let mut commit_index_file = File::new(&base_path, FileType::CommitIndex);
        let mut strings_file = File::new(&base_path, FileType::Strings);
        let mut big_strings_file = File::new(&base_path, FileType::BigStrings);
        let mut big_strings_offsets_file = File::new(&base_path, FileType::BigStringsOffsets);

        /*
        TODO:

        fill Persistent::{shape,string_interner,context_hashes, hashes}
        Persistent::hashes::list_first_index need to be set to number of hashes in the file hashes.db
        */

        //let hashes = Hashes::try_new(&base_path);
        let hashes_file = File::new(&base_path, FileType::Hashes);

        //let mut context_hashes_cycles = VecDeque::with_capacity(PRESERVE_CYCLE_COUNT);
        //for _ in 0..PRESERVE_CYCLE_COUNT {
        //    context_hashes_cycles.push_back(Default::default())
        //}

        let shapes = DirectoryShapes::deserialize(&mut shape_file, &mut shape_index_file);
        let string_interner = StringInterner::deserialize(
            &mut strings_file,
            &mut big_strings_file,
            &mut big_strings_offsets_file,
        );
        let (hashes, context_hashes, context_hashes_cycles) =
            deserialize_hashes(hashes_file, &mut commit_index_file);

        Ok(Self {
            data_file,
            shape_file,
            shape_index_file,
            commit_index_file,
            strings_file,
            hashes,
            big_strings_file,
            big_strings_offsets_file,
            // hashes_file,
            // hashes_file_index: 0,
            shapes,
            string_interner,
            // hashes: Default::default(),
            context_hashes,
            context_hashes_cycles,
            // data: Vec::with_capacity(100_000),
        })
    }

    #[cfg(test)]
    pub(crate) fn put_object_hash(&mut self, entry_hash: ObjectHash) -> HashId {
        let vacant = self.get_vacant_object_hash().unwrap();
        vacant.write_with(|entry| *entry = entry_hash)
    }

    fn get_hash_id_from_offset<'a>(&self, object_ref: ObjectReference) -> Result<HashId, DBError> {
        let offset = object_ref.offset();

        let mut buffer: [u8; 4] = Default::default();

        // Read one byte to get the `ObjectHeader`
        self.data_file.read_exact_at(&mut buffer[..1], offset);
        let object_header: ObjectHeader = ObjectHeader::from_bytes([buffer[0]]);

        let header_length: u64 = match object_header.get_length() {
            ObjectLength::OneByte => 2,
            ObjectLength::TwoBytes => 3,
            ObjectLength::FourBytes => 5,
        };

        let offset = offset.add(header_length);
        self.data_file.read_exact_at(&mut buffer, offset);

        let hash_id = deserialize_hash_id(&buffer)?.0.unwrap();

        Ok(hash_id)
    }
}

fn deserialize_hashes(
    hashes_file: File,
    commit_index_file: &mut File,
) -> (
    Hashes,
    std::collections::HashMap<u64, ObjectReference, crate::NoHash>,
    VecDeque<Vec<u64>>,
) {
    let hashes = Hashes::try_new(hashes_file);
    let mut context_hashes: std::collections::HashMap<u64, ObjectReference, crate::NoHash> =
        Default::default();
    let context_hashes_cycles: VecDeque<Vec<u64>> = Default::default();

    let mut offset = 0u64;
    let end = commit_index_file.offset().as_u64();

    while offset < end {
        // commit index file is a sequence of entries that look like:
        // [hash_id u32 ne bytes | offset u64 ne bytes | hash <HASH_LEN> bytes]
        let mut hash_id_bytes = [0u8; 4];
        commit_index_file.read_exact_at(&mut hash_id_bytes, offset.into());
        offset += hash_id_bytes.len() as u64;
        let hash_id = u32::from_ne_bytes(hash_id_bytes);

        let mut hash_offset_bytes = [0u8; 8];
        commit_index_file.read_exact_at(&mut hash_offset_bytes, offset.into());
        offset += hash_offset_bytes.len() as u64;
        let hash_offset = u64::from_ne_bytes(hash_offset_bytes);

        let mut commit_hash: ObjectHash = Default::default();
        commit_index_file.read_exact_at(&mut commit_hash, offset.into());
        offset += commit_hash.len() as u64;

        let object_reference = ObjectReference::new(HashId::new(hash_id), Some(hash_offset.into()));

        context_hashes.insert(hash_offset, object_reference);
    }

    // TODO context_hash_cycles?

    (hashes, context_hashes, context_hashes_cycles)
}

fn serialize_context_hash(hash_id: HashId, offset: AbsoluteOffset, hash: &[u8]) -> Vec<u8> {
    let mut output = Vec::<u8>::with_capacity(100);

    let offset: u64 = offset.as_u64();
    let hash_id: u32 = hash_id.as_u32();

    // TODO: why ne bytes? will make the storage files non-portable
    output.write_all(&hash_id.to_ne_bytes()).unwrap();
    output.write_all(&offset.to_ne_bytes()).unwrap();
    output.write_all(hash).unwrap();

    output
}

impl KeyValueStoreBackend for Persistent {
    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        Ok(self.hashes.contains(hash_id))
    }

    fn put_context_hash(&mut self, object_ref: ObjectReference) -> Result<(), DBError> {
        let commit_hash = self.get_hash(object_ref).unwrap().unwrap();
        // let commit_hash = self
        //     .hashes
        //     .get_hash(commit_hash_id)?
        //     .ok_or(DBError::MissingObject {
        //         hash_id: commit_hash_id,
        //     })?;

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash[..]);
        let hashed = hasher.finish();

        let output = serialize_context_hash(
            object_ref.hash_id(),
            object_ref.offset(),
            commit_hash.as_ref(),
        );
        self.commit_index_file.append(&output);

        self.context_hashes.insert(hashed, object_ref);
        if let Some(back) = self.context_hashes_cycles.back_mut() {
            back.push(hashed);
        };

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

    fn get_hash(&self, object_ref: ObjectReference) -> Result<Option<Cow<ObjectHash>>, DBError> {
        let hash_id = self.get_hash_id(object_ref)?;
        self.hashes.get_hash(hash_id)
    }

    // fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, DBError> {
    //     todo!()
    // }

    fn get_vacant_object_hash(&mut self) -> Result<VacantObjectHash, DBError> {
        self.hashes.get_vacant_object_hash()
    }

    fn clear_objects(&mut self) -> Result<(), DBError> {
        Ok(())
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
        string_interner.extend_from(&self.string_interner);
    }

    fn get_current_offset(&self) -> Result<Option<AbsoluteOffset>, DBError> {
        Ok(Some(self.data_file.offset()))
    }

    fn append_serialized_data(&mut self, data: &[u8]) -> Result<(), DBError> {
        self.data_file.append(data);

        let strings = self.string_interner.serialize();

        self.strings_file.append(&strings.strings);
        self.big_strings_file.append(&strings.big_strings);
        self.big_strings_offsets_file
            .append(&strings.big_strings_offsets);

        let shapes = self.shapes.serialize();
        self.shape_file.append(shapes.shapes);
        self.shape_index_file.append(shapes.index);

        self.hashes.commit();

        self.data_file.sync();
        self.strings_file.sync();
        self.big_strings_file.sync();
        self.big_strings_offsets_file.sync();
        self.hashes.hashes_file.sync();
        self.commit_index_file.sync();

        Ok(())
    }

    fn synchronize_full(&mut self) -> Result<(), DBError> {
        Ok(())
    }

    fn get_object(
        &self,
        object_ref: ObjectReference,
        storage: &mut Storage,
    ) -> Result<Object, DBError> {
        self.get_object_bytes(object_ref, &mut storage.data)?;

        let object_bytes = std::mem::take(&mut storage.data);
        let result =
            persistent::deserialize_object(&object_bytes, object_ref.offset(), storage, self);
        storage.data = object_bytes;

        result.map_err(Into::into)
    }

    fn get_object_bytes<'a>(
        &self,
        object_ref: ObjectReference,
        buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], DBError> {
        self.data_file
            .get_object_bytes(object_ref, buffer)
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
            )
            .unwrap();

        self.append_serialized_data(&output)?;
        self.put_context_hash(commit_ref)?;

        let commit_hash = get_commit_hash(commit_ref, self).map_err(Box::new)?;
        Ok((commit_hash, serialize_stats))
    }

    fn synchronize_data(
        &mut self,
        batch: Vec<(HashId, Arc<[u8]>)>,
        output: &[u8],
    ) -> Result<Option<AbsoluteOffset>, DBError> {
        self.append_serialized_data(output)?;
        self.get_current_offset()
    }

    fn get_hash_id(&self, object_ref: ObjectReference) -> Result<HashId, DBError> {
        let hash_id = match object_ref.hash_id_opt() {
            Some(hash_id) => hash_id,
            None => self.get_hash_id_from_offset(object_ref)?,
        };

        Ok(hash_id)
    }
}
