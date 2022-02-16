use std::{
    convert::{TryFrom, TryInto},
    marker::PhantomData,
    sync::Arc,
};

use parking_lot::RwLock;
use static_assertions::assert_eq_size;

use crate::ObjectHash;

use super::DEFAULT_LIST_LENGTH;

#[derive(Debug)]
struct VecAliveCounter<T> {
    alive_counter: u32,
    inner: Vec<T>,
}

impl<T> std::ops::Deref for VecAliveCounter<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for VecAliveCounter<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> VecAliveCounter<T> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            alive_counter: 0,
            inner: Vec::with_capacity(cap),
        }
    }
}

#[derive(Debug)]
pub struct SharedChunk<T> {
    inner: Arc<RwLock<VecAliveCounter<T>>>,
}

assert_eq_size!([u8; 40], RwLock<VecAliveCounter<u8>>);

impl<T> Clone for SharedChunk<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> SharedChunk<T> {
    fn with_capacity(chunk_capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(VecAliveCounter::with_capacity(chunk_capacity))),
        }
    }

    fn with<F, R>(&self, index: usize, fun: F) -> R
    where
        F: FnOnce(Option<&T>) -> R,
    {
        let inner = self.inner.read();
        fun(Some(&inner[index]))
    }

    fn len(&self) -> usize {
        // TODO: Should we optimize this ?
        self.inner.read().len()
    }
}

// impl SharedChunk<ObjectHash> {
//     fn clear(&self, index: usize, is_last_chunk: bool) -> bool {
//         let mut inner = self.inner.write();

//         // let old = match std::mem::take(&mut inner[index]) {
//         //     Some(old) => old,
//         //     None => return (None, false),
//         // };

//         inner.alive_counter = inner.alive_counter.checked_sub(1).unwrap();

//         if inner.alive_counter == 0 && !is_last_chunk {
//             inner.inner = Vec::new();
//             return true;
//         }

//         false
//     }

//     fn push(&self, elem: ObjectHash, is_alive: bool) -> usize {
//         let mut inner = self.inner.write();

//         let index = inner.len();

//         if is_alive {
//             inner.alive_counter += 1;
//         }
//         inner.push(elem);

//         index
//     }

//     fn insert_alive_at(&self, index: usize, value: ObjectHash, chunk_capacity: usize) {
//         let mut inner = self.inner.write();

//         if inner.capacity() == 0 {
//             assert_eq!(inner.alive_counter, 0);
//             inner.inner = Vec::with_capacity(chunk_capacity);
//             inner.resize_with(chunk_capacity, Default::default);
//         }

//         inner[index] = value;
//         // if std::mem::replace(&mut inner[index], value).is_none() {
//         inner.alive_counter += 1;
//         // }

//         assert!(inner.alive_counter as usize <= chunk_capacity);
//     }
// }

impl<T: std::fmt::Debug + Eq> SharedChunk<Option<T>> {
    fn push(&self, elem: Option<T>) -> usize {
        let mut inner = self.inner.write();

        let index = inner.len();

        if elem.is_some() {
            inner.alive_counter += 1;
        }
        inner.push(elem);

        // let count = inner.iter().fold(0, |acc, v| {
        //     if v.is_some() {
        //         acc + 1
        //     } else {
        //         acc
        //     }
        // });
        // assert_eq!(inner.alive_counter, count);

        index
    }

    fn clear(&self, index: usize, is_last_chunk: bool) -> (Option<T>, bool) {
        let mut inner = self.inner.write();

        if inner.capacity() == 0 {
            return (None, false);
        }

        let old = match std::mem::take(&mut inner[index]) {
            Some(old) => old,
            None => return (None, false),
        };

        inner.alive_counter = inner.alive_counter.checked_sub(1).unwrap();

        if inner.alive_counter == 0 && !is_last_chunk {
            inner.inner = Vec::new();
            return (Some(old), true);
        }

        (Some(old), false)
    }

    fn insert_alive_at(&self, index: usize, value: Option<T>, chunk_capacity: usize) {
        let mut inner = self.inner.write();

        // let count = inner.iter().fold(0, |acc, v| {
        //     if v.is_some() {
        //         acc + 1
        //     } else {
        //         acc
        //     }
        // });
        // assert_eq!(inner.alive_counter, count);

        if inner.capacity() == 0 {
            assert_eq!(inner.alive_counter, 0);
            inner.inner = Vec::with_capacity(chunk_capacity);
            inner.resize_with(chunk_capacity, Default::default);
        }

        if std::mem::replace(&mut inner[index], value).is_none() {
            inner.alive_counter += 1;
        }

        // let count = inner.iter().fold(0, |acc, v| {
        //     if v.is_some() {
        //         acc + 1
        //     } else {
        //         acc
        //     }
        // });
        // assert_eq!(inner.alive_counter, count);

        assert!(inner.alive_counter as usize <= chunk_capacity);
    }
}

#[derive(Debug)]
pub struct SharedChunkedVec<T> {
    pub list_of_chunks: Vec<SharedChunk<T>>,
    chunk_capacity: usize,
    /// Number of elements in the chunks
    nelems: usize,

    synced_at: usize,
}

impl<T> SharedChunkedVec<T> {
    pub fn empty() -> Self {
        Self {
            list_of_chunks: Vec::new(),
            chunk_capacity: 1_000,
            nelems: 0,
            synced_at: 0,
        }
    }

    pub fn with_chunk_capacity(chunk_capacity: usize) -> Self {
        assert_ne!(chunk_capacity, 0);

        let chunk: SharedChunk<T> = SharedChunk::with_capacity(chunk_capacity);

        let mut list_of_chunks: Vec<SharedChunk<T>> = Vec::with_capacity(DEFAULT_LIST_LENGTH);
        list_of_chunks.push(chunk);

        Self {
            list_of_chunks,
            chunk_capacity,
            nelems: 0,
            synced_at: 0,
        }
    }

    pub fn with_chunk_capacity_empty(chunk_capacity: usize) -> Self {
        assert_ne!(chunk_capacity, 0);

        let list_of_chunks: Vec<SharedChunk<T>> = Vec::with_capacity(DEFAULT_LIST_LENGTH);

        Self {
            list_of_chunks,
            chunk_capacity,
            nelems: 0,
            synced_at: 0,
        }
    }

    fn append_chunks(&mut self, mut chunks: Vec<SharedChunk<T>>) {
        self.list_of_chunks.append(&mut chunks)
    }

    pub fn clone_new_chunks(&mut self) -> Option<Vec<SharedChunk<T>>> {
        let new_chunks = self.list_of_chunks.get(self.synced_at..)?;

        if new_chunks.is_empty() {
            return None;
        }

        self.synced_at = self.list_of_chunks.len();

        Some(new_chunks.iter().cloned().collect())
    }

    /// Returns the last chunk with space available.
    ///
    /// Allocates one more chunk in 2 cases:
    /// - The last chunk has reached `Self::chunk_capacity` limit
    /// - `Self::list_of_chunks` is empty
    fn get_next_chunk(&mut self) -> &mut SharedChunk<T> {
        let chunk_capacity = self.chunk_capacity;

        let must_alloc_new_chunk = self
            .list_of_chunks
            .last()
            .map(|chunk| {
                debug_assert!(chunk.len() <= chunk_capacity);
                chunk.len() == chunk_capacity
            })
            .unwrap_or(true);

        if must_alloc_new_chunk {
            self.list_of_chunks
                .push(SharedChunk::with_capacity(self.chunk_capacity));
        }

        // Never fail, we just allocated one in case it's empty
        self.list_of_chunks.last_mut().unwrap()
    }

    pub fn capacity(&self) -> usize {
        self.chunk_capacity * self.list_of_chunks.len()
    }

    fn get_indexes_at(&self, index: usize) -> (usize, usize) {
        let list_index = index / self.chunk_capacity;
        let chunk_index = index % self.chunk_capacity;

        (list_index, chunk_index)
    }

    pub fn len(&self) -> usize {
        self.nelems
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn with<F, R>(&self, index: usize, fun: F) -> R
    where
        F: FnOnce(Option<&T>) -> R,
    {
        let (list_index, chunk_index) = self.get_indexes_at(index);

        self.list_of_chunks[list_index].with(chunk_index, fun)
    }
}

// impl SharedChunkedVec<ObjectHash> {
//     fn clear(&self, index: usize) -> bool {
//         let (list_index, chunk_index) = self.get_indexes_at(index);

//         let is_last_chunk = list_index + 1 == self.list_of_chunks.len();
//         self.list_of_chunks[list_index].clear(chunk_index, is_last_chunk)
//     }

//     fn insert_at(&self, index: usize, value: ObjectHash) {
//         let (list_index, chunk_index) = self.get_indexes_at(index);
//         self.list_of_chunks[list_index].insert_alive_at(chunk_index, value, self.chunk_capacity);
//     }

//     pub fn resize_with(&mut self, new_len: usize) {
//         while self.nelems < new_len {
//             self.push_impl(Default::default(), false);
//         }
//     }

//     pub fn push(&mut self, elem: ObjectHash) -> usize {
//         self.push_impl(elem, true)
//     }

//     pub fn push_impl(&mut self, elem: ObjectHash, is_alive: bool) -> usize {
//         let index = self.len();
//         self.nelems += 1;

//         let index_in_chunk = self.get_next_chunk().push(elem, is_alive);
//         let list_index = self.list_of_chunks.len() - 1;

//         assert_eq!((list_index * self.chunk_capacity) + index_in_chunk, index);

//         index
//     }
// }

impl<T: std::fmt::Debug + Eq> SharedChunkedVec<Option<T>> {
    fn clear(&self, index: usize) -> (Option<T>, bool) {
        let (list_index, chunk_index) = self.get_indexes_at(index);

        let is_last_chunk = list_index + 1 == self.list_of_chunks.len();
        self.list_of_chunks[list_index].clear(chunk_index, is_last_chunk)
    }

    pub fn push(&mut self, elem: Option<T>) -> usize {
        let index = self.len();
        self.nelems += 1;

        let index_in_chunk = self.get_next_chunk().push(elem);
        let list_index = self.list_of_chunks.len() - 1;

        assert_eq!((list_index * self.chunk_capacity) + index_in_chunk, index);

        index
    }

    pub fn resize_with(&mut self, new_len: usize) {
        while self.nelems < new_len {
            self.push(None);
        }
    }

    fn insert_at(&self, index: usize, value: Option<T>) {
        let (list_index, chunk_index) = self.get_indexes_at(index);
        self.list_of_chunks[list_index].insert_alive_at(chunk_index, value, self.chunk_capacity);
    }
}

#[derive(Debug)]
pub struct SharedIndexMap<K, V> {
    pub entries: SharedChunkedVec<V>,
    _phantom: PhantomData<K>,
}

impl<K, V> SharedIndexMap<K, V> {
    pub fn empty() -> Self {
        Self {
            entries: SharedChunkedVec::empty(),
            _phantom: PhantomData,
        }
    }

    pub fn with_chunk_capacity(cap: usize) -> Self {
        Self {
            entries: SharedChunkedVec::with_chunk_capacity(cap),
            _phantom: PhantomData,
        }
    }

    fn with_chunk_capacity_empty(chunk_capacity: usize) -> Self {
        Self {
            entries: SharedChunkedVec::with_chunk_capacity_empty(chunk_capacity),
            _phantom: PhantomData,
        }
    }

    pub fn get_view(&self) -> SharedIndexMapView<K, V> {
        SharedIndexMapView {
            inner: Self::with_chunk_capacity_empty(self.entries.chunk_capacity),
        }
    }

    pub fn append_chunks(&mut self, chunks: Vec<SharedChunk<V>>) {
        self.entries.append_chunks(chunks)
    }

    pub fn clone_new_chunks(&mut self) -> Option<Vec<SharedChunk<V>>> {
        self.entries.clone_new_chunks()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn alive_dead(&self) -> (usize, usize) {
        let mut alive = 0;
        let mut dead = 0;

        for chunk in &self.entries.list_of_chunks {
            if chunk.inner.read().is_empty() {
                dead += 1;
            } else {
                alive += 1;
            }
        }

        (alive, dead)
    }
}

impl<K, V> SharedIndexMap<K, V>
where
    K: TryInto<usize>,
{
    pub fn contains_key(&self, key: K) -> Result<bool, K::Error> {
        let index = key.try_into()?;
        Ok(index < self.entries.len())
    }

    pub fn with<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&V>) -> R,
    {
        let index = key.try_into()?;
        Ok(self.entries.with(index, fun))
    }

    pub fn chunk_index_of(&self, key: K) -> Result<usize, K::Error> {
        let index: usize = key.try_into()?;
        Ok(index / self.entries.chunk_capacity)
    }
}

impl<K, V: std::fmt::Debug + Eq> SharedIndexMap<K, Option<V>>
where
    K: TryFrom<usize>,
    K: TryInto<usize>,
    K: std::fmt::Debug + Clone,
{
    pub fn push(&mut self, value: V) -> Result<K, <K as TryFrom<usize>>::Error> {
        let index = self.entries.push(Some(value));
        let key = K::try_from(index);

        assert!(self
            .with(key.as_ref().ok().unwrap().clone(), |v| v.is_some())
            .ok()
            .unwrap());

        key
    }
}

// impl<K> SharedIndexMap<K, ObjectHash>
// where
//     K: TryInto<usize> + std::fmt::Debug + Clone,
// {
//     pub fn clear(&self, key: K) -> Result<bool, K::Error> {
//         let index = key.try_into()?;
//         Ok(self.entries.clear(index))
//     }

//     pub fn insert_at(&mut self, key: K, value: ObjectHash) -> Result<(), K::Error> {
//         let index: usize = key.try_into()?;

//         if index >= self.entries.len() {
//             self.entries.resize_with(index + 1);
//         }

//         self.entries.insert_at(index, value);

//         Ok(())
//     }
// }

// impl<K> SharedIndexMap<K, ObjectHash>
// where
//     K: TryFrom<usize>,
//     K: TryInto<usize>,
//     K: std::fmt::Debug + Clone,
// {
//     pub fn push(&mut self, value: ObjectHash) -> Result<K, <K as TryFrom<usize>>::Error> {
//         let index = self.entries.push(value);
//         let key = K::try_from(index);

//         assert!(self
//             .with(key.as_ref().ok().unwrap().clone(), |v| v.is_some())
//             .ok()
//             .unwrap());

//         key
//     }
// }

impl<K, V: std::fmt::Debug + Eq> SharedIndexMap<K, Option<V>>
where
    K: TryInto<usize> + std::fmt::Debug + Clone,
{
    pub fn clear(&self, key: K) -> Result<(Option<V>, bool), K::Error> {
        let index = key.try_into()?;
        Ok(self.entries.clear(index))
    }

    pub fn insert_at(&mut self, key: K, value: V) -> Result<(), K::Error> {
        let index: usize = key.try_into()?;

        if index >= self.entries.len() {
            self.entries.resize_with(index + 1);
        }

        self.entries.insert_at(index, Some(value));

        Ok(())
    }
}

pub struct SharedIndexMapView<K, V> {
    inner: SharedIndexMap<K, V>,
}

impl<K, V> SharedIndexMapView<K, V> {
    pub fn append_chunks(&mut self, chunks: Vec<SharedChunk<V>>) {
        self.inner.append_chunks(chunks)
    }

    pub fn nchunks(&self) -> usize {
        self.inner.entries.list_of_chunks.len()
    }

    pub fn alive_dead(&self) -> (usize, usize) {
        self.inner.alive_dead()
    }
}

impl<K, V: std::fmt::Debug + Eq> SharedIndexMapView<K, Option<V>>
where
    K: TryInto<usize> + std::fmt::Debug + Clone,
{
    pub fn clear(&self, key: K) -> Result<(Option<V>, bool), K::Error> {
        self.inner.clear(key)
    }
}

// impl<K> SharedIndexMapView<K, ObjectHash>
// where
//     K: TryInto<usize> + std::fmt::Debug + Clone,
// {
//     pub fn clear(&self, key: K) -> Result<bool, K::Error> {
//         self.inner.clear(key)
//     }
// }

impl<K, V> SharedIndexMapView<K, V>
where
    K: TryInto<usize>,
{
    pub fn with<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.with(key, fun)
    }

    pub fn chunk_index_of(&self, key: K) -> Result<usize, K::Error> {
        self.inner.chunk_index_of(key)
    }
}
