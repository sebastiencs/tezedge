use std::{
    convert::{TryFrom, TryInto},
    marker::PhantomData,
    sync::Arc,
};

use parking_lot::RwLock;

use super::DEFAULT_LIST_LENGTH;

#[derive(Debug)]
pub struct SharedChunk<T> {
    inner: Arc<RwLock<Vec<T>>>,
}

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
            inner: Arc::new(RwLock::new(Vec::with_capacity(chunk_capacity))),
        }
    }

    fn with<F, R>(&self, index: usize, fun: F) -> R
    where
        F: FnOnce(Option<&T>) -> R,
    {
        let inner = self.inner.read();
        fun(inner.get(index))
    }

    fn with_mut<F, R>(&self, index: usize, fun: F) -> R
    where
        F: FnOnce(Option<&mut T>) -> R,
    {
        let mut inner = self.inner.write();
        fun(inner.get_mut(index))
    }

    fn len(&self) -> usize {
        // TODO: Should we optimize this ?
        self.inner.read().len()
    }

    fn push(&self, elem: T) {
        self.inner.write().push(elem);
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

        match self.list_of_chunks.get(list_index) {
            Some(chunk) => chunk.with(chunk_index, fun),
            None => fun(None),
        }
    }

    fn with_mut<F, R>(&self, index: usize, fun: F) -> R
    where
        F: FnOnce(Option<&mut T>) -> R,
    {
        let (list_index, chunk_index) = self.get_indexes_at(index);

        match self.list_of_chunks.get(list_index) {
            Some(chunk) => chunk.with_mut(chunk_index, fun),
            None => fun(None),
        }
    }

    pub fn push(&mut self, elem: T) -> usize {
        let index = self.len();
        self.nelems += 1;

        self.get_next_chunk().push(elem);

        index
    }

    pub fn resize_with<F>(&mut self, new_len: usize, mut fun: F)
    where
        F: FnMut() -> T,
    {
        while self.nelems < new_len {
            self.push(fun());
        }
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
            inner: Self::with_chunk_capacity_empty(self.entries.chunk_capacity)
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
}

impl<K, V> SharedIndexMap<K, V>
where
    K: TryInto<usize>,
{
    pub fn contains_key(&self, key: K) -> Result<bool, K::Error> {
        let index = key.try_into()?;
        Ok(index < self.entries.len())
    }

    pub fn set(&mut self, key: K, value: V) -> Result<V, K::Error> {
        let index = key.try_into()?;

        let old = self
            .entries
            .with_mut(index, |old| std::mem::replace(old.unwrap(), value));

        Ok(old)
    }

    pub fn with<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&V>) -> R,
    {
        let index = key.try_into()?;
        Ok(self.entries.with(index, fun))
    }

    pub fn with_mut<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&mut V>) -> R,
    {
        let index = key.try_into()?;
        Ok(self.entries.with_mut(index, fun))
    }

    pub fn chunk_index_of(&self, key: K) -> Result<usize, K::Error> {
        let index: usize = key.try_into()?;
        Ok(index / self.entries.chunk_capacity)
    }
}

impl<K, V> SharedIndexMap<K, V>
where
    K: TryFrom<usize>,
{
    pub fn push(&mut self, value: V) -> Result<K, <K as TryFrom<usize>>::Error> {
        let index = self.entries.push(value);
        K::try_from(index)
    }
}

impl<K, V> SharedIndexMap<K, V>
where
    K: TryInto<usize>,
    K: TryFrom<usize>,
    V: Default,
{
    pub fn insert_at(&mut self, key: K, value: V) -> Result<V, <K as TryInto<usize>>::Error> {
        let index: usize = key.try_into()?;

        if index >= self.entries.len() {
            self.entries.resize_with(index + 1, V::default);
        }

        let old = self
            .entries
            .with_mut(index, |old| std::mem::replace(old.unwrap(), value));

        Ok(old)
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
}

impl<K, V> SharedIndexMapView<K, V>
where
    K: TryInto<usize>,
{
    pub fn set(&mut self, key: K, value: V) -> Result<V, K::Error> {
        self.inner.set(key, value)
    }

    pub fn with<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.with(key, fun)
    }

    pub fn with_mut<F, R>(&self, key: K, fun: F) -> Result<R, K::Error>
    where
        F: FnOnce(Option<&mut V>) -> R,
    {
        self.inner.with_mut(key, fun)
    }

    pub fn chunk_index_of(&self, key: K) -> Result<usize, K::Error> {
        self.inner.chunk_index_of(key)
    }
}
