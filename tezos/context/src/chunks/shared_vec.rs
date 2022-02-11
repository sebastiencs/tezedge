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

// impl<T> SharedChunk<T>
// where
//     T: Clone,
// {
//     fn get(&self, index: usize) -> Option<T> {
//         self.inner.read().get(index).cloned()
//     }
// }

#[derive(Debug)]
pub struct SharedChunkedVec<T> {
    pub list_of_chunks: Vec<SharedChunk<T>>,
    chunk_capacity: usize,
    /// Number of elements in the chunks
    nelems: usize,

    sync_at: usize,
}

impl<T> SharedChunkedVec<T> {
    pub fn empty() -> Self {
        Self {
            list_of_chunks: Vec::new(),
            chunk_capacity: 1_000,
            nelems: 0,
            sync_at: 0,
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
            sync_at: 0,
        }
    }

    pub fn with_chunk_capacity_empty(chunk_capacity: usize) -> Self {
        assert_ne!(chunk_capacity, 0);

        let list_of_chunks: Vec<SharedChunk<T>> = Vec::with_capacity(DEFAULT_LIST_LENGTH);

        Self {
            list_of_chunks,
            chunk_capacity,
            nelems: 0,
            sync_at: 0,
        }
    }

    fn append_chunks(&mut self, mut chunks: Vec<SharedChunk<T>>) {
        self.list_of_chunks.append(&mut chunks)
    }

    pub fn clone_new_chunks(&mut self) -> Option<Vec<SharedChunk<T>>> {
        let new_chunks = self.list_of_chunks.get(self.sync_at..)?;

        if new_chunks.is_empty() {
            return None;
        }

        let new_chunks: Vec<_> = new_chunks.iter().cloned().collect();
        // let new_chunks: Vec<_> = new_chunks.iter().map(|chunk| chunk.clone()).collect();

        // let old = self.sync_at;
        self.sync_at = self.list_of_chunks.len();

        // println!("OLD SYNC_AT={:?} NEW={:?}", old, self.sync_at);
        Some(new_chunks)
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

    // fn clear(&mut self) {
    //     self.nelems = 0;
    //     self.sync_at = 0;
    //     self.list_of_chunks.clear();
    // }
}

// impl<T> SharedChunkedVec<T>
// where
//     T: Clone,
// {
//     pub fn get(&self, index: usize) -> Option<T> {
//         let (list_index, chunk_index) = self.get_indexes_at(index);

//         self.list_of_chunks.get(list_index)?.get(chunk_index)
//     }
// }

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

    pub fn with_chunk_capacity_empty(chunk_capacity: usize) -> Self {
        Self {
            entries: SharedChunkedVec::with_chunk_capacity_empty(chunk_capacity),
            _phantom: PhantomData,
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

    // pub fn clear(&mut self) {
    //     self.entries.clear();
    // }

    // pub fn get_index(&self, index: usize) -> Option<&V> {
    //     self.entries.get(index)
    // }

    // pub fn iter_values(&self) -> impl Iterator<Item = &V> {
    //     self.entries.iter()
    // }

    // pub fn iter_with_keys(&self) -> IndexMapIter<'_, K, V> {
    //     IndexMapIter {
    //         chunks: self.entries.iter(),
    //         _phantom: PhantomData,
    //     }
    // }

    // pub fn clear(&mut self) {
    //     self.entries.clear();
    // }
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

    // pub fn get(&self, key: K) -> Result<Option<&V>, K::Error> {
    //     Ok(self.entries.get(key.try_into()?))
    // }

    // pub fn get_mut(&mut self, key: K) -> Result<Option<&mut V>, K::Error> {
    //     Ok(self.entries.get_mut(key.try_into()?))
    // }
}

// impl<K, V> SharedIndexMap<K, V>
// where
//     K: TryInto<usize>,
//     V: Clone,
// {
//     pub fn get(&self, key: K) -> Result<Option<V>, K::Error> {
//         Ok(self.entries.get(key.try_into()?))
//     }
// }

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
    // pub fn get_vacant_entry(&mut self) -> Result<(K, &mut V), <K as TryFrom<usize>>::Error> {
    //     let index = self.entries.push(Default::default());
    //     Ok((K::try_from(index)?, &mut self.entries[index]))
    // }

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
