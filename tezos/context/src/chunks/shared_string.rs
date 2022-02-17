// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{borrow::Cow, ops::Range, sync::Arc};

use super::DEFAULT_LIST_LENGTH;

#[derive(Debug, Clone, PartialEq, Eq)]
enum SliceOfString<const CHUNK_CAPACITY: usize> {
    Immutable(Arc<str>),
    Mutable(String),
}

impl<const CHUNK_CAPACITY: usize> std::ops::Deref for SliceOfString<CHUNK_CAPACITY> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            SliceOfString::Immutable(s) => s,
            SliceOfString::Mutable(s) => s.as_str(),
        }
    }
}

impl<const CHUNK_CAPACITY: usize> SliceOfString<CHUNK_CAPACITY> {
    fn clear(&mut self) {
        if let Self::Mutable(s) = self {
            s.clear();
        } else {
            *self = Self::Mutable(String::with_capacity(CHUNK_CAPACITY));
        }
    }
}

/// Structure similar to `ChunkedVec` but using `String` instead of `Vec<T>`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedString<const CHUNK_CAPACITY: usize> {
    list_of_chunks: Vec<SliceOfString<CHUNK_CAPACITY>>,
    /// Number of bytes
    nbytes: usize,
}

impl<const CHUNK_CAPACITY: usize> ChunkedString<CHUNK_CAPACITY> {
    /// Returns a new `ChunkedString` without allocating
    pub fn empty() -> Self {
        Self {
            list_of_chunks: Vec::new(),
            nbytes: 0,
        }
    }

    pub fn new() -> Self {
        assert_ne!(CHUNK_CAPACITY, 0);

        let chunk = SliceOfString::Mutable(String::with_capacity(CHUNK_CAPACITY));

        let mut list_of_vec: Vec<SliceOfString<CHUNK_CAPACITY>> =
            Vec::with_capacity(DEFAULT_LIST_LENGTH);
        list_of_vec.push(chunk);

        Self {
            list_of_chunks: list_of_vec,
            nbytes: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nbytes == 0
    }

    pub fn extend_from(&mut self, other: &Self) {
        let mut last_immutable = 0;

        while let Some(SliceOfString::Immutable(_)) = self.list_of_chunks.get(last_immutable) {
            last_immutable += 1;
        }

        let index_to_clone = last_immutable;
        let chunks_to_clone = other.list_of_chunks.get(index_to_clone..).unwrap_or(&[]);

        for (index, chunk) in chunks_to_clone.iter().enumerate() {
            match self.list_of_chunks.get_mut(index_to_clone + index) {
                Some(mut self_chunk) => {
                    self.nbytes -= self_chunk.len();
                    self.nbytes += chunk.len();

                    match (&mut self_chunk, chunk) {
                        (SliceOfString::Mutable(_), SliceOfString::Immutable(_)) => {
                            *self_chunk = chunk.clone();
                        }
                        (SliceOfString::Mutable(s), SliceOfString::Mutable(_)) => {
                            let our_length = s.len();
                            let other_length = chunk.len();

                            if our_length != other_length {
                                assert!(our_length < other_length);
                                s.push_str(&chunk[our_length..]);
                            }
                        }
                        (SliceOfString::Immutable(_), _) => unreachable!(),
                    }
                }
                None => {
                    let new = match chunk {
                        SliceOfString::Mutable(owned) => {
                            let mut s = String::with_capacity(CHUNK_CAPACITY);
                            s.push_str(owned);
                            SliceOfString::Mutable(s)
                        }
                        s => s.clone(), // Clone the Arc
                    };

                    self.nbytes += new.len();

                    self.list_of_chunks.push(new);
                }
            }
        }

        assert_eq!(self.nbytes, other.nbytes);
    }

    /// Extends the last chunk with `slice`
    ///
    /// Return the index of the slice in the chunks, and its length
    pub fn push_str(&mut self, slice: &str) -> (usize, usize) {
        let start = self.len();
        let slice_length = slice.len();
        let mut remaining_slice = slice;

        while !remaining_slice.is_empty() {
            let last_chunk = self.get_next_chunk();
            let space_in_chunk = CHUNK_CAPACITY - last_chunk.len();

            let (slice, rest) = if remaining_slice.len() > space_in_chunk {
                remaining_slice.split_at(space_in_chunk)
            } else {
                (remaining_slice, "")
            };

            remaining_slice = rest;
            last_chunk.push_str(slice);
        }

        self.nbytes += slice_length;
        (start, slice_length)
    }

    /// Returns the last chunk with space available.
    ///
    /// Allocates one more chunk in 2 cases:
    /// - The last chunk has reached `Self::chunk_capacity` limit
    /// - `Self::list_of_chunks` is empty
    fn get_next_chunk(&mut self) -> &mut String {
        let must_alloc_new_chunk = self
            .list_of_chunks
            .last()
            .map(|chunk| {
                debug_assert!(chunk.len() <= CHUNK_CAPACITY);
                chunk.len() == CHUNK_CAPACITY
            })
            .unwrap_or(true);

        if must_alloc_new_chunk {
            let empty_owned = self.convert_last_owned_to_shared();

            assert!(empty_owned.is_empty());
            assert_eq!(empty_owned.capacity(), CHUNK_CAPACITY);

            self.list_of_chunks
                .push(SliceOfString::Mutable(empty_owned));
        }

        // Never fail, we just allocated one in case it's empty
        match self.list_of_chunks.last_mut().unwrap() {
            SliceOfString::Mutable(owned) => owned,
            SliceOfString::Immutable(_) => panic!("Invalid state"),
        }
    }

    fn convert_last_owned_to_shared(&mut self) -> String {
        let last = match self.list_of_chunks.last_mut() {
            Some(last) => last,
            None => return String::with_capacity(CHUNK_CAPACITY),
        };

        let owned = match last {
            SliceOfString::Mutable(owned) => owned,
            SliceOfString::Immutable(_) => panic!("Invalid state"),
        };

        assert_eq!(owned.capacity(), CHUNK_CAPACITY);

        let shared = Arc::<str>::from(owned.as_str());
        owned.clear();

        let owned = std::mem::replace(last, SliceOfString::Immutable(shared));

        match owned {
            SliceOfString::Mutable(owned) => owned,
            SliceOfString::Immutable(_) => panic!("Invalid state"),
        }
    }

    pub fn capacity(&self) -> usize {
        CHUNK_CAPACITY * self.list_of_chunks.len()
    }

    pub fn get(&self, Range { start, end }: Range<usize>) -> Option<Cow<str>> {
        let slice_length = end - start;
        let (list_index, chunk_index) = self.get_indexes_at(start);

        let chunk = self.list_of_chunks.get(list_index)?;

        if chunk_index + slice_length <= CHUNK_CAPACITY {
            chunk
                .get(chunk_index..chunk_index + slice_length)
                .map(Cow::Borrowed)
        } else {
            let mut slice = String::with_capacity(slice_length);
            let mut iter_chunk = self.list_of_chunks.get(list_index..)?.iter();
            let mut start_in_chunk = chunk_index;
            let mut length = slice_length;

            while length > 0 {
                let chunk = iter_chunk.next()?;
                let end_in_chunk = (start_in_chunk + length).min(CHUNK_CAPACITY);

                let part_slice = chunk.get(start_in_chunk..end_in_chunk)?;
                slice.push_str(part_slice);

                length -= end_in_chunk - start_in_chunk;
                start_in_chunk = 0;
            }

            debug_assert_eq!(slice.len(), slice_length);

            Some(Cow::Owned(slice))
        }
    }

    fn get_indexes_at(&self, index: usize) -> (usize, usize) {
        let list_index = index / CHUNK_CAPACITY;
        let chunk_index = index % CHUNK_CAPACITY;

        (list_index, chunk_index)
    }

    pub fn len(&self) -> usize {
        self.nbytes
    }

    pub fn deallocate(&mut self) {
        self.list_of_chunks = Vec::new();
        self.nbytes = 0;
    }

    pub fn clear(&mut self) {
        self.list_of_chunks.truncate(1);
        if let Some(first_chunk) = self.list_of_chunks.last_mut() {
            first_chunk.clear();
        };
        self.nbytes = 0;
    }
}
