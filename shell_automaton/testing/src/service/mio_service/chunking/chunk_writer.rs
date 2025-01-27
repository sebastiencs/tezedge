// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::io::{self, Write};

use tezos_messages::p2p::binary_message::BinaryChunk;

use super::extendable_as_writable::ExtendableAsWritable;

/// BinaryChunk Writer.
#[derive(Debug, Clone)]
pub struct ChunkWriter {
    bytes: BinaryChunk,
    /// Index of the next to write byte inside the chunk.
    index: usize,
}

impl ChunkWriter {
    pub fn new(bytes: BinaryChunk) -> Self {
        Self { bytes, index: 0 }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.content().len() == 0
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.index >= self.bytes.raw().len()
    }

    #[inline]
    fn next_bytes(&self) -> &[u8] {
        &self.bytes.raw()[self.index..]
    }

    pub fn write_to<W>(&mut self, writer: &mut W) -> Result<(), io::Error>
    where
        W: Write,
    {
        loop {
            let size = writer.write(self.next_bytes())?;
            self.index += size;
            if self.is_finished() {
                return Ok(());
            } else if size == 0 {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "written 0 bytes"));
            }
        }
    }

    #[allow(dead_code)]
    pub fn write_to_extendable<T>(&mut self, extendable: &mut T) -> Result<(), io::Error>
    where
        T: Extend<u8>,
    {
        self.write_to(&mut ExtendableAsWritable::from(extendable))
    }
}
