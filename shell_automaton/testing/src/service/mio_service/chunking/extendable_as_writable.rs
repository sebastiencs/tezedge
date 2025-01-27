// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};

pub struct ExtendableAsWritable<'a, T: Extend<u8>> {
    extendable: &'a mut T,
}

impl<'a, T: Extend<u8>> Read for ExtendableAsWritable<'a, T> {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl<'a, T: Extend<u8>> Write for ExtendableAsWritable<'a, T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extendable.extend(buf.iter().cloned());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, T: Extend<u8>> From<&'a mut T> for ExtendableAsWritable<'a, T> {
    fn from(extendable: &'a mut T) -> Self {
        Self { extendable }
    }
}
