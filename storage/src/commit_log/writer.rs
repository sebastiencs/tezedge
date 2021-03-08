use crate::commit_log::error::TezedgeCommitLogError;
use crate::commit_log::{Index, DATA_FILE_NAME, INDEX_FILE_NAME, TH_LENGTH};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::ops::Sub;
use std::path::{Path, PathBuf};

pub(crate) struct Writer {
    index_file: File,
    data_file: File,
}

impl Writer {
    pub(crate) fn new<P: AsRef<Path>>(dir: P) -> Result<Self, TezedgeCommitLogError> {
        if !dir.as_ref().exists() {
            std::fs::create_dir_all(dir.as_ref())?;
        }
        if dir.as_ref().exists() & !dir.as_ref().is_dir() {
            return Err(TezedgeCommitLogError::PathError);
        }

        let mut index_file_path = PathBuf::new();
        index_file_path.push(dir.as_ref());
        index_file_path.push(INDEX_FILE_NAME);

        let mut data_file_path = PathBuf::new();
        data_file_path.push(dir.as_ref());
        data_file_path.push(DATA_FILE_NAME);

        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(index_file_path.as_path())?;

        let data_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(data_file_path.as_path())?;

        Ok(Self {
            index_file,
            data_file,
        })
    }

    pub(crate) fn write(&mut self, buf: &[u8]) -> Result<u64, TezedgeCommitLogError> {
        {
            let mut index_file_buf_writer = BufWriter::new(&mut self.index_file);
            let mut data_file_buf_writer = BufWriter::new(&mut self.data_file);
            if buf.len() > u64::MAX as usize {
                return Err(TezedgeCommitLogError::MessageLengthError);
            }
            let message_len = buf.len() as u64;
            let message_pos = data_file_buf_writer.seek(SeekFrom::End(0))?;
            data_file_buf_writer.write_all(&buf)?;
            let th = Index::new(message_pos, message_len);
            index_file_buf_writer.seek(SeekFrom::End(0))?;
            index_file_buf_writer.write_all(&th.to_vec())?;
            data_file_buf_writer.flush()?;
            index_file_buf_writer.flush()?;
        }
        Ok(self.last_index() as u64)
    }

    pub fn last_index(&self) -> i64 {
        let metadata = match self.index_file.metadata() {
            Ok(m) => m,
            Err(_) => return -1,
        };
        let items_count = metadata.len() / (TH_LENGTH as u64);
        (items_count as i64).sub(1)
    }

    pub(crate) fn flush(&mut self) -> Result<(), TezedgeCommitLogError> {
        self.data_file.flush()?;
        self.index_file.flush()?;
        Ok(())
    }
}