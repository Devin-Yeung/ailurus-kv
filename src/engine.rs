use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::Errors::IndexUpdateFail;
use crate::errors::{Errors, Result};
use crate::{index, options};
use bytes::Bytes;
use std::collections::HashMap;

pub struct Engine {
    options: options::Options,
    active_file: DataFile,
    older_file: HashMap<u32, DataFile>,
    index: Box<dyn index::Indexer>,
}

impl Engine {
    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        let log_record_pos = self.append_log_record(record)?;
        match self.index.put(key.to_vec(), log_record_pos) {
            true => Ok(()),
            false => Err(IndexUpdateFail),
        }
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let pos = match self.index.get(key.to_vec()) {
            None => return Err(Errors::KeyNotFound),
            Some(x) => x,
        };

        let log_record = match self.active_file.id() == pos.file_id {
            true => self.active_file.read(pos.offset)?,
            false => match self.older_file.get(&pos.file_id) {
                None => return Err(Errors::DatafileNotFound),
                Some(x) => x.read(pos.offset)?,
            },
        };

        return match log_record.record_type {
            LogRecordType::Normal => Ok(log_record.into()),
            LogRecordType::Deleted => Err(Errors::KeyNotFound), // TODO: design decision, Result<Option<Bytes>> or Result<Bytes>
        };
    }

    fn append_log_record(&mut self, record: LogRecord) -> Result<LogRecordPos> {
        let dir_path = &self.options.dir_path;

        // encode the record using bitcask layout
        let record = record.encode();
        let record_len = record.len() as u64;

        // check if the datafile can hold the log record
        if self.active_file.offset() + record_len > self.options.data_file_size {
            self.active_file.sync()?;
            let fid = self.active_file.id();
            let fresh = DataFile::new(dir_path, fid + 1)?;
            // swap out the currently full datafile, swap in a fresh one
            self.older_file
                .insert(fid, std::mem::replace(&mut self.active_file, fresh));
        }

        // append the log record to the fresh one
        self.active_file.write(&record)?;

        if self.options.sync_writes {
            self.active_file.sync()?;
        }

        // indexing info
        Ok(LogRecordPos {
            file_id: self.active_file.id(),
            offset: self.active_file.offset(),
        })
    }
}
