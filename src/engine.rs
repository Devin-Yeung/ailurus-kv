use std::collections::HashMap;
use bytes::Bytes;
use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::{Result, Errors};
use crate::{index, options};
use crate::errors::Errors::IndexUpdateFail;

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
            false => Err(IndexUpdateFail)
        }
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
            self.older_file.insert(fid, std::mem::replace(&mut self.active_file, fresh));
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