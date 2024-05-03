use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use error_stack::Report;
use parking_lot::Mutex;

use crate::data::log_record::{LogRecord, LogRecordType};
use crate::engine::Engine;
use crate::errors::{Errors, Result};
use crate::options::WriteBatchOptions;

pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl<'a> WriteBatch<'a> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Report::new(Errors::EmptyKey));
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        let mut guard = self.pending_writes.lock();
        guard.insert(key.to_vec(), record);
        drop(guard);
        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Report::new(Errors::EmptyKey));
        }

        match self.engine.index.get(key.to_vec()) {
            None => {
                // Key does not exist in the index, check the batch
                let mut guard = self.pending_writes.lock();
                if guard.contains_key(&key.to_vec()) {
                    guard.remove(&key.to_vec());
                }
            }
            Some(_) => {
                let record = LogRecord {
                    key: key.to_vec(),
                    value: Default::default(),
                    record_type: LogRecordType::Deleted,
                };
                let mut guard = self.pending_writes.lock();
                guard.insert(key.to_vec(), record);
            }
        }
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut guard = self.pending_writes.lock();

        if guard.len() == 0 {
            return Ok(());
        }

        if guard.len() > self.options.batch_size {
            return Err(Report::new(Errors::BatchSizeExceeded));
        }

        let _lock = self.engine.batch_commit_lock.lock();

        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        // write batch to data file, with seq_no encoded
        for (_, item) in guard.iter() {
            todo!()
        }

        Ok(())
    }
}
