use crate::data::data_file::{DataFile, DATAFILE_SUFFIX, INITIAL_DATAFILE_ID};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::Errors::IndexUpdateFail;
use crate::errors::{Errors, Result};
use crate::index::indexer;
use crate::{index, options};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub struct Engine {
    options: options::Options,
    active_file: DataFile,
    older_file: HashMap<u32, DataFile>,
    index: Box<dyn index::Indexer>,
}

impl Engine {
    pub fn new(opts: options::Options) -> Result<Self> {
        // validate the configuration
        options::check_options(&opts)?;

        if opts.dir_path.is_dir() {
            if let Err(_) = fs::create_dir_all(&opts.dir_path) {
                // TODO: log the err
                return Err(Errors::CreateDbDirFail);
            }
        }

        // load the datafiles (including active and inactive)
        let mut datafiles = load_datafiles(&opts.dir_path)?;
        let index = indexer(datafiles.values(), &opts.index_type);

        let active = match datafiles.len() {
            0 => {
                // Empty database, open a fresh new active datafile
                DataFile::new(&opts.dir_path, INITIAL_DATAFILE_ID)?
            }
            _ => {
                // the datafile with largest fid is the currently active datafile
                let active_fid = datafiles.keys().max().unwrap().clone();
                datafiles.remove(&active_fid).unwrap()
            }
        };

        Ok(Engine {
            options: opts,
            active_file: active,
            older_file: datafiles,
            index,
        })
    }

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

        // Check the existence of the key
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

        return match log_record {
            // already check the existence of key, if we go a `None` from datafile (indicate an EOF),
            // it means datafiles must have been destroyed or something unexpected happened
            None => Err(Errors::InternalError),
            Some(record) => {
                match record.record_type {
                    LogRecordType::Normal => Ok(record.into()),
                    LogRecordType::Deleted => Err(Errors::KeyNotFound), // TODO: design decision, Result<Option<Bytes>> or Result<Bytes>
                }
            }
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

fn load_datafiles<P: AsRef<Path>>(path: P) -> Result<HashMap<u32, DataFile>> {
    let dir = fs::read_dir(&path).map_err(|_| Errors::ReadDbDirFail)?;
    let mut datafiles = HashMap::<u32, DataFile>::new();

    for file in dir {
        if let Ok(entry) = file {
            let fname = entry.file_name();

            if fname.to_str().unwrap().ends_with(DATAFILE_SUFFIX) {
                // example datafile name: `00001.data`
                let split: Vec<&str> = fname.to_str().unwrap().split(".").collect();
                let fid = match split[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Errors::DatafileCorrupted);
                    }
                };
                datafiles.insert(fid, DataFile::new(&path, fid)?);
            }
        }
    }

    Ok(datafiles)
}
