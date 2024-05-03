use crate::data::data_file::{DataFile, DATAFILE_SUFFIX, INITIAL_DATAFILE_ID};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::{Errors, Result};
use crate::index::indexer;
use crate::{index, options};
use bytes::Bytes;
use error_stack::{Report, ResultExt};
use log::error;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub struct Engine {
    pub(crate) options: options::Options,
    active_file: DataFile,
    idle_file: HashMap<u32, DataFile>,
    pub(crate) index: Box<dyn index::Indexer>,
}

impl Engine {
    pub fn new(opts: options::Options) -> Result<Self> {
        // validate the configuration
        options::check_options(&opts)?;

        if opts.dir_path.is_dir() {
            fs::create_dir_all(&opts.dir_path).change_context(Errors::CreateDbDirFail)?;
        }

        // load the datafiles (including active and inactive)
        let mut datafiles = load_datafiles(&opts.dir_path)?;
        let index = indexer(datafiles.values(), &opts.index_type)?;

        let active = match datafiles.len() {
            0 => {
                // Empty database, open a fresh new active datafile
                DataFile::new(&opts.dir_path, INITIAL_DATAFILE_ID)?
            }
            _ => {
                // the datafile with the largest fid is the currently active datafile
                let active_fid = *datafiles.keys().max().unwrap();
                datafiles.remove(&active_fid).unwrap()
            }
        };

        Ok(Engine {
            options: opts,
            active_file: active,
            idle_file: datafiles,
            index,
        })
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Report::new(Errors::EmptyKey));
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        let log_record_pos = self.append_log_record(record)?;
        match self.index.put(key.to_vec(), log_record_pos) {
            true => Ok(()),
            false => Err(Report::new(Errors::IndexUpdateFail)),
        }
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Report::new(Errors::EmptyKey));
        }

        if self.index.get(key.to_vec()).is_none() {
            return Err(Report::new(Errors::KeyNotFound));
        };

        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(), // value can be anything
            record_type: LogRecordType::Deleted,
        };

        self.append_log_record(record)?;

        // update index
        if !self.index.delete(key.to_vec()) {
            return Err(Report::new(Errors::IndexUpdateFail));
        }
        Ok(())
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Report::new(Errors::EmptyKey));
        }

        // Check the existence of the key
        let pos = match self.index.get(key.to_vec()) {
            None => return Err(Report::new(Errors::KeyNotFound)),
            Some(x) => x,
        };

        self.at(&pos)
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.sync()?;
        for datafile in self.idle_file.values() {
            datafile.sync()?;
        }
        Ok(())
    }

    pub fn at(&self, pos: &LogRecordPos) -> Result<Bytes> {
        let log_record = match self.active_file.id() == pos.file_id {
            true => self.active_file.read(pos.offset)?,
            false => match self.idle_file.get(&pos.file_id) {
                None => return Err(Report::new(Errors::DatafileNotFound)),
                Some(x) => x.read(pos.offset)?,
            },
        };

        match log_record {
            // already check the existence of key, if we got a `None` from datafile (indicate an EOF),
            // it means datafiles must have been destroyed or something unexpected happened
            None => Err(Report::new(Errors::InternalError)),
            Some(record) => {
                match record.record_type {
                    LogRecordType::Normal => Ok(record.value.into()),
                    LogRecordType::Deleted => Err(Report::new(Errors::KeyNotFound)), // TODO: design decision, Result<Option<Bytes>> or Result<Bytes>
                }
            }
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
            self.idle_file
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
            offset: self.active_file.offset() - record_len, // offset indicate the start position
        })
    }
}

fn load_datafiles<P: AsRef<Path>>(path: P) -> Result<HashMap<u32, DataFile>> {
    let dir = fs::read_dir(&path).map_err(|_| Errors::ReadDbDirFail)?;
    let mut datafiles = HashMap::<u32, DataFile>::new();

    for entry in dir.flatten() {
        let fname = entry.file_name();

        if fname.to_str().unwrap().ends_with(DATAFILE_SUFFIX) {
            // example datafile name: `00001.data`
            let split: Vec<&str> = fname.to_str().unwrap().split('.').collect();
            let fid = split[0]
                .parse::<u32>()
                .change_context(Errors::DatafileCorrupted)?;
            datafiles.insert(fid, DataFile::new(&path, fid)?);
        }
    }

    Ok(datafiles)
}

#[cfg(test)]
mod tests {
    use crate::engine;
    use crate::errors::Errors;
    use crate::mock::engine_wrapper::{EngineWrapper, ENGINEDISTRIBUTOR};
    use bytes::Bytes;
    use std::fs;

    #[test]
    fn simple_put_and_get() {
        let db = engine!(["Hello", "World"]);
        assert_eq!(db.get("Hello".into()).unwrap(), Bytes::from("World"));
    }

    #[test]
    fn put_many_get_many() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        assert_eq!(engine.get("a".into()).unwrap(), "val-a");
        assert_eq!(engine.get("b".into()).unwrap(), "val-b");
        assert_eq!(engine.get("c".into()).unwrap(), "val-c");
    }

    #[test]
    fn overwrite_put() {
        let db = engine!(["Hello", "Hello"], ["Hello", "World"]);
        assert_eq!(db.get("Hello".into()).unwrap(), Bytes::from("World"));
    }

    #[test]
    fn get_non_exist_key() {
        let db = engine!();
        let x = db.get("Non Exist".into());
        assert_eq!(
            x.unwrap_err().downcast_ref::<Errors>().unwrap(),
            &Errors::KeyNotFound
        );
    }

    #[test]
    fn delete_exist() {
        let mut db = engine!(["Hello", "World"]);
        let report = db.delete("Hello".into());
        assert_eq!(report.unwrap(), ());
    }

    #[test]
    fn delete_non_exist() {
        let mut db = engine!(["Hello", "World"]);
        let report = db.delete("non_exist".into());
        assert_eq!(
            report.unwrap_err().downcast_ref::<Errors>().unwrap(),
            &Errors::KeyNotFound
        );
    }

    #[test]
    fn delete_non_exist_in_empty_db() {
        let mut db = engine!();
        let report = db.delete("non_exist".into());
        assert_eq!(
            report.unwrap_err().downcast_ref::<Errors>().unwrap(),
            &Errors::KeyNotFound,
        );
    }

    #[test]
    fn fulfill_one_datafile() {
        let mut db = EngineWrapper::new(
            crate::options::OptionsBuilder::default()
                .dir_path(ENGINEDISTRIBUTOR.path())
                .sync_writes(false) // performance consideration
                .data_file_size(8 * 1000) // 8KB per datafile
                .build()
                .unwrap(),
        );

        // fulfill the datafile
        for i in 0..500 {
            /*
            | 1B for Type  | 4B for CRC  | 1B for keysz |
            | 1B for valsz | 4B for key  | 5B for value |
            ==> 16B in total
            */
            let key = format!("{:04}", i);
            let val = format!("{:05}", i);
            db.put(key.into(), val.into()).unwrap();
        }
        db.sync().unwrap();

        let path = db.path().to_path_buf().canonicalize().unwrap();
        assert_eq!(
            fs::read_dir(&path)
                .unwrap()
                .flatten()
                .collect::<Vec<_>>()
                .len(),
            1
        );

        // This record should be in a new datafile
        db.put("Hello".into(), "World".into()).unwrap();
        db.sync().unwrap();
        assert_eq!(
            fs::read_dir(&path)
                .unwrap()
                .flatten()
                .collect::<Vec<_>>()
                .len(),
            2
        )
    }

    #[test]
    fn datafile_remaining_not_enough() {
        let mut db = EngineWrapper::new(
            crate::options::OptionsBuilder::default()
                .dir_path(ENGINEDISTRIBUTOR.path())
                .sync_writes(false) // performance consideration
                .data_file_size(8 * 1000) // 8KB per datafile
                .build()
                .unwrap(),
        );

        // not fulfill the datafile, but only 16 bytes available
        for i in 0..499 {
            /*
            | 1B for Type  | 4B for CRC  | 1B for keysz |
            | 1B for valsz | 4B for key  | 5B for value |
            ==> 16B in total
            */
            let key = format!("{:04}", i);
            let val = format!("{:05}", i);
            db.put(key.into(), val.into()).unwrap();
        }
        db.sync().unwrap();

        let path = db.path().to_path_buf().canonicalize().unwrap();
        assert_eq!(
            fs::read_dir(&path)
                .unwrap()
                .flatten()
                .collect::<Vec<_>>()
                .len(),
            1
        );

        // This record required 17 bytes, should be in a new datafile
        db.put("Hello".into(), "World".into()).unwrap();
        db.sync().unwrap();
        assert_eq!(
            fs::read_dir(&path)
                .unwrap()
                .flatten()
                .collect::<Vec<_>>()
                .len(),
            2
        )
    }

    #[test]
    fn reopen() {
        let mut db = EngineWrapper::new(
            crate::options::OptionsBuilder::default()
                .dir_path(ENGINEDISTRIBUTOR.path())
                .data_file_size(2 * 1000)
                .sync_writes(false)
                .build()
                .unwrap(),
        );

        for i in 0..1024 {
            /*
            | 1B for Type  | 4B for CRC  | 1B for keysz |
            | 1B for valsz | 4B for key  | 5B for value |
            ==> 16B in total
            */
            let key = format!("{:04}", i);
            let val = format!("{:05}", i);
            db.put(key.into(), val.into()).unwrap();
        }
        db.sync().unwrap();

        let db = db.reopen();
        assert_eq!(db.get("0000".into()).unwrap(), "00000");
        assert_eq!(db.get("1023".into()).unwrap(), "01023");
    }
}
