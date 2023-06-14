use crate::data::log_record::LogRecord;
use crate::errors::{Errors, Result};
use crate::fio;
use std::path::Path;
use bytes::{Buf, Bytes, BytesMut};
use crate::fio::io_manager;

pub const DATAFILE_SUFFIX: &str = ".data";
pub const INITIAL_DATAFILE_ID: u32 = 0;

pub struct DataFile {
    id: u32,
    offset: u64,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, id: u32) -> Result<DataFile> {
        let mut fname = path.as_ref().to_path_buf();
        let fname = match fname.is_dir() {
            true => {
                let datafile = std::format!("{:09}{}", id, DATAFILE_SUFFIX);
                fname.join(datafile)
            }
            false => return Err(Errors::DatafileNotFound)
        };

        let offset = match std::fs::File::open(fname) {
            Ok(f) => {
                // TODO: docs didn't tell me what's the possible error, let me unwrap it
                f.metadata().unwrap().len()
            }
            Err(_) => {
                // TODO: log the error
                return Err(Errors::FailToOpenFile);
            }
        };

        let io_manager = Box::new(io_manager(path)?);

        Ok(
            DataFile {
                id,
                offset,
                io_manager,
            }
        )
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    pub fn read(&self, offset: u64) -> Result<Option<LogRecord>> {
        // TODO: design decision, return Err(EOF) or Ok(None) when EOF reached
        todo!()
    }
}
