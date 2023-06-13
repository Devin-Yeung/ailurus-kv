use crate::data::log_record::LogRecord;
use crate::errors::Result;
use crate::fio;
use std::path::Path;

pub const DATAFILE_SUFFIX: &str = ".data";
pub const INITIAL_DATAFILE_ID: u32 = 0;

pub struct DataFile {
    id: u32,
    offset: u64,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, id: u32) -> Result<DataFile> {
        todo!()
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

    pub fn read(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }
}
