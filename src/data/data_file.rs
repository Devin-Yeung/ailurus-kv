use std::path::Path;
use crate::fio;
use crate::errors::Result;

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
}