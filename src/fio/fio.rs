use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::fio::IOManager;
use crate::errors::{Errors, Result};

pub struct FileIO {
    /// file io wrapper
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        return match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path) {
            Ok(file) => {
                Ok(FileIO { fd: Arc::new(RwLock::new(file)) })
            }
            Err(e) => {
                Err(Errors::FailToOpenFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let reader = self.fd.read();
        return match reader.read_at(buf, offset) {
            Ok(n) => { Ok(n) }
            Err(_) => { Err(Errors::FailToReadFromFile) }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut writer = self.fd.write();
        return match writer.write(buf) {
            Ok(n) => { Ok(n) }
            Err(_) => { Err(Errors::FailToWriteToFile) }
        };
    }

    fn sync(&self) -> Result<()> {
        let reader = self.fd.read();
        if let Err(_) = reader.sync_all() {
            return Err(Errors::FailToSyncFile);
        }
        Ok(())
    }
}