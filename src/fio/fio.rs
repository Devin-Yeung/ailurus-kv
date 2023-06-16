use crate::errors::{Errors, Result};
use crate::fio::IOManager;
use log::error;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

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
            .open(path)
        {
            Ok(file) => Ok(FileIO {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                error!("{}", e);
                Err(Errors::FailToOpenFile)
            }
        };
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let reader = self.fd.read();
        reader.read_exact_at(buf, offset).map_err(|e| {
            error!("{}", e);
            Errors::FailToReadFromFile
        })
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut writer = self.fd.write();
        return match writer.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("{}", e);
                Err(Errors::FailToWriteToFile)
            }
        };
    }

    fn sync(&self) -> Result<()> {
        let reader = self.fd.read();
        if let Err(e) = reader.sync_all() {
            error!("{}", e);
            return Err(Errors::FailToSyncFile);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_file() -> PathBuf {
        let _ = fs::create_dir("tmp");

        let temp_file = tempfile::Builder::new()
            .prefix("ailurus_kv")
            .tempfile_in("tmp")
            .unwrap();
        temp_file.path().to_owned()
    }

    #[test]
    fn test_read_success() {
        let file_path = tmp_file();
        let mut file = FileIO::new(&file_path).unwrap();
        let data = b"Hello, World!";
        file.write(data).unwrap();

        let mut buf = vec![0; data.len()];
        let result = file.read(&mut buf, 0);
        assert_eq!(result, Ok(()));
        assert_eq!(buf, data);

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_write_success() {
        let file_path = tmp_file();
        let mut file = FileIO::new(&file_path).unwrap();
        let data = b"Hello, World!";

        let result = file.write(data);
        assert_eq!(result, Ok(data.len()));

        let mut buf = vec![0; data.len()];
        let _ = file.sync();
        let _ = file.read(&mut buf, 0);
        assert_eq!(buf, data);

        fs::remove_file(&file_path).unwrap();
    }
}
