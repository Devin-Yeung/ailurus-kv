use crate::err;
use crate::errors::{Errors, Result};
use crate::fio::IOManager;
use log::error;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt};
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
                err!(Errors::FailToOpenFile)
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
        })?;
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut writer = self.fd.write();
        return match writer.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("{}", e);
                err!(Errors::FailToWriteToFile)
            }
        };
    }

    fn sync(&self) -> Result<()> {
        let reader = self.fd.read();
        if let Err(e) = reader.sync_all() {
            error!("{}", e);
            return err!(Errors::FailToSyncFile);
        }
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        let size = self
            .fd
            .read()
            .metadata()
            .map_err(|e| {
                error!("{}", e);
                Errors::InternalError
            })?
            .size();
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_file() -> PathBuf {
        if !Path::new("tmp").is_dir() {
            let _ = fs::create_dir("tmp");
        }

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
        #[cfg(not(feature = "debug"))]
        assert_eq!(result, Ok(()));
        #[cfg(feature = "debug")]
        assert_eq!(result.unwrap(), ());
        assert_eq!(buf, data);

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_write_success() {
        let file_path = tmp_file();
        let mut file = FileIO::new(&file_path).unwrap();
        let data = b"Hello, World!";

        let result = file.write(data);
        #[cfg(not(feature = "debug"))]
        assert_eq!(result, Ok(data.len()));
        #[cfg(feature = "debug")]
        assert_eq!(result.unwrap(), data.len());

        let mut buf = vec![0; data.len()];
        let _ = file.sync();
        let _ = file.read(&mut buf, 0);
        assert_eq!(buf, data);

        fs::remove_file(&file_path).unwrap();
    }
}
