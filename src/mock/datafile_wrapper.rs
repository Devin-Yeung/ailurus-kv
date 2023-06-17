use crate::data::data_file::{DataFile, DATAFILE_SUFFIX};
use lazy_static::lazy_static;
use std::fs::{remove_file, OpenOptions};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

lazy_static! {
    static ref DATAFILEDISTRIBUTOR: DataFileDistributor = DataFileDistributor::new();
}

pub struct DataFileDistributor {
    count: Mutex<u32>,
}

impl DataFileDistributor {
    pub(crate) fn new() -> Self {
        DataFileDistributor {
            count: Mutex::new(0),
        }
    }

    pub(crate) fn id(&self) -> u32 {
        let mut guard = self.count.lock().unwrap();
        let id = *guard;
        *guard += 1;
        id
    }
}

pub struct DataFileWrapper {
    datafile: DataFile,
    path: PathBuf,
}

impl DataFileWrapper {
    pub(crate) fn new<P: AsRef<Path>>(path: P, id: u32) -> Self {
        let path = path.as_ref().to_path_buf();

        let path = match path.is_dir() {
            true => {
                let datafile = std::format!("{:09}{}", id, DATAFILE_SUFFIX);
                path.join(datafile)
            }
            false => panic!("Invalid path"),
        };

        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap()
            .sync_all();

        let datafile = DataFile::new(path.parent().unwrap(), id).unwrap();

        DataFileWrapper { datafile, path }
    }
}

impl Default for DataFileWrapper {
    fn default() -> Self {
        DataFileWrapper::new(Path::new("tmp"), DATAFILEDISTRIBUTOR.id())
    }
}

impl Drop for DataFileWrapper {
    fn drop(&mut self) {
        remove_file(&self.path).unwrap();
    }
}

impl Deref for DataFileWrapper {
    type Target = DataFile;

    fn deref(&self) -> &Self::Target {
        &self.datafile
    }
}

impl DerefMut for DataFileWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.datafile
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let df = DataFileWrapper::default();
        let path = df.path.to_owned();
        assert!(path.is_file());
        drop(df);
        assert!(!path.is_file());
    }
}
