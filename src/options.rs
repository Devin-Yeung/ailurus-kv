use crate::err;
use crate::errors::{Errors, Result};
use std::path::PathBuf;

#[non_exhaustive]
#[derive(Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}

#[derive(Clone)]
pub struct Options {
    /// location of database
    pub dir_path: PathBuf,
    /// Size of data file
    pub data_file_size: u64,
    /// Whether to sync in each writes
    pub sync_writes: bool,
    /// Indexing Method
    pub index_type: IndexType,
}

pub(crate) fn check_options(opts: &Options) -> Result<()> {
    if opts.dir_path.to_str().is_none() {
        return err!(Errors::InvalidDbPath);
    }

    if opts.data_file_size == 0 {
        return err!(Errors::DatafileSizeTooSmall);
    }

    Ok(())
}

pub struct IteratorOptions {
    pub filter: Box<dyn FnMut(&Vec<u8>) -> bool>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            filter: Box::new(|_| true),
            reverse: false,
        }
    }
}
