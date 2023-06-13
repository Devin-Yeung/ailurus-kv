use std::path::PathBuf;
use crate::errors::{Errors, Result};

#[non_exhaustive]
pub enum IndexType {
    BTree,
    SkipList,
}

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
        return Err(Errors::InvalidDbPath);
    }

    if opts.data_file_size <= 0 {
        return Err(Errors::DatafileSizeTooSmall);
    }

    Ok(())
}
