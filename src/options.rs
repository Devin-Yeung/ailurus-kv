use crate::errors::{Errors, Result};
use derive_builder::Builder;
use error_stack::Report;
use std::path::PathBuf;

#[non_exhaustive]
#[derive(Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}

#[derive(Clone, Builder)]
pub struct Options {
    /// location of database
    pub dir_path: PathBuf,
    /// Size of data file
    #[builder(default = "8 * 1024 * 1024")]
    pub data_file_size: u64,
    /// Whether to sync in each writes
    #[builder(default = "false")]
    pub sync_writes: bool,
    /// Indexing Method
    #[builder(default = "crate::options::IndexType::BTree")]
    pub index_type: IndexType,
}

pub(crate) fn check_options(opts: &Options) -> Result<()> {
    if opts.dir_path.to_str().is_none() {
        return Err(Report::new(Errors::InvalidDbPath));
    }

    if opts.data_file_size == 0 {
        return Err(Report::new(Errors::DatafileSizeTooSmall));
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

#[derive(Clone, Builder)]
pub struct WriteBatchOptions {
    /// Size of batch
    #[builder(default = "8 * 1024 * 1024")]
    pub batch_size: u32,
    /// Whether to sync when commit happens
    #[builder(default = "true")]
    pub sync_on_commit: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        WriteBatchOptionsBuilder::default().build().unwrap()
    }
}
