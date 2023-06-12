use std::path::PathBuf;

pub struct Options {
    /// location of database
    pub dir_path: PathBuf,
    /// Size of data file
    pub data_file_size: u64,
    /// Whether to sync in each writes
    pub sync_writes: bool,
}