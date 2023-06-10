use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("Fail to open file")]
    FailToOpenFile,
    #[error("Fail to read from file")]
    FailToReadFromFile,
    #[error("Fail to write to file")]
    FailToWriteToFile,
    #[error("Fail to sync file")]
    FailToSyncFile,
}

pub type Result<T> = std::result::Result<T, Errors>;
