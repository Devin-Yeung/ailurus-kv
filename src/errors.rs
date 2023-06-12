use thiserror::Error;

#[derive(Error, Debug, Eq, PartialEq)]
pub enum Errors {
    #[error("Fail to open file")]
    FailToOpenFile,
    #[error("Fail to read from file")]
    FailToReadFromFile,
    #[error("Fail to write to file")]
    FailToWriteToFile,
    #[error("Fail to sync file")]
    FailToSyncFile,
    #[error("Key is empty")]
    EmptyKey,
    #[error("Fail to update the memory index")]
    IndexUpdateFail,
}

pub type Result<T> = std::result::Result<T, Errors>;
