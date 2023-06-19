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
    #[error("Key not found in storage")]
    KeyNotFound,
    #[error("Datafile not found in storage")]
    DatafileNotFound,
    #[error("Datafile size is too small")]
    DatafileSizeTooSmall,
    #[error("Datafile has been Corrupted")]
    DatafileCorrupted,
    #[error("Fail to update the memory index")]
    IndexUpdateFail,
    #[error("Fail to create database directory")]
    CreateDbDirFail,
    #[error("Read to create database directory")]
    ReadDbDirFail,
    #[error("Path to database is invalid")]
    InvalidDbPath,
    #[error("Something unexpected happen")]
    InternalError,
}

#[cfg(not(feature = "debug"))]
pub type Result<T> = std::result::Result<T, Errors>;
#[cfg(feature = "debug")]
pub type Result<T> = anyhow::Result<T, anyhow::Error>;
