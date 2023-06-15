use crate::errors::{Errors, Result};
use bytes::Bytes;

#[non_exhaustive]
pub enum LogRecordType {
    Normal,
    Deleted,
}

pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl TryFrom<u8> for LogRecordType {
    type Error = Errors;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        return match value {
            1 => Ok(LogRecordType::Normal),
            2 => Ok(LogRecordType::Deleted),
            _ => Err(Errors::DatafileCorrupted),
        };
    }
}

impl Into<Bytes> for LogRecord {
    fn into(self) -> Bytes {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRecordPos {
    /// The ID of the log file where the record is located.
    pub(crate) file_id: u32,
    /// The byte offset within the log file where the record starts.
    pub(crate) offset: u64,
}

impl LogRecord {
    /// Encodes the `LogRecord` into a byte vector.
    /// TODO: bitcask layout ascii art
    ///
    /// # Returns
    ///
    /// Returns a `Vec<u8>` containing the encoded representation of the `LogRecord`.
    ///
    pub fn encode(&self) -> Vec<u8> {
        unimplemented!()
    }

    /// Return the size of the `LogRecord`
    ///
    /// # Notes
    ///
    /// The time complexity of the call does *not* guarantee O(1) due to the implementation
    pub fn size(&self) -> u64 {
        todo!()
    }

    pub fn crc(&self) -> u32 {
        todo!()
    }
}
