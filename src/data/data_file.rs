use crate::data::log_record::LogRecord;
use crate::errors::{Errors, Result};
use crate::fio::io_manager;
use crate::{err, fio};
use bytes::{Buf, BytesMut};
use log::error;
use prost::{decode_length_delimiter, length_delimiter_len};
use std::fmt::{Debug, Formatter};
use std::path::Path;

pub const DATAFILE_SUFFIX: &str = ".data";
pub const INITIAL_DATAFILE_ID: u32 = 0;

pub struct DataFile {
    id: u32,
    offset: u64,
    io_manager: Box<dyn fio::IOManager>,
}

impl Debug for DataFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("id", &self.id)
            .field("offset", &self.offset)
            .finish()
    }
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, id: u32) -> Result<DataFile> {
        let fname = path.as_ref().to_path_buf();
        let fname = match fname.is_dir() {
            true => {
                let datafile = std::format!("{:09}{}", id, DATAFILE_SUFFIX);
                fname.join(datafile)
            }
            false => {
                error!("Database dir {:?} Not exist", fname);
                return err!(Errors::DatafileNotFound);
            }
        };

        // Check the existence of Datafile, if not exist, create one
        if !fname.is_file() {
            let _ = std::fs::File::create(&fname).map_err(|e| {
                error!("{}", e);
                return Errors::CreateDbFileFail;
            })?;
        }

        let offset = match std::fs::File::open(&fname) {
            Ok(f) => f
                .metadata()
                .map_err(|e| {
                    error!("{}", e);
                    Errors::InternalError
                })?
                .len(),
            Err(e) => {
                error!("{}", e);
                return err!(Errors::FailToOpenFile);
            }
        };

        let io_manager = Box::new(io_manager(fname)?);

        Ok(DataFile {
            id,
            offset,
            io_manager,
        })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let bytes_read = self.io_manager.write(buf)?;
        self.offset += bytes_read as u64;
        Ok(bytes_read)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn read(&self, offset: u64) -> Result<Option<LogRecord>> {
        // TODO: design decision, return Err(EOF) or Ok(None) when EOF reached
        // Layout of LogRecord
        // +-------+--------+-----------+-------------+-----------+-------------+
        // |  4B   |   1B   |    mut    |     mut     |    mut    |     mut     |
        // +-------+--------+-----------+-------------+-----------+-------------+
        // |  CRC  |  Type  |  KeySize  |  ValueSize  |    Key    |    Value    |
        // +-------+--------+-----------+-------------+-----------+-------------+

        let max_header_sz = std::mem::size_of::<u32>() /* size of CRC */
            + std::mem::size_of::<u8>() /* size of Type */
            + length_delimiter_len(u32::MAX as usize) * 2 /* variable key size and value size */;

        // if remaining bytes is zero, means EOF reached
        let mut header = match (self.io_manager.size()? - offset) as usize {
            remaining if remaining == 0 => return Ok(None),
            remaining if remaining < max_header_sz => BytesMut::zeroed(remaining),
            remaining if remaining > max_header_sz => BytesMut::zeroed(max_header_sz),
            _ => unreachable!(),
        };

        self.io_manager.read(&mut header, offset)?;

        let crc = header.get_u32();
        let record_type = header.get_u8();

        // bytes will advance automatically
        let key_size =
            decode_length_delimiter(&mut header).map_err(|_| Errors::DatafileCorrupted)?;
        let value_size =
            decode_length_delimiter(&mut header).map_err(|_| Errors::DatafileCorrupted)?;

        // EOF reached
        if key_size == 0 && value_size == 0 {
            return Ok(None);
        }

        let header_size = std::mem::size_of::<u32>() /* size of CRC */
            + std::mem::size_of::<u8>() /* size of Type */
            + length_delimiter_len(key_size) /* length of key size */
            + length_delimiter_len(value_size) /* length of key size */;

        let mut kv_buf = BytesMut::zeroed(key_size + value_size);
        self.io_manager
            .read(&mut kv_buf, offset + header_size as u64)?;

        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len()).unwrap().to_vec(),
            record_type: record_type.try_into()?,
        };

        if crc != log_record.crc() {
            error!("CRC does not match");
            return err!(Errors::DatafileCorrupted);
        }

        Ok(Some(log_record))
    }
}

#[cfg(test)]
mod tests {
    use crate::data::log_record::{LogRecord, LogRecordType};
    use crate::mock::datafile_wrapper::DataFileWrapper;

    #[test]
    fn get_one_key() {
        let mut df = DataFileWrapper::default();
        let record = LogRecord {
            key: "ailurus-kv".as_bytes().to_vec(),
            value: "is Awesome".as_bytes().to_vec(),
            record_type: LogRecordType::Normal,
        };
        df.write(&record.encode()).unwrap();
        assert_eq!(df.read(0).unwrap().unwrap(), record);
    }
}
