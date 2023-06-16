use crate::data::log_record::{LogRecord, LogRecordType};
use crate::errors::{Errors, Result};
use crate::fio;
use crate::fio::io_manager;
use bytes::{Buf, Bytes, BytesMut};
use log::error;
use prost::{decode_length_delimiter, length_delimiter_len};
use std::path::Path;

pub const DATAFILE_SUFFIX: &str = ".data";
pub const INITIAL_DATAFILE_ID: u32 = 0;

pub struct DataFile {
    id: u32,
    offset: u64,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, id: u32) -> Result<DataFile> {
        let fname = path.as_ref().to_path_buf();
        let fname = match fname.is_dir() {
            true => {
                let datafile = std::format!("{:09}{}", id, DATAFILE_SUFFIX);
                fname.join(datafile)
            }
            false => return Err(Errors::DatafileNotFound),
        };

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
                return Err(Errors::FailToOpenFile);
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
        let mut header = BytesMut::zeroed(
            std::mem::size_of::<u32>() /* size of CRC */
                + std::mem::size_of::<u8>() /* size of Type */
                + length_delimiter_len(u32::MAX as usize) * 2, /* variable key size and value size */
        );

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
            return Err(Errors::DatafileCorrupted);
        }

        Ok(Some(log_record))
    }
}
