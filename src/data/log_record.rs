use crate::errors::Errors;
use bytes::{Buf, BufMut, BytesMut};
use prost::encode_length_delimiter;

#[non_exhaustive]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum LogRecordType {
    Normal,
    Deleted,
}

#[derive(Eq, PartialEq, Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl TryFrom<u8> for LogRecordType {
    type Error = Errors;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(LogRecordType::Normal),
            2 => Ok(LogRecordType::Deleted),
            _ => Err(Errors::DatafileCorrupted),
        }
    }
}

impl From<LogRecordType> for u8 {
    fn from(value: LogRecordType) -> Self {
        match value {
            LogRecordType::Normal => 1,
            LogRecordType::Deleted => 2,
        }
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
    // +-------+--------+-----------+-------------+-----------+-------------+
    // |  4B   |   1B   |    mut    |     mut     |    mut    |     mut     |
    // +-------+--------+-----------+-------------+-----------+-------------+
    // |  CRC  |  Type  |  KeySize  |  ValueSize  |    Key    |    Value    |
    // +-------+--------+-----------+-------------+-----------+-------------+
    ///
    /// # Returns
    ///
    /// Returns a `Vec<u8>` containing the encoded representation of the `LogRecord`.
    ///
    pub fn encode(&self) -> Vec<u8> {
        // Layout of LogRecord
        // +-------+--------+-----------+-------------+-----------+-------------+
        // |  4B   |   1B   |    mut    |     mut     |    mut    |     mut     |
        // +-------+--------+-----------+-------------+-----------+-------------+
        // |  CRC  |  Type  |  KeySize  |  ValueSize  |    Key    |    Value    |
        // +-------+--------+-----------+-------------+-----------+-------------+
        let buf = self.compress();

        // CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let mut crc = BytesMut::new();
        crc.put_u32(hasher.finalize());

        // chain the crc with data
        let len = buf.len() + crc.len();
        let mut combined = crc.chain(buf);

        combined.copy_to_bytes(len).to_vec()
    }

    fn compress(&self) -> BytesMut {
        // Compress the LogRecord to following structure, preparing for the encoding step
        // +--------+-----------+-------------+-----------+-------------+
        // |   1B   |    mut    |     mut     |    mut    |     mut     |
        // +--------+-----------+-------------+-----------+-------------+
        // |  Type  |  KeySize  |  ValueSize  |    Key    |    Value    |
        // +--------+-----------+-------------+-----------+-------------+
        // (Difference between the encoding result is CRC field is missing)
        let mut buf = BytesMut::new();
        // encode the record type
        buf.put_u8(self.record_type.into());
        // encode the key size and value size
        encode_length_delimiter(self.key.len(), &mut buf).unwrap(); // TODO: deal with the error
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        // encode key and value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        buf
    }

    /// Return the size of the `LogRecord`
    ///
    /// # Notes
    ///
    /// The time complexity of the call does *not* guarantee O(1) due to the implementation
    pub fn size(&self) -> u64 {
        // TODO: [perf] improve the performance
        self.encode().len() as u64
    }

    pub fn crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.compress());
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_record_compression() {
        let record = LogRecord {
            key: "ailurus-kv".as_bytes().to_vec(), // 10 bytes
            value: "is Awesome".as_bytes().to_vec(),
            record_type: LogRecordType::Normal,
        };

        let expected = [
            1_u8,  /* record type */
            10_u8, /* key size is 10B */
            10_u8, /* value size is 10B */
            b'a', b'i', b'l', b'u', b'r', b'u', b's', b'-', b'k',
            b'v', /* key:   ailurus-kv */
            b'i', b's', b' ', b'A', b'w', b'e', b's', b'o', b'm',
            b'e', /* value: is Awesome */
        ];

        assert_eq!(record.compress()[..], expected);
    }

    #[test]
    fn empty_record_compression() {
        let record = LogRecord {
            key: vec![], // 10 bytes
            value: vec![],
            record_type: LogRecordType::Normal,
        };

        let expected = [
            1_u8, /* record type */
            0_u8, /* key size is 0B */
            0_u8, /* value size is 0B */
                  /* key and value is empty */
        ];

        assert_eq!(record.compress()[..], expected);
    }

    #[test]
    fn simple_record_encoding() {
        let record = LogRecord {
            key: "ailurus-kv".as_bytes().to_vec(), // 10 bytes
            value: "is Awesome".as_bytes().to_vec(),
            record_type: LogRecordType::Normal,
        };

        let expected = [
            0x04, 0xcd, 0x63, 0xdd,  /* Manually calculated CRC */
            1_u8,  /* record type */
            10_u8, /* key size is 10B */
            10_u8, /* value size is 10B */
            b'a', b'i', b'l', b'u', b'r', b'u', b's', b'-', b'k',
            b'v', /* key:   ailurus-kv */
            b'i', b's', b' ', b'A', b'w', b'e', b's', b'o', b'm',
            b'e', /* value: is Awesome */
        ];

        assert_eq!(record.encode()[..], expected);
    }

    #[test]
    fn simple_crc() {
        let record = LogRecord {
            key: "ailurus-kv".as_bytes().to_vec(), // 10 bytes
            value: "is Awesome".as_bytes().to_vec(),
            record_type: LogRecordType::Normal,
        };

        assert_eq!(record.crc(), 0x04cd63dd_u32);
    }
}
