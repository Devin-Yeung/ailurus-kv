mod fio;

use std::path::Path;
use crate::errors::Result;
use crate::fio::fio::FileIO;

pub trait IOManager: Send + Sync {
    /// Reads data from the underlying storage into the provided buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - A slice representing the buffer where the read data will be stored.
    /// * `offset` - The offset within the storage from where to start reading.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the result of the read operation. If the read is successful,
    /// it returns `Ok(bytes_read)` where `bytes_read` is the number of bytes read into the buffer.
    /// If an error occurs during the read operation, it returns `Err(error)` with an associated error value.
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Writes data from the provided buffer to the underlying storage.
    ///
    /// # Arguments
    ///
    /// * `buf` - A mutable slice representing the buffer containing the data to be written.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the result of the write operation. If the write is successful,
    /// it returns `Ok(bytes_written)` where `bytes_written` is the number of bytes written from the buffer.
    /// If an error occurs during the write operation, it returns `Err(error)` with an associated error value.
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// Ensures that all previous write operations are persisted to the underlying storage.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the result of the synchronization operation. If the synchronization
    /// is successful, it returns `Ok(())`. If an error occurs during the synchronization, it returns `Err(error)`
    /// with an associated error value.
    fn sync(&self) -> Result<()>;
}

pub fn io_manager<'a, 'b, P: AsRef<Path> + 'a>(path: P) -> Result<impl IOManager + 'b> {
    FileIO::new(path)
}