mod btree;
use crate::data::data_file::DataFile;
use crate::data::log_record::LogRecordPos;
use crate::errors::Result;
use crate::index::btree::BTree;
use crate::options::{IndexType, IteratorOptions};
use bytes::Bytes;

pub trait Indexer {
    /// Inserts a key-value pair into the index.
    ///
    /// # Arguments
    ///
    /// * `key` - A vector of bytes representing the key.
    /// * `pos` - The position of the log record in the index.
    ///
    /// # Returns
    ///
    /// Returns `true` if the insertion was successful, `false` otherwise.
    fn put(&mut self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// Retrieves the position of a key in the index, if it exists.
    ///
    /// # Arguments
    ///
    /// * `key` - A vector of bytes representing the key.
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing the position of the key if it exists in the index,
    /// or `None` if the key is not found.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Removes a key-value pair from the index.
    ///
    /// # Arguments
    ///
    /// * `key` - A vector of bytes representing the key.
    ///
    /// # Returns
    ///
    /// Returns `true` if the deletion was successful, `false` otherwise.
    fn delete(&mut self, key: Vec<u8>) -> bool;

    /// Returns an iterator over the index.
    ///
    /// This method returns an iterator that implements the `IndexIterator` trait.
    ///
    /// # Arguments
    ///
    /// * `options` - An `IteratorOptions` struct specifying the options for the iterator.
    ///
    /// # Returns
    ///
    /// A box to an object that implements the `IndexIterator` trait.
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;

    /// Retrieve the keys stored in the index.
    ///
    /// This method returns a vector of `Bytes` representing the keys stored in the index.
    /// It retrieves the keys in their current order and returns them as a result.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<Bytes>)`: A vector of `Bytes` representing the keys if the operation is successful.
    /// * `Err(Error)`: An error variant if there is a failure in retrieving the keys.
    fn keys(&self) -> Result<Vec<Bytes>>;
}

pub trait Indexable {
    fn index<'a, D>(datafiles: D) -> Result<Box<dyn Indexer>>
    where
        D: IntoIterator<Item = &'a DataFile>,
        Self: Sized;
}

pub trait IndexIterator {
    /// Rewinds the iterator to the beginning.
    fn rewind(&mut self);

    /// Seeks the iterator to a specific key.
    /// If key not found, seeks the iterator to the key *greater* than the given key,
    /// the order is define in [IteratorOptions]
    ///
    /// [IteratorOptions]: crate::options::IteratorOptions
    ///
    /// # Arguments
    ///
    /// * `key` - A vector of bytes representing the key to seek.
    fn seek(&mut self, key: Vec<u8>);

    /// Retrieves the next key-value pair from the iterator.
    ///
    /// Returns `Some` with a reference to the key and value if there is a next element,
    /// or `None` if the iterator has reached the end.
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

pub fn indexer<'a, D>(datafiles: D, index_type: &IndexType) -> Result<Box<dyn Indexer>>
where
    D: IntoIterator<Item = &'a DataFile>,
{
    match index_type {
        IndexType::BTree => Ok(BTree::index(datafiles)?),
        IndexType::SkipList => todo!(),
    }
}
