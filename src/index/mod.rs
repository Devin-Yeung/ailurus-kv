mod btree;
use crate::data::data_file::DataFile;
use crate::data::log_record::LogRecordPos;
use crate::index::btree::BTree;
use crate::options::IndexType;

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
}

pub fn indexer<'a, D>(datafiles: D, index_type: &IndexType) -> Box<dyn Indexer>
where
    D: IntoIterator<Item = &'a DataFile>,
{
    return match index_type {
        IndexType::BTree => Box::new(BTree::from(datafiles)),
        IndexType::SkipList => todo!(),
    };
}
