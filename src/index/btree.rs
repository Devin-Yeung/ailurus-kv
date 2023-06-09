use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;

pub struct BTree {
    /// A wrapper around a BTreeMap to provide concurrent access.
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        todo!()
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        todo!()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        todo!()
    }
}