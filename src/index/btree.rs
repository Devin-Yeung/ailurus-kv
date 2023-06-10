use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;

pub struct BTree {
    /// A wrapper around a BTreeMap to provide concurrent access.
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            tree: Arc::new(RwLock::new(BTreeMap::new()))
        }
    }
}

impl Indexer for BTree {
    fn put(&mut self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut writer = self.tree.write();
        writer.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let reader = self.tree.read();
        reader.get(&key).copied()
    }

    fn delete(&mut self, key: Vec<u8>) -> bool {
        let mut writer = self.tree.write();
        writer.remove(&key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let mut b = BTree::new();
        assert!(b.put("".as_bytes().to_vec(), LogRecordPos { file_id: 42, offset: 42 }));
        assert!(b.put("".as_bytes().to_vec(), LogRecordPos { file_id: 1024, offset: 1024 }));
    }

    #[test]
    fn get() {
        let mut b = BTree::new();
        assert!(b.put("42".as_bytes().to_vec(), LogRecordPos { file_id: 42, offset: 42 }));
        assert!(b.put("1024".as_bytes().to_vec(), LogRecordPos { file_id: 1024, offset: 1024 }));

        assert_eq!(
            b.get("42".as_bytes().to_vec()).unwrap(),
            LogRecordPos { file_id: 42, offset: 42 });

        assert_eq!(
            b.get("1024".as_bytes().to_vec()).unwrap(),
            LogRecordPos { file_id: 1024, offset: 1024 });

        assert_eq!(
            b.get("".as_bytes().to_vec()),
            None);
    }

    #[test]
    fn delete() {
        let mut b = BTree::new();
        assert!(b.put("42".as_bytes().to_vec(), LogRecordPos { file_id: 42, offset: 42 }));
        assert!(b.put("1024".as_bytes().to_vec(), LogRecordPos { file_id: 1024, offset: 1024 }));

        b.delete("42".as_bytes().to_vec());
        assert_eq!(
            b.get("42".as_bytes().to_vec()),
            None);

        assert_eq!(
            b.get("1024".as_bytes().to_vec()).unwrap(),
            LogRecordPos { file_id: 1024, offset: 1024 });

        b.delete("1024".as_bytes().to_vec());
        assert_eq!(
            b.get("1024".as_bytes().to_vec()),
            None);
    }
}