use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecordPos, LogRecordType};
use crate::errors::Result;
use crate::index::{IndexIterator, Indexable, Indexer};
use crate::options::IteratorOptions;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BTree {
    /// A wrapper around a BTreeMap to provide concurrent access.
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexable for BTree {
    fn index<'a, D>(datafiles: D) -> Result<Box<dyn Indexer>>
    where
        D: IntoIterator<Item = &'a DataFile>,
        Self: Sized,
    {
        // return a btree index using the given Datafile
        let mut index = BTree::new();
        for datafile in datafiles {
            let mut offset = 0;
            loop {
                let log_record = match datafile.read(offset)? {
                    None => break,
                    Some(record) => record,
                };

                let pos = LogRecordPos {
                    file_id: datafile.id(),
                    offset,
                };

                match log_record.record_type {
                    LogRecordType::Normal => index.put(log_record.key.to_vec(), pos),
                    LogRecordType::Deleted => index.delete(log_record.key.to_vec()),
                };

                offset += log_record.size(); // TODO: [perf]: size() call is costly
            }
        }
        Ok(Box::new(index))
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

    fn iterator<'a>(&self, options: IteratorOptions) -> &'a dyn IndexIterator {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let mut b = BTree::new();
        assert!(b.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 42,
                offset: 42,
            },
        ));
        assert!(b.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1024,
                offset: 1024,
            },
        ));
    }

    #[test]
    fn get() {
        let mut b = BTree::new();
        assert!(b.put(
            "42".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 42,
                offset: 42,
            },
        ));
        assert!(b.put(
            "1024".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1024,
                offset: 1024,
            },
        ));

        assert_eq!(
            b.get("42".as_bytes().to_vec()).unwrap(),
            LogRecordPos {
                file_id: 42,
                offset: 42,
            }
        );

        assert_eq!(
            b.get("1024".as_bytes().to_vec()).unwrap(),
            LogRecordPos {
                file_id: 1024,
                offset: 1024,
            }
        );

        assert_eq!(b.get("".as_bytes().to_vec()), None);
    }

    #[test]
    fn delete() {
        let mut b = BTree::new();
        assert!(b.put(
            "42".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 42,
                offset: 42,
            },
        ));
        assert!(b.put(
            "1024".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1024,
                offset: 1024,
            },
        ));

        b.delete("42".as_bytes().to_vec());
        assert_eq!(b.get("42".as_bytes().to_vec()), None);

        assert_eq!(
            b.get("1024".as_bytes().to_vec()).unwrap(),
            LogRecordPos {
                file_id: 1024,
                offset: 1024,
            }
        );

        b.delete("1024".as_bytes().to_vec());
        assert_eq!(b.get("1024".as_bytes().to_vec()), None);
    }
}
