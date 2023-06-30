use crate::engine::Engine;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;
use bytes::Bytes;

pub struct Iterator<'a> {
    index_iterator: Box<dyn IndexIterator>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iterator: self.index.iterator(options),
            engine: self,
        }
    }
}

impl Iterator<'_> {
    pub fn rewind(&mut self) {
        self.index_iterator.rewind();
    }

    pub fn seek(&mut self, key: Vec<u8>) {
        self.index_iterator.seek(key);
    }

    pub fn next(&mut self) -> Option<(Bytes, Bytes)> {
        if let Some(item) = self.index_iterator.next() {
            let value = self.engine.at(&item.1).unwrap();
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}
