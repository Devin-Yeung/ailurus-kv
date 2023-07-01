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

#[cfg(test)]
mod tests {
    use crate::engine;
    use crate::options::IteratorOptions;

    #[test]
    fn rewind() {
        let engine = engine!(["Hello", "World"], ["World", "Hello"]);
        let mut iter = engine.iter(IteratorOptions::default());
        for _ in 0..2 {
            let _ = iter.next();
        }
        iter.rewind();
        assert_eq!(iter.next().unwrap(), ("Hello".into(), "World".into()))
    }

    #[test]
    fn iter() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions::default());
        assert_eq!(iter.next().unwrap(), ("a".into(), "val-a".into()));
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
        assert_eq!(iter.next().unwrap(), ("c".into(), "val-c".into()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn reverse_iter() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        assert_eq!(iter.next().unwrap(), ("c".into(), "val-c".into()));
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
        assert_eq!(iter.next().unwrap(), ("a".into(), "val-a".into()));
    }

    #[test]
    fn reverse_rewind() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        assert_eq!(iter.next().unwrap(), ("c".into(), "val-c".into()));
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
        iter.rewind();
        assert_eq!(iter.next().unwrap(), ("c".into(), "val-c".into()));
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
        assert_eq!(iter.next().unwrap(), ("a".into(), "val-a".into()));
    }

    #[test]
    fn seek() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions::default());
        iter.seek("b".into());
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
    }

    #[test]
    fn reverse_seek() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        iter.seek("b".into());
        assert_eq!(iter.next().unwrap(), ("b".into(), "val-b".into()));
        assert_eq!(iter.next().unwrap(), ("a".into(), "val-a".into()));
    }
}
