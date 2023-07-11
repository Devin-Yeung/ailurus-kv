use crate::engine::Engine;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;
use bytes::Bytes;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry {
    key: Bytes,
    value: Bytes,
}

pub struct EngineIterator<'a> {
    index_iterator: Box<dyn IndexIterator>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> EngineIterator {
        EngineIterator {
            index_iterator: self.index.iterator(options),
            engine: self,
        }
    }
}

impl EngineIterator<'_> {
    pub fn rewind(&mut self) {
        self.index_iterator.rewind();
    }

    pub fn seek(&mut self, key: Vec<u8>) {
        self.index_iterator.seek(key);
    }

    pub fn next(&mut self) -> Option<Entry> {
        if let Some((key, pos)) = self.index_iterator.next() {
            let value = self.engine.at(pos).unwrap();
            return Some(Entry {
                key: key.to_vec().into(),
                value,
            });
        }
        None
    }
}

impl<'a> std::iter::Iterator for EngineIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine;
    use crate::iterator::Entry;
    use crate::options::IteratorOptions;

    macro_rules! entry {
        ($key:expr, $val:expr) => {{
            $crate::iterator::Entry {
                key: ::bytes::Bytes::from($key),
                value: ::bytes::Bytes::from($val),
            }
        }};
    }

    #[test]
    fn rewind() {
        let engine = engine!(["Hello", "World"], ["World", "Hello"]);
        let mut iter = engine.iter(IteratorOptions::default());
        for _ in 0..2 {
            let _ = iter.next();
        }
        iter.rewind();
        assert_eq!(iter.next(), Some(entry!["Hello", "World"]));
    }

    #[test]
    fn std_iter() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let iterator = engine.iter(IteratorOptions::default());
        assert_eq!(
            iterator.into_iter().collect::<Vec<Entry>>(),
            vec![
                entry!["a", "val-a"],
                entry!["b", "val-b"],
                entry!["c", "val-c"],
            ]
        )
    }

    #[test]
    fn iter() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions::default());
        assert_eq!(iter.next(), Some(entry!["a", "val-a"]));
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
        assert_eq!(iter.next(), Some(entry!["c", "val-c"]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn reverse_iter() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        assert_eq!(iter.next(), Some(entry!["c", "val-c"]));
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
        assert_eq!(iter.next(), Some(entry!["a", "val-a"]));
    }

    #[test]
    fn reverse_rewind() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        assert_eq!(iter.next(), Some(entry!["c", "val-c"]));
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
        iter.rewind();
        assert_eq!(iter.next(), Some(entry!["c", "val-c"]));
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
        assert_eq!(iter.next(), Some(entry!["a", "val-a"]));
    }

    #[test]
    fn seek() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions::default());
        iter.seek("b".into());
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
    }

    #[test]
    fn reverse_seek() {
        let engine = engine!(["a", "val-a"], ["b", "val-b"], ["c", "val-c"]);
        let mut iter = engine.iter(IteratorOptions {
            filter: Box::new(|_| true),
            reverse: true,
        });
        iter.seek("b".into());
        assert_eq!(iter.next(), Some(entry!["b", "val-b"]));
        assert_eq!(iter.next(), Some(entry!["a", "val-a"]));
    }
}
