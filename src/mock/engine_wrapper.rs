use crate::engine::Engine;
use crate::options::{IndexType, Options};
use lazy_static::lazy_static;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Mutex;

const PREFIX: &str = "tmp/engine";

lazy_static! {
    static ref ENGINEDISTRIBUTOR: EngineDistributor = EngineDistributor::new();
}
pub struct Inner {
    id: u32,
    count: u32,
}

pub struct EngineDistributor {
    inner: Mutex<Inner>,
}

impl EngineDistributor {
    pub fn new() -> Self {
        EngineDistributor {
            inner: Mutex::new(Inner { id: 0, count: 0 }),
        }
    }

    pub fn path(&self) -> PathBuf {
        let mut path = PathBuf::from(PREFIX);
        let mut inner = self.inner.lock().unwrap();
        path.push(inner.id.to_string());
        inner.id += 1;
        inner.count += 1;
        drop(inner);
        path
    }

    pub fn drop(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.count -= 1;
        if inner.count == 0 {
            fs::remove_dir(PREFIX).unwrap();
        }
        drop(inner);
    }
}

pub struct EngineWrapper {
    engine: Engine,
    path: PathBuf,
}

impl EngineWrapper {
    pub(crate) fn new(opts: crate::options::Options) -> EngineWrapper {
        // create dir if not exist
        if !opts.dir_path.is_dir() {
            fs::create_dir_all(&opts.dir_path).unwrap()
        }

        EngineWrapper {
            path: opts.dir_path.to_owned(),
            engine: Engine::new(opts).unwrap(),
        }
    }

    pub(crate) fn reopen(mut self) -> EngineWrapper {
        // FIXME: The old engine is not dropped when the reopened engine is opened
        // so the `drop` method of the old engine may not be applied timely
        let engine = Engine::new(self.options.clone()).unwrap();
        let _ = std::mem::replace(&mut self.engine, engine);
        self
    }
}

impl Default for EngineWrapper {
    fn default() -> Self {
        let opts = Options {
            dir_path: ENGINEDISTRIBUTOR.path(),
            data_file_size: 8 * 1024 * 1024, // 8 MB
            sync_writes: true,
            index_type: IndexType::BTree,
        };
        EngineWrapper::new(opts)
    }
}

impl Deref for EngineWrapper {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl DerefMut for EngineWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.engine
    }
}

impl Drop for EngineWrapper {
    fn drop(&mut self) {
        for entry in fs::read_dir(&self.path).unwrap().flatten() {
            fs::remove_file(entry.path()).unwrap()
        }
        fs::remove_dir(&self.path).unwrap();
        ENGINEDISTRIBUTOR.drop();
    }
}

#[cfg(test)]
mod tests {
    use crate::mock::engine_wrapper::{EngineWrapper, ENGINEDISTRIBUTOR};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::thread::spawn;

    #[test]
    fn distribute_one_engine() {
        let engine = EngineWrapper::default();
        let path = engine.path.clone();
        assert!(path.is_dir());
        drop(engine);
        assert!(!path.is_dir());
    }

    #[test]
    fn path_never_collision() {
        let memo = Arc::new(Mutex::new(HashSet::<PathBuf>::new()));
        let mut handlers = Vec::new();

        for _ in 1..100 {
            let memo = memo.clone();
            let handler = spawn(move || {
                let generated = ENGINEDISTRIBUTOR.path();
                let mut guard = memo.lock().unwrap();
                assert!(!guard.contains(&generated));
                guard.insert(generated);
                drop(guard);
            });
            handlers.push(handler);
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    }
}
