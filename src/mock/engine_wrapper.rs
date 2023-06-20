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

pub struct EngineDistributor {
    id: Mutex<u32>,
    count: Mutex<u32>,
}

impl EngineDistributor {
    pub fn new() -> Self {
        EngineDistributor {
            id: Mutex::new(0),
            count: Mutex::new(0),
        }
    }

    pub fn path(&self) -> PathBuf {
        let mut path = PathBuf::from(PREFIX);
        let id: u32 = *self.id.lock().unwrap();
        *self.id.lock().unwrap() += 1;
        *self.count.lock().unwrap() += 1;
        path.push(id.to_string());
        // create dir if not exist
        if !path.is_dir() {
            fs::create_dir_all(&path).unwrap()
        }
        path
    }

    pub fn drop(&self) {
        *self.count.lock().unwrap() -= 1;
        if *self.count.lock().unwrap() == 0 {
            fs::remove_dir(PREFIX).unwrap();
        }
    }
}

pub struct EngineWrapper {
    engine: Engine,
    path: PathBuf,
}

impl EngineWrapper {
    pub(crate) fn new(opts: crate::options::Options) -> EngineWrapper {
        EngineWrapper {
            path: opts.dir_path.to_owned(),
            engine: Engine::new(opts).unwrap(),
        }
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
    use crate::mock::engine_wrapper::EngineWrapper;

    #[test]
    fn distribute_one_engine() {
        let engine = EngineWrapper::default();
        let path = engine.path.clone();
        assert!(path.is_dir());
        drop(engine);
        assert!(!path.is_dir());
    }
}
