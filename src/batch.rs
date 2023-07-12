use crate::data::log_record::LogRecord;
use crate::engine::Engine;
use crate::options::WriteBatchOptions;
use std::collections::HashMap;

pub struct WriteBatch<'a> {
    pending_writes: HashMap<Vec<u8>, LogRecord>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}
