#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRecordPos {
    /// The ID of the log file where the record is located.
    pub(crate) file_id: u32,
    /// The byte offset within the log file where the record starts.
    pub(crate) offset: u64,
}
