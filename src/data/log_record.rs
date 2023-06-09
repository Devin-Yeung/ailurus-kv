pub struct LogRecordPos {
    /// The ID of the log file where the record is located.
    file_id: u32,
    /// The byte offset within the log file where the record starts.
    offset: u64,
}
