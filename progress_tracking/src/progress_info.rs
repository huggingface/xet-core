use std::fmt::Debug;
use std::sync::Arc;

/// A class to make all the bookkeeping clear with progress updating.
#[derive(Clone, Debug)]
pub struct ItemProgressUpdate {
    pub item_name: Arc<str>,
    pub total_bytes: u64,

    // Bytes completed are the total bytes completed, either through
    // deduplication, upload/download, loading from cache, etc.
    pub bytes_completed: u64,
    pub bytes_completion_increment: u64,
}

impl ItemProgressUpdate {
    pub fn merge_in(&mut self, other: ItemProgressUpdate) {
        debug_assert_eq!(self.item_name, other.item_name);

        // Just in case the total got updated, as can be the case when we don't know the
        // size ahead of time.
        self.total_bytes = self.total_bytes.max(other.total_bytes);
        self.bytes_completed = self.bytes_completed.max(other.bytes_completed);
        self.bytes_completion_increment += other.bytes_completion_increment;
    }
}

/// A report of the total progress across files and upload/download items.
///
/// Because of deduplication and caching, the bytes uploaded or downloaded may
/// be different than the bytes transferred.  We thus track this using two metrics.
/// total_transfer_bytes gives the total bytes for upload or download.
/// total bytes gives the total bytes processed, either by deduplication, caching, upload, download, etc.
#[derive(Clone, Debug, Default)]
pub struct ProgressUpdate {
    pub item_updates: Vec<ItemProgressUpdate>,

    /// The total bytes known to process
    pub total_bytes: u64,

    /// The change in total bytes known from the last update
    pub total_bytes_increment: u64,

    /// The total bytes that have been processed
    pub total_bytes_completed: u64,

    /// How much this update adjusts the total bytes..
    pub total_bytes_completion_increment: u64,

    /// The total bytes that have been processed
    pub total_bytes_completion_rate: Option<f64>,

    /// Total bytes known that need to be uploaded or downloaded.   
    pub total_transfer_bytes: u64,

    /// The change in total transfer bytes known from the last update
    pub total_transfer_bytes_increment: u64,

    /// The total bytes that have been uploaded or downloaded.
    pub total_transfer_bytes_completed: u64,

    /// How much this update adjusts the total transfer bytes.
    pub total_transfer_bytes_completion_increment: u64,

    /// The total bytes that have been processed
    pub total_transfer_bytes_completion_rate: Option<f64>,
}

impl ProgressUpdate {
    pub fn is_empty(&self) -> bool {
        self.item_updates.is_empty()
            && self.total_bytes_increment == 0
            && self.total_bytes_completion_increment == 0
            && self.total_transfer_bytes_increment == 0
            && self.total_transfer_bytes_completion_increment == 0
    }

    pub fn merge_in(&mut self, other: ProgressUpdate) {
        self.item_updates.extend(other.item_updates);

        self.total_bytes = self.total_bytes.max(other.total_bytes);
        self.total_bytes_increment += other.total_bytes_increment;
        self.total_bytes_completed = self.total_bytes_completed.max(other.total_bytes_completed);
        self.total_bytes_completion_increment += other.total_bytes_completion_increment;

        self.total_transfer_bytes = self.total_transfer_bytes.max(other.total_transfer_bytes);
        self.total_transfer_bytes_increment += other.total_transfer_bytes_increment;
        self.total_transfer_bytes_completed =
            self.total_transfer_bytes_completed.max(other.total_transfer_bytes_completed);
        self.total_transfer_bytes_completion_increment += other.total_transfer_bytes_completion_increment;
    }
}
