use std::fmt::Debug;
use std::sync::Arc;

/// A class to make all the bookkeeping clear with progress updating.
#[derive(Clone, Debug)]
pub struct ProgressUpdate {
    pub item_name: Arc<str>,
    pub total_count: u64,
    pub completed_count: u64,
    pub update_increment: u64,
}

impl ProgressUpdate {
    pub fn merge_in(&mut self, other: ProgressUpdate) {
        debug_assert_eq!(self.item_name, other.item_name);

        // Just in case the total got updated, as can be the case when we don't know the
        // size ahead of time.
        self.total_count = self.total_count.max(other.total_count);
        self.completed_count = self.completed_count.max(other.completed_count);
        self.update_increment += other.update_increment;
    }
}

/// A batch of updates; some may be aggregated.
#[derive(Clone, Debug)]
pub struct ProgressUpdateBatch {
    pub item_updates: Vec<ProgressUpdate>,

    pub total_bytes: u64,
    pub total_bytes_completed: u64,
    pub total_bytes_completion_increment: u64,
}
