pub mod item_tracking;
mod no_op_tracker;
mod progress_info;
pub mod upload_tracking;
pub mod verification_wrapper;

pub use no_op_tracker::NoOpProgressUpdater;
pub use progress_info::{ProgressUpdate, ProgressUpdateBatch};

/// The trait that a progress updater that reports per-item progress completion.
#[async_trait::async_trait]
pub trait TrackingProgressUpdater: std::fmt::Debug + Send + Sync {
    /// Register a set of updates as a list of ProgressUpdate instances, which
    /// contain the name and progress information.    
    async fn register_updates(&self, updates: ProgressUpdateBatch);
}
