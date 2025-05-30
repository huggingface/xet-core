pub mod aggregator;
pub mod item_tracking;
mod no_op_tracker;
mod progress_info;
pub mod upload_tracking;
pub mod verification_wrapper;

use async_trait::async_trait;
pub use no_op_tracker::NoOpProgressUpdater;
pub use progress_info::{ItemProgressUpdate, ProgressUpdate};

/// The trait that a progress updater that reports per-item progress completion.
#[async_trait]
pub trait TrackingProgressUpdater: Send + Sync {
    /// Register a set of updates as a list of ProgressUpdate instances, which
    /// contain the name and progress information.    
    async fn register_updates(&self, updates: ProgressUpdate);

    /// Flush any updates out, if needed
    async fn flush(&self) {}
}
