use std::fmt::Debug;

/// ProgressUpdater helper to updater some component that progress
/// has occurred.
pub trait ProgressUpdater: Debug + Send + Sync {
    /// updater takes 1 parameter which is an increment value to progress
    /// **not the total progress value**
    fn update(&self, increment: u64);
}

#[derive(Debug)]
pub struct NoOpProgressUpdater;

impl ProgressUpdater for NoOpProgressUpdater {
    fn update(&self, _: u64) {}
}
