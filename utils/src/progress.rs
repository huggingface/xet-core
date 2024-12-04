use std::fmt::Debug;

pub trait ProgressUpdater: Debug + Send + Sync {
    fn update(&self, increment: u64);
}

#[derive(Debug)]
pub struct NoOpProgressUpdater;

impl ProgressUpdater for NoOpProgressUpdater {
    fn update(&self, _: u64) {}
}
