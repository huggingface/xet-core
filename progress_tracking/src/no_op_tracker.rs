use std::sync::Arc;

use crate::{ProgressUpdate, TrackingProgressUpdater};

#[derive(Debug, Default)]
pub struct NoOpProgressUpdater;

impl NoOpProgressUpdater {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait::async_trait]
impl TrackingProgressUpdater for NoOpProgressUpdater {
    async fn register_updates(&self, _updates: ProgressUpdate) {}
}
