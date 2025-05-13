use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

/// A class to make all the bookkeeping clear with the
#[derive(Clone, Debug)]
pub struct ProgressUpdate {
    pub item_name: Arc<str>,
    pub total_count: u64,
    pub completed_count: u64,
    pub update_increment: u64,
}

/// A simple progress updater that simply reports when
/// progress has occured.
#[async_trait]
pub trait SimpleProgressUpdater: Debug + Send + Sync {
    /// updater takes 1 parameter which is an increment value to progress
    /// **not the total progress value**
    async fn update(&self, increment: u64);

    /// Optionally sets the total number of items available.
    async fn set_total(&self, _n_units: u64) {}
}

/// The trait that a progress updater that reports per-item progress completion.
#[async_trait]
pub trait TrackingProgressUpdater: Debug + Send + Sync {
    /// Register a set of updates as a list of ProgressUpdate instances, which
    /// contain the name and progress information.    
    async fn register_updates(&self, updates: &[ProgressUpdate]);
}

/// This struct allows us to wrap the larger progress updater in a simple form for
/// specific items.
#[derive(Debug)]
pub struct ItemProgressUpdater {
    item_name: Arc<str>,
    total_count: AtomicU64,
    completed_count: AtomicU64,
    inner: Arc<dyn TrackingProgressUpdater>,
}

impl ItemProgressUpdater {
    /// In case we need to just track completion of a single item within a function,
    /// this method creates such a class to enable updates.
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>, item_name: Arc<str>, total_count: Option<u64>) -> Arc<Self> {
        let s = Self {
            item_name,
            total_count: AtomicU64::new(total_count.unwrap_or(0)),
            completed_count: AtomicU64::new(0),
            inner,
        };

        Arc::new(s)
    }
}

/// In case we just want to
#[async_trait]
impl SimpleProgressUpdater for ItemProgressUpdater {
    async fn update(&self, increment: u64) {
        self.completed_count.fetch_add(increment, Ordering::Relaxed);

        let progress_update = ProgressUpdate {
            item_name: self.item_name.clone(),
            total_count: self.total_count.load(Ordering::Relaxed),
            completed_count: self.completed_count.load(Ordering::Relaxed),
            update_increment: increment,
        };

        self.inner.register_updates(&[progress_update]).await;
    }

    async fn set_total(&self, n_units: u64) {
        self.total_count.store(n_units, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub struct NoOpProgressUpdater;

impl NoOpProgressUpdater {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl SimpleProgressUpdater for NoOpProgressUpdater {
    async fn update(&self, _increment: u64) {}
}

#[async_trait]
impl TrackingProgressUpdater for NoOpProgressUpdater {
    async fn register_updates(&self, _updates: &[ProgressUpdate]) {}
}
