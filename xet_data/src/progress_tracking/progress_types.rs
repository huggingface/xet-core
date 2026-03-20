use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use more_asserts::debug_assert_le;

use super::UniqueID;
use super::speed_tracker::{DEFAULT_MIN_OBSERVATIONS_FOR_RATE, DEFAULT_SPEED_HALF_LIFE, SpeedTracker};
use super::upload_tracking::CompletionTracker;

/// Per-item atomic progress counters. Created by `GroupProgress::new_item()`.
pub struct ItemProgress {
    pub id: UniqueID,
    pub name: Arc<str>,
    pub total_bytes: AtomicU64,
    pub bytes_completed: AtomicU64,
    pub transfer_bytes: AtomicU64,
    pub transfer_bytes_completed: AtomicU64,
    pub size_finalized: AtomicBool,
}

impl ItemProgress {
    fn new(id: UniqueID, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            total_bytes: AtomicU64::new(0),
            bytes_completed: AtomicU64::new(0),
            transfer_bytes: AtomicU64::new(0),
            transfer_bytes_completed: AtomicU64::new(0),
            size_finalized: AtomicBool::new(false),
        }
    }

    /// Snapshot of this item's progress. Reads completions first (Acquire),
    /// then totals, which reduces transient skew in sampled values.
    pub fn report(&self) -> ItemProgressReport {
        let bytes_completed = self.bytes_completed.load(Ordering::Acquire);
        let transfer_bytes_completed = self.transfer_bytes_completed.load(Ordering::Acquire);
        let total_bytes = self.total_bytes.load(Ordering::Acquire);
        let transfer_bytes = self.transfer_bytes.load(Ordering::Acquire);

        debug_assert_le!(bytes_completed, total_bytes);
        debug_assert_le!(transfer_bytes_completed, transfer_bytes);

        ItemProgressReport {
            item_name: self.name.to_string(),
            total_bytes,
            bytes_completed,
        }
    }
}

impl std::fmt::Debug for ItemProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ItemProgress")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("total_bytes", &self.total_bytes.load(Ordering::Relaxed))
            .field("bytes_completed", &self.bytes_completed.load(Ordering::Relaxed))
            .finish()
    }
}

/// Aggregate progress across all items, with an item registry.
/// Owns the atomic group-level counters and a map of per-item progress.
pub struct GroupProgress {
    pub total_bytes: AtomicU64,
    pub total_bytes_completed: AtomicU64,
    pub total_transfer_bytes: AtomicU64,
    pub total_transfer_bytes_completed: AtomicU64,
    items: Mutex<HashMap<UniqueID, Arc<ItemProgress>>>,
    speed_tracker: Mutex<SpeedTracker>,
}

impl GroupProgress {
    /// Create a group progress tracker using default speed estimation parameters.
    pub fn new() -> Arc<Self> {
        Self::with_speed_config(DEFAULT_SPEED_HALF_LIFE, DEFAULT_MIN_OBSERVATIONS_FOR_RATE)
    }

    /// Create a group progress tracker with explicit speed estimation parameters.
    pub fn with_speed_config(half_life: Duration, min_observations: u32) -> Arc<Self> {
        Arc::new(Self {
            total_bytes: AtomicU64::new(0),
            total_bytes_completed: AtomicU64::new(0),
            total_transfer_bytes: AtomicU64::new(0),
            total_transfer_bytes_completed: AtomicU64::new(0),
            items: Mutex::new(HashMap::new()),
            speed_tracker: Mutex::new(SpeedTracker::new(half_life).with_min_observations(min_observations)),
        })
    }

    /// Create a new tracked item and register it in the items map.
    /// Returns an `ItemProgressUpdater` handle for the caller to report progress.
    pub fn new_item(self: &Arc<Self>, id: UniqueID, name: impl Into<Arc<str>>) -> Arc<ItemProgressUpdater> {
        let item = Arc::new(ItemProgress::new(id, name.into()));
        self.items.lock().unwrap().insert(id, item.clone());
        Arc::new(ItemProgressUpdater {
            item,
            group: self.clone(),
        })
    }

    /// Create a new CompletionTracker backed by this group's progress.
    pub fn new_completion_tracker(self: &Arc<Self>) -> CompletionTracker {
        CompletionTracker::new(self.clone())
    }

    /// Snapshot of aggregate progress. Reads completions first (Acquire), then totals
    /// to reduce transient sampling skew.
    ///
    /// Speed is estimated via [`SpeedTracker`], which uses an exponentially-weighted
    /// moving average to produce smoothed bytes-per-second rates.
    ///
    /// This call updates internal speed-estimation state, so repeated calls are
    /// not strictly idempotent. Rate fields remain `None` until enough speed
    /// observations have been collected.
    pub fn report(&self) -> GroupProgressReport {
        let total_bytes_completed = self.total_bytes_completed.load(Ordering::Acquire);
        let total_transfer_bytes_completed = self.total_transfer_bytes_completed.load(Ordering::Acquire);
        let total_bytes = self.total_bytes.load(Ordering::Acquire);
        let total_transfer_bytes = self.total_transfer_bytes.load(Ordering::Acquire);

        debug_assert_le!(total_bytes_completed, total_bytes);
        debug_assert_le!(total_transfer_bytes_completed, total_transfer_bytes);

        let mut tracker = self.speed_tracker.lock().unwrap();
        tracker.update(total_bytes_completed, total_transfer_bytes_completed);
        let (bytes_rate, transfer_rate) = tracker.rates();

        GroupProgressReport {
            total_bytes,
            total_bytes_completed,
            total_bytes_completion_rate: bytes_rate,
            total_transfer_bytes,
            total_transfer_bytes_completed,
            total_transfer_bytes_completion_rate: transfer_rate,
        }
    }

    /// Snapshot of all per-item progress.
    pub fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        let items = self.items.lock().unwrap();
        items.iter().map(|(id, item)| (*id, item.report())).collect()
    }

    /// Snapshot of one item's progress.
    pub fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        let items = self.items.lock().unwrap();
        items.get(&id).map(|item| item.report())
    }

    /// Debug verification that all items are complete.
    pub fn assert_complete(&self) {
        #[cfg(debug_assertions)]
        {
            let total_bytes_completed = self.total_bytes_completed.load(Ordering::Acquire);
            let total_bytes = self.total_bytes.load(Ordering::Acquire);
            assert_eq!(
                total_bytes_completed, total_bytes,
                "GroupProgress not complete: total_bytes_completed={total_bytes_completed} != total_bytes={total_bytes}"
            );

            let total_transfer_bytes_completed = self.total_transfer_bytes_completed.load(Ordering::Acquire);
            let total_transfer_bytes = self.total_transfer_bytes.load(Ordering::Acquire);
            assert_eq!(
                total_transfer_bytes_completed, total_transfer_bytes,
                "GroupProgress not complete: total_transfer_bytes_completed={total_transfer_bytes_completed} != total_transfer_bytes={total_transfer_bytes}"
            );

            let items = self.items.lock().unwrap();
            for (id, item) in items.iter() {
                let completed = item.bytes_completed.load(Ordering::Acquire);
                let total = item.total_bytes.load(Ordering::Acquire);
                assert_eq!(
                    completed, total,
                    "Item '{}' ({id}) not complete: bytes_completed={completed} != total_bytes={total}",
                    item.name
                );
            }
        }
    }
}

impl Default for GroupProgress {
    fn default() -> Self {
        // Note: returns Self, not Arc<Self>. Use GroupProgress::new() for Arc.
        Self {
            total_bytes: AtomicU64::new(0),
            total_bytes_completed: AtomicU64::new(0),
            total_transfer_bytes: AtomicU64::new(0),
            total_transfer_bytes_completed: AtomicU64::new(0),
            items: Mutex::new(HashMap::new()),
            speed_tracker: Mutex::new(
                SpeedTracker::new(DEFAULT_SPEED_HALF_LIFE).with_min_observations(DEFAULT_MIN_OBSERVATIONS_FOR_RATE),
            ),
        }
    }
}

impl std::fmt::Debug for GroupProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupProgress")
            .field("total_bytes", &self.total_bytes.load(Ordering::Relaxed))
            .field("total_bytes_completed", &self.total_bytes_completed.load(Ordering::Relaxed))
            .field("total_transfer_bytes", &self.total_transfer_bytes.load(Ordering::Relaxed))
            .field("total_transfer_bytes_completed", &self.total_transfer_bytes_completed.load(Ordering::Relaxed))
            .finish()
    }
}

/// Handle for reporting progress on a single item. All progress updates
/// (both download and upload paths) go through this type.
///
/// Replaces both `DownloadTaskUpdater` and the per-file update logic
/// that was previously in `CompletionTracker`.
pub struct ItemProgressUpdater {
    item: Arc<ItemProgress>,
    group: Arc<GroupProgress>,
}

impl ItemProgressUpdater {
    /// Create a standalone updater for debug/testing purposes.
    /// Creates its own throwaway GroupProgress.
    #[cfg(test)]
    pub fn new_standalone(name: &str) -> Arc<Self> {
        let group = GroupProgress::new();
        let item = Arc::new(ItemProgress::new(UniqueID::new(), Arc::from(name)));
        Arc::new(Self { item, group })
    }

    // === Size updates (use fetch_update for full atomicity) ===

    /// Update the total item size. When `is_final` is true, subsequent calls are ignored.
    pub fn update_item_size(&self, total: u64, is_final: bool) {
        if self.item.size_finalized.load(Ordering::Acquire) {
            if is_final {
                debug_assert_eq!(self.item.total_bytes.load(Ordering::Acquire), total);
            }
            return;
        }
        if is_final {
            self.item.size_finalized.store(true, Ordering::Release);
        }
        let result = self
            .item
            .total_bytes
            .fetch_update(Ordering::Release, Ordering::Acquire, |old| if total > old { Some(total) } else { None });
        if let Ok(old) = result {
            self.group.total_bytes.fetch_add(total - old, Ordering::Release);
        }
    }

    /// Update the total transfer (network) bytes for this item.
    pub fn update_transfer_size(&self, total: u64) {
        let result = self
            .item
            .transfer_bytes
            .fetch_update(Ordering::Release, Ordering::Acquire, |old| if total > old { Some(total) } else { None });
        if let Ok(old) = result {
            self.group.total_transfer_bytes.fetch_add(total - old, Ordering::Release);
        }
    }

    // === Completion updates (group first, then item) ===

    /// Report decompressed/processed bytes completed for this item.
    pub fn report_bytes_completed(&self, increment: u64) {
        if increment == 0 {
            return;
        }
        self.group.total_bytes_completed.fetch_add(increment, Ordering::Release);
        let new_completed = self.item.bytes_completed.fetch_add(increment, Ordering::Release) + increment;
        debug_assert_le!(
            new_completed,
            self.item.total_bytes.load(Ordering::Acquire),
            "item '{}' bytes_completed ({}) exceeded total_bytes after +{}",
            self.item.name,
            new_completed,
            increment
        );
    }

    /// Report transfer (network) bytes completed.
    pub fn report_transfer_bytes_completed(&self, increment: u64) {
        if increment == 0 {
            return;
        }
        self.group
            .total_transfer_bytes_completed
            .fetch_add(increment, Ordering::Release);
        let new_completed = self.item.transfer_bytes_completed.fetch_add(increment, Ordering::Release) + increment;
        debug_assert_le!(
            new_completed,
            self.item.transfer_bytes.load(Ordering::Acquire),
            "item '{}' transfer_bytes_completed ({}) exceeded transfer_bytes after +{}",
            self.item.name,
            new_completed,
            increment
        );
    }

    // === Aliases for reconstruction pipeline compatibility ===

    /// Alias for `report_bytes_completed` -- used by the reconstruction data writer.
    pub fn report_bytes_written(&self, increment: u64) {
        self.report_bytes_completed(increment);
    }

    /// Alias for `report_transfer_bytes_completed` -- used by xorb block download.
    pub fn report_transfer_progress(&self, delta: u64) {
        self.report_transfer_bytes_completed(delta);
    }

    /// Read the current bytes_completed for this item.
    pub fn total_bytes_completed(&self) -> u64 {
        self.item.bytes_completed.load(Ordering::Acquire)
    }

    // === Debug verification ===

    /// Assert this item is fully complete (completed == total for both bytes and transfer).
    pub fn assert_complete(&self) {
        #[cfg(debug_assertions)]
        {
            let completed = self.item.bytes_completed.load(Ordering::Acquire);
            let total = self.item.total_bytes.load(Ordering::Acquire);
            assert_eq!(
                completed, total,
                "item '{}' not complete: bytes_completed={completed} != total_bytes={total}",
                self.item.name
            );
            let t_completed = self.item.transfer_bytes_completed.load(Ordering::Acquire);
            let t_total = self.item.transfer_bytes.load(Ordering::Acquire);
            assert_eq!(
                t_completed, t_total,
                "item '{}' not complete: transfer_bytes_completed={t_completed} != transfer_bytes={t_total}",
                self.item.name
            );
        }
    }

    pub fn item(&self) -> &Arc<ItemProgress> {
        &self.item
    }

    pub fn report(&self) -> ItemProgressReport {
        self.item.report()
    }

    pub fn group(&self) -> &Arc<GroupProgress> {
        &self.group
    }
}

impl std::fmt::Debug for ItemProgressUpdater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ItemProgressUpdater").field("item", &self.item).finish()
    }
}

// === Snapshot report structs ===

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all))]
pub struct GroupProgressReport {
    pub total_bytes: u64,
    pub total_bytes_completed: u64,
    pub total_bytes_completion_rate: Option<f64>,
    pub total_transfer_bytes: u64,
    pub total_transfer_bytes_completed: u64,
    pub total_transfer_bytes_completion_rate: Option<f64>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all))]
pub struct ItemProgressReport {
    pub item_name: String,
    pub total_bytes: u64,
    pub bytes_completed: u64,
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, advance, pause};

    use super::*;

    #[test]
    fn test_group_progress_new_item() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_item_progress_updater_bytes() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.report_bytes_completed(50);

        assert_eq!(updater.total_bytes_completed(), 50);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_item_progress_updater_transfer() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_transfer_size(80);
        updater.report_transfer_bytes_completed(30);

        assert_eq!(group.total_transfer_bytes.load(Ordering::Relaxed), 80);
        assert_eq!(group.total_transfer_bytes_completed.load(Ordering::Relaxed), 30);
    }

    #[test]
    fn test_update_item_size_finalized() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_item_size(200, false);

        assert_eq!(updater.item().total_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_update_item_size_monotonic_increase() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, false);
        updater.update_item_size(300, false);

        assert_eq!(updater.item().total_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_update_item_size_same_value_noop() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, false);
        updater.update_item_size(100, false);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_report_snapshot() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "file.bin");
        updater.update_item_size(1000, true);
        updater.update_transfer_size(800);
        updater.report_bytes_completed(500);
        updater.report_transfer_bytes_completed(300);

        let report = group.report();
        assert_eq!(report.total_bytes, 1000);
        assert_eq!(report.total_bytes_completed, 500);
        assert_eq!(report.total_transfer_bytes, 800);
        assert_eq!(report.total_transfer_bytes_completed, 300);
    }

    #[test]
    fn test_item_reports() {
        let group = GroupProgress::new();
        let id1 = UniqueID::new();
        let id2 = UniqueID::new();

        let u1 = group.new_item(id1, "a.bin");
        let u2 = group.new_item(id2, "b.bin");

        u1.update_item_size(100, true);
        u1.report_bytes_completed(60);
        u2.update_item_size(200, true);
        u2.report_bytes_completed(200);

        let reports = group.item_reports();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[&id1].bytes_completed, 60);
        assert_eq!(reports[&id2].bytes_completed, 200);
    }

    #[test]
    fn test_multiple_items_group_totals() {
        let group = GroupProgress::new();
        let u1 = group.new_item(UniqueID::new(), "a.bin");
        let u2 = group.new_item(UniqueID::new(), "b.bin");

        u1.update_item_size(100, true);
        u2.update_item_size(200, true);
        u1.report_bytes_completed(50);
        u2.report_bytes_completed(100);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_assert_complete() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_transfer_size(80);
        updater.report_bytes_completed(100);
        updater.report_transfer_bytes_completed(80);

        updater.assert_complete();
        group.assert_complete();
    }

    #[test]
    fn test_zero_increment_noop() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.report_bytes_completed(0);

        assert_eq!(updater.total_bytes_completed(), 0);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_report_bytes_written_alias() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.report_bytes_written(50);

        assert_eq!(updater.total_bytes_completed(), 50);
    }

    #[test]
    fn test_report_transfer_progress_alias() {
        let group = GroupProgress::new();
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_transfer_size(90);
        updater.report_transfer_progress(40);

        assert_eq!(updater.item().transfer_bytes_completed.load(Ordering::Relaxed), 40);
        assert_eq!(group.total_transfer_bytes_completed.load(Ordering::Relaxed), 40);
    }

    #[tokio::test]
    async fn test_report_rates_none_until_min_observations_then_some() {
        pause();

        let group = GroupProgress::with_speed_config(Duration::from_secs(10), 3);
        let updater = group.new_item(UniqueID::new(), "test.bin");
        updater.update_item_size(10_000, true);
        updater.update_transfer_size(10_000);

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_progress(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_none());
        assert!(report.total_transfer_bytes_completion_rate.is_none());

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_progress(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_none());
        assert!(report.total_transfer_bytes_completion_rate.is_none());

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_progress(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_some());
        assert!(report.total_transfer_bytes_completion_rate.is_some());
    }
}
