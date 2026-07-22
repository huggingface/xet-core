use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use more_asserts::debug_assert_le;
use xet_client::cas_types::{CommitStage, ShardUploadEvent};
use xet_runtime::utils::UniqueId;

use super::speed_tracker::{DEFAULT_MIN_OBSERVATIONS_FOR_RATE, DEFAULT_SPEED_HALF_LIFE, SpeedTracker};

/// Per-item atomic progress counters. Created by `GroupProgress::new_item()`.
pub struct ItemProgress {
    pub id: UniqueId,
    pub name: Arc<str>,
    pub total_bytes: AtomicU64,
    pub bytes_completed: AtomicU64,
    pub transfer_bytes: AtomicU64,
    pub transfer_bytes_completed: AtomicU64,
    pub size_finalized: AtomicBool,
}

impl ItemProgress {
    fn new(id: UniqueId, name: Arc<str>) -> Self {
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
    items: Mutex<HashMap<UniqueId, Arc<ItemProgress>>>,
    speed_tracker: Mutex<SpeedTracker>,
}

impl GroupProgress {
    /// Create a group progress tracker using default speed estimation parameters.
    pub fn new() -> Self {
        Self::with_speed_config(DEFAULT_SPEED_HALF_LIFE, DEFAULT_MIN_OBSERVATIONS_FOR_RATE)
    }

    /// Create a group progress tracker with explicit speed estimation parameters.
    pub fn with_speed_config(half_life: Duration, min_observations: u32) -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_bytes_completed: AtomicU64::new(0),
            total_transfer_bytes: AtomicU64::new(0),
            total_transfer_bytes_completed: AtomicU64::new(0),
            items: Mutex::new(HashMap::new()),
            speed_tracker: Mutex::new(SpeedTracker::new(half_life).with_min_observations(min_observations)),
        }
    }

    /// Create a new tracked item and register it in the items map.
    /// Returns an `ItemProgressUpdater` handle for the caller to report progress.
    pub fn new_item(self: &Arc<Self>, id: UniqueId, name: impl Into<Arc<str>>) -> Arc<ItemProgressUpdater> {
        let item = Arc::new(ItemProgress::new(id, name.into()));
        self.items.lock().unwrap().insert(id, item.clone());
        Arc::new(ItemProgressUpdater {
            item,
            group: self.clone(),
        })
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
            shard: None,
        }
    }

    /// Snapshot of all per-item progress.
    pub fn item_reports(&self) -> HashMap<UniqueId, ItemProgressReport> {
        let items = self.items.lock().unwrap();
        items.iter().map(|(id, item)| (*id, item.report())).collect()
    }

    /// Snapshot of one item's progress.
    pub fn item_report(&self, id: UniqueId) -> Option<ItemProgressReport> {
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

/// Aggregate progress for concurrent shard uploads: raw transfer bytes plus the
/// server-reported validation/commit progress streamed back on the v2 NDJSON shard
/// upload response. Per-shard state is keyed by the `UniqueId` returned from
/// `register_shard_transfer`.
#[derive(Default)]
pub struct ShardUploadProgress {
    pub total_shard_bytes: AtomicU64,
    pub total_shard_bytes_upload_completed: AtomicU64,
    pub total_shards: AtomicU32,
    pub total_shard_validation_entries: AtomicU64,
    pub total_shard_validation_entries_completed: AtomicU64,
    pub total_shards_uploaded_to_store: AtomicU32,
    pub total_shards_synced: AtomicU32,
    pub total_shards_completed: AtomicU32,
    /// Latest NDJSON event observed per shard, used by `update` to detect and discard
    /// stale/out-of-order/duplicate frames.
    pub shard_upload_state: Mutex<HashMap<UniqueId, ShardUploadEvent>>,
}

impl ShardUploadProgress {
    /// Snapshot of aggregate shard upload/validation progress.
    pub fn report(&self) -> ShardUploadProgressReport {
        ShardUploadProgressReport {
            total_shard_bytes: self.total_shard_bytes.load(Ordering::Relaxed),
            total_shard_bytes_upload_completed: self.total_shard_bytes_upload_completed.load(Ordering::Relaxed),
            total_shards: self.total_shards.load(Ordering::Relaxed),
            total_shard_validation_entries: self.total_shard_validation_entries.load(Ordering::Relaxed),
            total_shard_validation_entries_completed: self
                .total_shard_validation_entries_completed
                .load(Ordering::Relaxed),
            total_shards_uploaded_to_store: self.total_shards_uploaded_to_store.load(Ordering::Relaxed),
            total_shards_synced: self.total_shards_synced.load(Ordering::Relaxed),
            total_shards_completed: self.total_shards_completed.load(Ordering::Relaxed),
        }
    }

    /// Register a new shard upload of `shard_size` bytes and return its tracking id.
    /// Seeds the id's NDJSON state as `Validating { verified: 0, total: 0 }` so the first
    /// event `update` receives for it always has a previous state to compare against.
    pub fn register_shard_transfer(&self, shard_size: u64) -> UniqueId {
        let id = UniqueId::new();
        self.shard_upload_state
            .lock()
            .unwrap()
            .insert(id, ShardUploadEvent::Validating { verified: 0, total: 0 });
        self.total_shard_bytes.fetch_add(shard_size, Ordering::Relaxed);
        self.total_shards.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Record incremental bytes transferred for a shard upload (raw HTTP body progress,
    /// not the server-side validation/commit progress tracked by `update`).
    pub fn increment_shard_transfer_progress(&self, nbytes: u64) {
        self.total_shard_bytes_upload_completed.fetch_add(nbytes, Ordering::Relaxed);
    }

    /// Apply a v2 NDJSON progress event for the shard tracked by `id`.
    ///
    /// Events for a given shard normally progress `Validating` -> `Committing(Uploading)`
    /// -> `Committing(Syncing)` -> `Result`, but frames can be skipped, duplicated, or arrive
    /// out of order over the wire. `Validating` is handled separately (its counters accumulate
    /// in place); `Committing`/`Result` are stage milestones, each replacing the stored state
    /// only when it represents genuine forward progress (see `precede`), with any earlier
    /// milestone that got skipped credited first so the aggregate counters can't get stuck.
    pub fn update(&self, id: UniqueId, event: &ShardUploadEvent) {
        // compare with the previous event in case of unordered or skipped event
        let mut guard = self.shard_upload_state.lock().unwrap();
        let maybe_old = guard.get_mut(&id);

        match event {
            ShardUploadEvent::Validating { verified, total } => {
                self.credit_validating_progress(maybe_old, *verified, *total);
            },
            ShardUploadEvent::Error { .. } | ShardUploadEvent::Unknown => {},
            // `Committing(*)` and `Result`: stage milestones. `Error`/`Unknown`/`Validating`
            // are handled above, so this only ever sees forward-moving stages.
            _ => {
                self.credit_skipped_validation(&maybe_old);
                self.credit_skipped_store_stage(&maybe_old, event);
                if maybe_old.is_none_or(|old| old.precede(event)) {
                    guard.insert(id, event.clone());
                    if matches!(event, ShardUploadEvent::Result) {
                        self.total_shards_synced.fetch_add(1, Ordering::Relaxed);
                        self.total_shards_completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            },
        }
    }

    /// `Validating` counters only ever move forward: increments are computed against the last
    /// recorded `(verified, total)` via `saturating_sub`, so a stale/duplicate frame with lower
    /// values contributes nothing.
    fn credit_validating_progress(&self, maybe_old: Option<&mut ShardUploadEvent>, verified: u64, total: u64) {
        let Some(ShardUploadEvent::Validating {
            verified: old_verified,
            total: old_total,
        }) = maybe_old
        else {
            return;
        };

        let verified_increment = verified.saturating_sub(*old_verified); // in case of unordered event
        let total_increment = total.saturating_sub(*old_total);
        if verified_increment > 0 {
            *old_verified += verified_increment;
            self.total_shard_validation_entries_completed
                .fetch_add(verified_increment, Ordering::Relaxed);
        }
        if total_increment > 0 {
            *old_total += total_increment;
            self.total_shard_validation_entries
                .fetch_add(total_increment, Ordering::Relaxed);
        }
    }

    /// The server may skip straight to `Committing`/`Result` without ever sending a final
    /// `Validating { verified: total, total }` frame. If the last recorded state is still
    /// `Validating` with `verified < total`, credit the remainder now so
    /// `total_shard_validation_entries_completed` doesn't get stuck below the total.
    fn credit_skipped_validation(&self, maybe_old: &Option<&mut ShardUploadEvent>) {
        if let Some(ShardUploadEvent::Validating {
            verified: old_verified,
            total: old_total,
        }) = maybe_old
        {
            let verified_increment = old_total.saturating_sub(*old_verified);
            if verified_increment > 0 {
                self.total_shard_validation_entries_completed
                    .fetch_add(verified_increment, Ordering::Relaxed);
            }
        }
    }

    /// Likewise, the explicit `Committing(Syncing)` frame may be skipped in favor of jumping
    /// straight to `Result`. Whichever event first represents the shard reaching the
    /// durable-write "store" stage counts it exactly once: `already_counted` is true only once
    /// the stored state has itself reached `Committing(Syncing)` or `Result`.
    fn credit_skipped_store_stage(&self, maybe_old: &Option<&mut ShardUploadEvent>, event: &ShardUploadEvent) {
        let reached_store_stage = matches!(
            event,
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            } | ShardUploadEvent::Result
        );
        let already_counted = matches!(
            maybe_old,
            Some(ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            }) | Some(ShardUploadEvent::Result)
        );
        if reached_store_stage && !already_counted {
            self.total_shards_uploaded_to_store.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for ShardUploadProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.report()))
    }
}

pub struct UploadGroupProgress {
    // File read and Xorb upload progress
    pub file_data: Arc<GroupProgress>,
    // Shard upload and validation progress
    pub shards: Arc<ShardUploadProgress>,
}

impl Default for UploadGroupProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl UploadGroupProgress {
    /// Create a group progress tracker using default speed estimation parameters.
    pub fn new() -> Self {
        Self::with_speed_config(DEFAULT_SPEED_HALF_LIFE, DEFAULT_MIN_OBSERVATIONS_FOR_RATE)
    }

    /// Create a group progress tracker with explicit speed estimation parameters.
    pub fn with_speed_config(half_life: Duration, min_observations: u32) -> Self {
        Self {
            file_data: Arc::new(GroupProgress::with_speed_config(half_life, min_observations)),
            shards: Default::default(),
        }
    }

    /// Create a new tracked item and register it in the items map.
    /// Returns an `ItemProgressUpdater` handle for the caller to report progress.
    pub fn new_item(self: &Arc<Self>, id: UniqueId, name: impl Into<Arc<str>>) -> Arc<ItemProgressUpdater> {
        self.file_data.new_item(id, name)
    }

    /// Snapshot of aggregate data and shard upload progress.
    pub fn report(&self) -> GroupProgressReport {
        let mut report = self.file_data.report();
        report.shard = Some(self.shards.report());
        report
    }

    /// Snapshot of all per-item progress.
    pub fn item_reports(&self) -> HashMap<UniqueId, ItemProgressReport> {
        self.file_data.item_reports()
    }

    /// Snapshot of one item's progress.
    pub fn item_report(&self, id: UniqueId) -> Option<ItemProgressReport> {
        self.file_data.item_report(id)
    }

    /// Debug verification that all items are complete.
    pub fn assert_complete(&self) {
        #[cfg(debug_assertions)]
        {
            self.file_data.assert_complete();
        }
    }

    /// Register a new shard upload size.
    pub fn register_shard_transfer(&self, shard_size: u64) -> UniqueId {
        self.shards.register_shard_transfer(shard_size)
    }

    /// Update shard transfer bytes for transfer "_id"
    pub fn increment_shard_transfer_progress(&self, _id: UniqueId, nbytes: u64) {
        self.shards.increment_shard_transfer_progress(nbytes)
    }

    /// Update shard upload progress
    pub fn register_shard_upload_progress(&self, id: UniqueId, event: &ShardUploadEvent) {
        self.shards.update(id, event)
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
    #[cfg(any(debug_assertions, test))]
    pub fn new_standalone(name: &str) -> Arc<Self> {
        let group = Arc::new(GroupProgress::new());
        let item = Arc::new(ItemProgress::new(UniqueId::new(), Arc::from(name)));
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
}

impl std::fmt::Debug for ItemProgressUpdater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ItemProgressUpdater").field("item", &self.item).finish()
    }
}

// === Snapshot report structs ===

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, from_py_object))]
pub struct GroupProgressReport {
    pub total_bytes: u64,
    pub total_bytes_completed: u64,
    pub total_bytes_completion_rate: Option<f64>,
    pub total_transfer_bytes: u64,
    pub total_transfer_bytes_completed: u64,
    pub total_transfer_bytes_completion_rate: Option<f64>,
    /// Shard finalization progress when an upload commit is tracking shards.
    ///
    /// `None` for downloads, dry-run, or older callers that predate the shard
    /// progress section — not an error.
    pub shard: Option<ShardUploadProgressReport>,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, from_py_object))]
pub struct ShardUploadProgressReport {
    pub total_shard_bytes: u64,
    pub total_shard_bytes_upload_completed: u64,
    pub total_shards: u32,
    pub total_shard_validation_entries: u64,
    pub total_shard_validation_entries_completed: u64,
    pub total_shards_uploaded_to_store: u32,
    pub total_shards_synced: u32,
    pub total_shards_completed: u32,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, from_py_object))]
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
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_item_progress_updater_bytes() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.report_bytes_completed(50);

        assert_eq!(updater.total_bytes_completed(), 50);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_item_progress_updater_transfer() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_transfer_size(80);
        updater.report_transfer_bytes_completed(30);

        assert_eq!(group.total_transfer_bytes.load(Ordering::Relaxed), 80);
        assert_eq!(group.total_transfer_bytes_completed.load(Ordering::Relaxed), 30);
    }

    #[test]
    fn test_update_item_size_finalized() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_item_size(200, false);

        assert_eq!(updater.item().total_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_update_item_size_monotonic_increase() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, false);
        updater.update_item_size(300, false);

        assert_eq!(updater.item().total_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_update_item_size_same_value_noop() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, false);
        updater.update_item_size(100, false);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_report_snapshot() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "file.bin");
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
        let group = Arc::new(GroupProgress::new());
        let id1 = UniqueId::new();
        let id2 = UniqueId::new();

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
        let group = Arc::new(GroupProgress::new());
        let u1 = group.new_item(UniqueId::new(), "a.bin");
        let u2 = group.new_item(UniqueId::new(), "b.bin");

        u1.update_item_size(100, true);
        u2.update_item_size(200, true);
        u1.report_bytes_completed(50);
        u2.report_bytes_completed(100);

        assert_eq!(group.total_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_assert_complete() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.update_transfer_size(80);
        updater.report_bytes_completed(100);
        updater.report_transfer_bytes_completed(80);

        updater.assert_complete();
        group.assert_complete();
    }

    #[test]
    fn test_zero_increment_noop() {
        let group = Arc::new(GroupProgress::new());
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(100, true);
        updater.report_bytes_completed(0);

        assert_eq!(updater.total_bytes_completed(), 0);
        assert_eq!(group.total_bytes_completed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_report_rates_none_until_min_observations_then_some() {
        pause();

        let group = Arc::new(GroupProgress::with_speed_config(Duration::from_secs(10), 3));
        let updater = group.new_item(UniqueId::new(), "test.bin");
        updater.update_item_size(10_000, true);
        updater.update_transfer_size(10_000);

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_bytes_completed(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_none());
        assert!(report.total_transfer_bytes_completion_rate.is_none());

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_bytes_completed(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_none());
        assert!(report.total_transfer_bytes_completion_rate.is_none());

        advance(Duration::from_millis(200)).await;
        updater.report_bytes_completed(1_000);
        updater.report_transfer_bytes_completed(800);
        let report = group.report();
        assert!(report.total_bytes_completion_rate.is_some());
        assert!(report.total_transfer_bytes_completion_rate.is_some());
    }

    #[test]
    fn test_shard_update_validating_ignores_stale_out_of_order_frame() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        progress.update(id, &ShardUploadEvent::Validating { verified: 2, total: 5 });
        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 2);
        assert_eq!(report.total_shard_validation_entries, 5);

        // A duplicate/reordered frame with lower values than what's already recorded must
        // not move the counters backward or double count.
        progress.update(id, &ShardUploadEvent::Validating { verified: 1, total: 3 });
        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 2);
        assert_eq!(report.total_shard_validation_entries, 5);

        // Forward progress after a stale frame still accumulates correctly.
        progress.update(id, &ShardUploadEvent::Validating { verified: 4, total: 5 });
        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 4);
        assert_eq!(report.total_shard_validation_entries, 5);
    }

    #[test]
    fn test_shard_update_committing_syncing_ignores_stale_and_duplicate_frames() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
        );
        assert_eq!(progress.report().total_shards_uploaded_to_store, 1);
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            }
        );

        // A duplicate/replayed frame for the same shard (e.g. retried NDJSON delivery) must
        // not be counted as a second shard reaching the store.
        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
        );
        assert_eq!(progress.report().total_shards_uploaded_to_store, 1);

        // "Uploading" arrives after "Syncing" was already observed (reordered in transit).
        // It must not regress the recorded stage either.
        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Uploading,
            },
        );
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            }
        );
    }

    #[test]
    fn test_shard_update_credits_skipped_validation_regardless_of_which_stage_arrives_first() {
        let progress = ShardUploadProgress::default();

        // Shard A: partial validation progress, then moves straight to `Committing(Uploading)`
        // without ever sending a final `Validating { verified: total, total }` frame. The
        // remaining (total - verified) must still be credited so the aggregate "completed"
        // counter doesn't get stuck below the total forever.
        let id_a = progress.register_shard_transfer(1000);
        progress.update(id_a, &ShardUploadEvent::Validating { verified: 3, total: 10 });
        progress.update(
            id_a,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Uploading,
            },
        );
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id_a).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Uploading
            }
        );

        // Shard B: validation was already fully reported before committing starts. Must not
        // double-count the already-completed entries.
        let id_b = progress.register_shard_transfer(1000);
        progress.update(
            id_b,
            &ShardUploadEvent::Validating {
                verified: 10,
                total: 10,
            },
        );
        progress.update(
            id_b,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Uploading,
            },
        );

        // Shard C: partial validation progress, then skips `Committing(Uploading)` entirely,
        // jumping straight to `Committing(Syncing)`. The credit isn't tied to one specific
        // stage -- it fires the first time we see anything past `Validating`.
        let id_c = progress.register_shard_transfer(1000);
        progress.update(id_c, &ShardUploadEvent::Validating { verified: 4, total: 10 });
        progress.update(
            id_c,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
        );
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id_c).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            }
        );

        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 30); // 10 (a) + 10 (b, no extra credit) + 10 (c)
        assert_eq!(report.total_shard_validation_entries, 30);
    }

    #[test]
    fn test_shard_update_result_after_skipped_committing_stage() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        // Validation is still incomplete (3/10) when the shard jumps straight to the terminal
        // frame without ever observing a `Committing` frame.
        progress.update(id, &ShardUploadEvent::Validating { verified: 3, total: 10 });

        progress.update(id, &ShardUploadEvent::Result);

        let report = progress.report();
        // The remaining (total - verified) is credited so the counter doesn't get stuck, even
        // though `Committing` -- where this credit was previously applied -- never arrived.
        assert_eq!(report.total_shard_validation_entries_completed, 10);
        assert_eq!(report.total_shard_validation_entries, 10);
        // Likewise, reaching `Result` directly implies the durable-write "store" stage
        // completed too, even though no `Committing(Syncing)` frame was ever observed.
        assert_eq!(report.total_shards_uploaded_to_store, 1);
        assert_eq!(report.total_shards_synced, 1);
        assert_eq!(report.total_shards_completed, 1);
        assert_eq!(*progress.shard_upload_state.lock().unwrap().get(&id).unwrap(), ShardUploadEvent::Result);
    }

    #[test]
    fn test_shard_update_result_after_explicit_syncing_does_not_double_count_store() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        // The explicit `Committing(Syncing)` frame is observed this time...
        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
        );
        assert_eq!(progress.report().total_shards_uploaded_to_store, 1);

        // ...so `Result` arriving afterward must not credit the store counter a second time.
        progress.update(id, &ShardUploadEvent::Result);
        let report = progress.report();
        assert_eq!(report.total_shards_uploaded_to_store, 1);
        assert_eq!(report.total_shards_synced, 1);
        assert_eq!(report.total_shards_completed, 1);
    }

    #[test]
    fn test_shard_update_result_is_terminal_and_rejects_late_or_duplicate_frames() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        progress.update(id, &ShardUploadEvent::Result);
        let report = progress.report();
        assert_eq!(report.total_shards_synced, 1);
        assert_eq!(report.total_shards_completed, 1);

        // A stray "Committing" frame arrives after the terminal frame (e.g. reordered). It
        // must not overwrite the recorded result.
        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Uploading,
            },
        );
        assert_eq!(*progress.shard_upload_state.lock().unwrap().get(&id).unwrap(), ShardUploadEvent::Result);

        // A duplicate/replayed terminal frame must not overwrite the recorded result or
        // double-count the completion counters.
        progress.update(id, &ShardUploadEvent::Result);
        let report = progress.report();
        assert_eq!(report.total_shards_synced, 1);
        assert_eq!(report.total_shards_completed, 1);
        assert_eq!(*progress.shard_upload_state.lock().unwrap().get(&id).unwrap(), ShardUploadEvent::Result);
    }

    #[test]
    fn test_shard_update_error_frame_is_ignored() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
        );
        progress.update(
            id,
            &ShardUploadEvent::Error {
                message: "boom".to_string(),
                retryable: false,
            },
        );

        let report = progress.report();
        assert_eq!(report.total_shards_synced, 0);
        assert_eq!(report.total_shards_completed, 0);
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing
            }
        );
    }

    #[test]
    fn test_shard_update_unknown_frame_is_ignored() {
        let progress = ShardUploadProgress::default();
        let id = progress.register_shard_transfer(1000);

        progress.update(
            id,
            &ShardUploadEvent::Committing {
                stage: CommitStage::Uploading,
            },
        );
        // Forward-compat unknown types must not advance counters or replace stored state.
        progress.update(id, &ShardUploadEvent::Unknown);

        let report = progress.report();
        assert_eq!(report.total_shards_uploaded_to_store, 0);
        assert_eq!(report.total_shards_synced, 0);
        assert_eq!(report.total_shards_completed, 0);
        assert_eq!(
            *progress.shard_upload_state.lock().unwrap().get(&id).unwrap(),
            ShardUploadEvent::Committing {
                stage: CommitStage::Uploading
            }
        );

        // A later known milestone still applies normally.
        progress.update(id, &ShardUploadEvent::Result);
        let report = progress.report();
        assert_eq!(report.total_shards_completed, 1);
        assert_eq!(*progress.shard_upload_state.lock().unwrap().get(&id).unwrap(), ShardUploadEvent::Result);
    }

    #[test]
    fn test_register_shard_transfer_increments_total_shards() {
        let progress = ShardUploadProgress::default();
        assert_eq!(progress.report().total_shards, 0);

        progress.register_shard_transfer(100);
        assert_eq!(progress.report().total_shards, 1);

        progress.register_shard_transfer(200);
        assert_eq!(progress.report().total_shards, 2);
    }

    #[test]
    fn test_shard_update_aggregates_validation_progress_across_multiple_shards() {
        let progress = ShardUploadProgress::default();
        let id_a = progress.register_shard_transfer(1000);
        let id_b = progress.register_shard_transfer(2000);

        progress.update(id_a, &ShardUploadEvent::Validating { verified: 2, total: 5 });
        progress.update(id_b, &ShardUploadEvent::Validating { verified: 1, total: 4 });

        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 3); // 2 + 1
        assert_eq!(report.total_shard_validation_entries, 9); // 5 + 4

        // Forward progress on one shard must not touch the other's recorded state, and the
        // aggregate counters must move only by the updated shard's own increment.
        progress.update(id_a, &ShardUploadEvent::Validating { verified: 5, total: 5 });
        let report = progress.report();
        assert_eq!(report.total_shard_validation_entries_completed, 6); // 3 + (5 - 2)
        assert_eq!(report.total_shard_validation_entries, 9); // unchanged: id_a's total was already 5

        let guard = progress.shard_upload_state.lock().unwrap();
        assert_eq!(*guard.get(&id_a).unwrap(), ShardUploadEvent::Validating { verified: 5, total: 5 });
        assert_eq!(*guard.get(&id_b).unwrap(), ShardUploadEvent::Validating { verified: 1, total: 4 });
    }
}
