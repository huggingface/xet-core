use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport};
use xet_runtime::utils::UniqueId;

use super::{ItemProgressUpdate, ProgressUpdate, TrackingProgressUpdater};

/// Trait for types that can produce progress reports via polling.
///
/// Both `FileDownloadSession` and `FileUploadSession` implement this trait,
/// enabling the callback updaters to poll them and forward updates to
/// legacy `TrackingProgressUpdater` callbacks.
pub trait ProgressReporter: Send + Sync {
    fn report(&self) -> GroupProgressReport;
    fn item_reports(&self) -> HashMap<UniqueId, ItemProgressReport>;
    fn item_report(&self, id: UniqueId) -> Option<ItemProgressReport> {
        self.item_reports().remove(&id)
    }
}

impl ProgressReporter for xet_data::processing::FileDownloadSession {
    fn report(&self) -> GroupProgressReport {
        self.report()
    }
    fn item_reports(&self) -> HashMap<UniqueId, ItemProgressReport> {
        self.item_reports()
    }
    fn item_report(&self, id: UniqueId) -> Option<ItemProgressReport> {
        self.item_report(id)
    }
}

impl ProgressReporter for xet_data::processing::FileUploadSession {
    fn report(&self) -> GroupProgressReport {
        self.report()
    }
    fn item_reports(&self) -> HashMap<UniqueId, ItemProgressReport> {
        self.item_reports()
    }
}

// === Bridge state for group-level diffing ===

struct GroupBridgeState {
    prev_group: GroupProgressReport,
    prev_items: HashMap<UniqueId, ItemProgressReport>,
}

impl GroupBridgeState {
    fn new() -> Self {
        Self {
            prev_group: GroupProgressReport::default(),
            prev_items: HashMap::new(),
        }
    }

    fn compute_diff(
        &mut self,
        group: GroupProgressReport,
        items: HashMap<UniqueId, ItemProgressReport>,
    ) -> ProgressUpdate {
        let total_bytes_increment = group.total_bytes.saturating_sub(self.prev_group.total_bytes);
        let total_bytes_completion_increment = group
            .total_bytes_completed
            .saturating_sub(self.prev_group.total_bytes_completed);
        let total_transfer_bytes_increment =
            group.total_transfer_bytes.saturating_sub(self.prev_group.total_transfer_bytes);
        let total_transfer_bytes_completion_increment = group
            .total_transfer_bytes_completed
            .saturating_sub(self.prev_group.total_transfer_bytes_completed);

        let mut item_updates = Vec::new();
        for (&id, report) in &items {
            let prev = self.prev_items.get(&id);
            let prev_completed = prev.map_or(0, |p| p.bytes_completed);
            let increment = report.bytes_completed.saturating_sub(prev_completed);

            if increment > 0 || prev.is_none() {
                item_updates.push(ItemProgressUpdate {
                    tracking_id: id,
                    item_name: Arc::from(report.item_name.as_str()),
                    total_bytes: report.total_bytes,
                    bytes_completed: report.bytes_completed,
                    bytes_completion_increment: increment,
                });
            }
        }

        let update = ProgressUpdate {
            item_updates,
            total_bytes: group.total_bytes,
            total_bytes_increment,
            total_bytes_completed: group.total_bytes_completed,
            total_bytes_completion_increment,
            total_bytes_completion_rate: group.total_bytes_completion_rate,
            total_transfer_bytes: group.total_transfer_bytes,
            total_transfer_bytes_increment,
            total_transfer_bytes_completed: group.total_transfer_bytes_completed,
            total_transfer_bytes_completion_increment,
            total_transfer_bytes_completion_rate: group.total_transfer_bytes_completion_rate,
        };

        self.prev_group = group;
        self.prev_items = items;

        update
    }
}

// === Bridge state for single-item diffing ===

struct ItemBridgeState {
    prev: Option<ItemProgressReport>,
}

impl ItemBridgeState {
    fn new() -> Self {
        Self { prev: None }
    }

    fn compute_diff(&mut self, item_id: UniqueId, report: ItemProgressReport) -> ProgressUpdate {
        let prev_completed = self.prev.as_ref().map_or(0, |p| p.bytes_completed);
        let prev_total = self.prev.as_ref().map_or(0, |p| p.total_bytes);

        let bytes_increment = report.bytes_completed.saturating_sub(prev_completed);
        let total_increment = report.total_bytes.saturating_sub(prev_total);

        let item_updates = if bytes_increment > 0 || self.prev.is_none() {
            vec![ItemProgressUpdate {
                tracking_id: item_id,
                item_name: Arc::from(report.item_name.as_str()),
                total_bytes: report.total_bytes,
                bytes_completed: report.bytes_completed,
                bytes_completion_increment: bytes_increment,
            }]
        } else {
            Vec::new()
        };

        let update = ProgressUpdate {
            item_updates,
            total_bytes: report.total_bytes,
            total_bytes_increment: total_increment,
            total_bytes_completed: report.bytes_completed,
            total_bytes_completion_increment: bytes_increment,
            total_bytes_completion_rate: None,
            total_transfer_bytes: 0,
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: 0,
            total_transfer_bytes_completion_increment: 0,
            total_transfer_bytes_completion_rate: None,
        };

        self.prev = Some(report);
        update
    }
}

// === Shared finalization logic ===

#[cfg(debug_assertions)]
fn wrap_updater(
    updater: Arc<dyn TrackingProgressUpdater>,
) -> (Arc<dyn TrackingProgressUpdater>, Option<Arc<super::ProgressUpdaterVerificationWrapper>>) {
    let v = super::ProgressUpdaterVerificationWrapper::new(updater);
    (v.clone(), Some(v))
}

#[cfg(not(debug_assertions))]
fn wrap_updater(updater: Arc<dyn TrackingProgressUpdater>) -> (Arc<dyn TrackingProgressUpdater>, Option<()>) {
    (updater, None)
}

// === GroupProgressCallbackUpdater ===

/// Bridges the new polling-based progress model to the old callback-based model
/// at the group level.
///
/// Spawns a background task that polls a `ProgressReporter` every 250ms,
/// computes incremental diffs across all items, and sends `ProgressUpdate`
/// structs to a `TrackingProgressUpdater`.
pub struct GroupProgressCallbackUpdater {
    stop_signal: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
    #[cfg(debug_assertions)]
    verifier: Option<Arc<super::ProgressUpdaterVerificationWrapper>>,
}

impl GroupProgressCallbackUpdater {
    /// Start polling `reporter` every 250ms and send group-level diffs to `updater`.
    pub fn start(reporter: Arc<dyn ProgressReporter>, updater: Arc<dyn TrackingProgressUpdater>) -> Self {
        let (updater, _verifier) = wrap_updater(updater);

        let stop_signal = Arc::new(Notify::new());
        let stop = stop_signal.clone();

        let handle = tokio::spawn(async move {
            let mut state = GroupBridgeState::new();
            let mut interval = tokio::time::interval(Duration::from_millis(250));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let group = reporter.report();
                        let items = reporter.item_reports();
                        let update = state.compute_diff(group, items);
                        if !update.is_empty() {
                            updater.register_updates(update).await;
                        }
                    }
                    _ = stop.notified() => {
                        break;
                    }
                }
            }

            let group = reporter.report();
            let items = reporter.item_reports();
            let update = state.compute_diff(group, items);
            if !update.is_empty() {
                updater.register_updates(update).await;
            }
            updater.flush().await;
        });

        Self {
            stop_signal,
            handle,
            #[cfg(debug_assertions)]
            verifier: _verifier,
        }
    }

    /// Stop the polling loop, send a final update, and in debug mode verify completeness.
    pub async fn finalize(self) {
        self.stop_signal.notify_one();
        let _ = self.handle.await;

        #[cfg(debug_assertions)]
        if let Some(v) = self.verifier {
            v.assert_complete().await;
        }
    }
}

// === ItemProgressCallbackUpdater ===

/// Bridges the new polling-based progress model to the old callback-based model
/// for a single item.
///
/// Spawns a background task that polls a single item from a `ProgressReporter`
/// every 250ms, computes incremental diffs, and sends `ProgressUpdate` structs
/// to a `TrackingProgressUpdater`.
pub struct ItemProgressCallbackUpdater {
    stop_signal: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
    #[cfg(debug_assertions)]
    verifier: Option<Arc<super::ProgressUpdaterVerificationWrapper>>,
}

impl ItemProgressCallbackUpdater {
    /// Start polling a single item from `reporter` every 250ms and send per-item
    /// diffs to `updater`.
    pub fn start(
        reporter: Arc<dyn ProgressReporter>,
        item_id: UniqueId,
        updater: Arc<dyn TrackingProgressUpdater>,
    ) -> Self {
        let (updater, _verifier) = wrap_updater(updater);

        let stop_signal = Arc::new(Notify::new());
        let stop = stop_signal.clone();

        let handle = tokio::spawn(async move {
            let mut state = ItemBridgeState::new();
            let mut interval = tokio::time::interval(Duration::from_millis(250));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(report) = reporter.item_report(item_id) {
                            let update = state.compute_diff(item_id, report);
                            if !update.is_empty() {
                                updater.register_updates(update).await;
                            }
                        }
                    }
                    _ = stop.notified() => {
                        break;
                    }
                }
            }

            if let Some(report) = reporter.item_report(item_id) {
                let update = state.compute_diff(item_id, report);
                if !update.is_empty() {
                    updater.register_updates(update).await;
                }
            }
            updater.flush().await;
        });

        Self {
            stop_signal,
            handle,
            #[cfg(debug_assertions)]
            verifier: _verifier,
        }
    }

    /// Stop the polling loop, send a final update, and in debug mode verify completeness.
    pub async fn finalize(self) {
        self.stop_signal.notify_one();
        let _ = self.handle.await;

        #[cfg(debug_assertions)]
        if let Some(v) = self.verifier {
            v.assert_complete().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_group_report(
        total_bytes: u64,
        total_bytes_completed: u64,
        total_transfer_bytes: u64,
        total_transfer_bytes_completed: u64,
    ) -> GroupProgressReport {
        GroupProgressReport {
            total_bytes,
            total_bytes_completed,
            total_bytes_completion_rate: None,
            total_transfer_bytes,
            total_transfer_bytes_completed,
            total_transfer_bytes_completion_rate: None,
        }
    }

    fn make_item_report(name: &str, total_bytes: u64, bytes_completed: u64) -> ItemProgressReport {
        ItemProgressReport {
            item_name: name.to_string(),
            total_bytes,
            bytes_completed,
        }
    }

    #[test]
    fn test_group_bridge_first_diff() {
        let mut state = GroupBridgeState::new();
        let id = UniqueId::new();

        let group = make_group_report(1000, 200, 800, 100);
        let items = HashMap::from([(id, make_item_report("a.bin", 1000, 200))]);

        let update = state.compute_diff(group, items);

        assert_eq!(update.total_bytes, 1000);
        assert_eq!(update.total_bytes_increment, 1000);
        assert_eq!(update.total_bytes_completed, 200);
        assert_eq!(update.total_bytes_completion_increment, 200);
        assert_eq!(update.total_transfer_bytes, 800);
        assert_eq!(update.total_transfer_bytes_increment, 800);
        assert_eq!(update.total_transfer_bytes_completed, 100);
        assert_eq!(update.total_transfer_bytes_completion_increment, 100);
        assert_eq!(update.item_updates.len(), 1);
        assert_eq!(update.item_updates[0].total_bytes, 1000);
        assert_eq!(update.item_updates[0].bytes_completed, 200);
        assert_eq!(update.item_updates[0].bytes_completion_increment, 200);
    }

    #[test]
    fn test_group_bridge_incremental_diff() {
        let mut state = GroupBridgeState::new();
        let id = UniqueId::new();

        let group1 = make_group_report(1000, 200, 800, 100);
        let items1 = HashMap::from([(id, make_item_report("a.bin", 1000, 200))]);
        state.compute_diff(group1, items1);

        let group2 = make_group_report(1000, 600, 800, 400);
        let items2 = HashMap::from([(id, make_item_report("a.bin", 1000, 600))]);
        let update = state.compute_diff(group2, items2);

        assert_eq!(update.total_bytes_increment, 0);
        assert_eq!(update.total_bytes_completion_increment, 400);
        assert_eq!(update.total_transfer_bytes_increment, 0);
        assert_eq!(update.total_transfer_bytes_completion_increment, 300);
        assert_eq!(update.item_updates.len(), 1);
        assert_eq!(update.item_updates[0].bytes_completion_increment, 400);
    }

    #[test]
    fn test_group_bridge_no_change_is_empty() {
        let mut state = GroupBridgeState::new();
        let id = UniqueId::new();

        let group = make_group_report(1000, 500, 800, 300);
        let items = HashMap::from([(id, make_item_report("a.bin", 1000, 500))]);
        state.compute_diff(group.clone(), items.clone());

        let update = state.compute_diff(group, items);

        assert!(update.is_empty());
    }

    #[test]
    fn test_group_bridge_new_item_appears() {
        let mut state = GroupBridgeState::new();
        let id1 = UniqueId::new();
        let id2 = UniqueId::new();

        let group1 = make_group_report(100, 50, 0, 0);
        let items1 = HashMap::from([(id1, make_item_report("a.bin", 100, 50))]);
        state.compute_diff(group1, items1);

        let group2 = make_group_report(300, 50, 0, 0);
        let items2 = HashMap::from([
            (id1, make_item_report("a.bin", 100, 50)),
            (id2, make_item_report("b.bin", 200, 0)),
        ]);
        let update = state.compute_diff(group2, items2);

        assert_eq!(update.total_bytes_increment, 200);
        assert_eq!(update.item_updates.len(), 1);
        assert_eq!(update.item_updates[0].tracking_id, id2);
        assert_eq!(update.item_updates[0].bytes_completion_increment, 0);
    }

    #[test]
    fn test_item_bridge_first_diff() {
        let mut state = ItemBridgeState::new();
        let id = UniqueId::new();
        let report = make_item_report("file.bin", 500, 100);

        let update = state.compute_diff(id, report);

        assert_eq!(update.total_bytes, 500);
        assert_eq!(update.total_bytes_increment, 500);
        assert_eq!(update.total_bytes_completed, 100);
        assert_eq!(update.total_bytes_completion_increment, 100);
        assert_eq!(update.item_updates.len(), 1);
        assert_eq!(update.item_updates[0].bytes_completion_increment, 100);
    }

    #[test]
    fn test_item_bridge_incremental_diff() {
        let mut state = ItemBridgeState::new();
        let id = UniqueId::new();

        state.compute_diff(id, make_item_report("file.bin", 500, 100));

        let update = state.compute_diff(id, make_item_report("file.bin", 500, 350));

        assert_eq!(update.total_bytes_increment, 0);
        assert_eq!(update.total_bytes_completion_increment, 250);
        assert_eq!(update.item_updates[0].bytes_completion_increment, 250);
    }

    #[test]
    fn test_item_bridge_no_change_is_empty() {
        let mut state = ItemBridgeState::new();
        let id = UniqueId::new();

        state.compute_diff(id, make_item_report("file.bin", 500, 200));
        let update = state.compute_diff(id, make_item_report("file.bin", 500, 200));

        assert!(update.is_empty());
    }

    #[test]
    fn test_item_bridge_total_grows() {
        let mut state = ItemBridgeState::new();
        let id = UniqueId::new();

        state.compute_diff(id, make_item_report("file.bin", 500, 100));
        let update = state.compute_diff(id, make_item_report("file.bin", 800, 100));

        assert_eq!(update.total_bytes, 800);
        assert_eq!(update.total_bytes_increment, 300);
        assert_eq!(update.total_bytes_completion_increment, 0);
        assert!(update.item_updates.is_empty());
    }
}
