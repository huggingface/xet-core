use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};

use super::progress_tracking::{ItemProgressUpdate, ProgressUpdate, TrackingProgressUpdater};

/// Trait for types that can produce progress reports via polling.
///
/// Both `FileDownloadSession` and `FileUploadSession` implement this trait,
/// enabling the `CallbackBridge` to poll them and forward updates to
/// legacy `TrackingProgressUpdater` callbacks.
pub trait ProgressReporter: Send + Sync {
    fn report(&self) -> GroupProgressReport;
    fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport>;
    fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        self.item_reports().remove(&id)
    }
}

impl ProgressReporter for xet_data::processing::FileDownloadSession {
    fn report(&self) -> GroupProgressReport {
        self.report()
    }
    fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        self.item_reports()
    }
    fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        self.item_report(id)
    }
}

impl ProgressReporter for xet_data::processing::FileUploadSession {
    fn report(&self) -> GroupProgressReport {
        self.report()
    }
    fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        self.item_reports()
    }
}

// === Bridge state for group-level diffing ===

struct GroupBridgeState {
    prev_group: GroupProgressReport,
    prev_items: HashMap<UniqueID, ItemProgressReport>,
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
        items: HashMap<UniqueID, ItemProgressReport>,
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

    fn compute_diff(&mut self, item_id: UniqueID, report: ItemProgressReport) -> ProgressUpdate {
        let prev_completed = self.prev.as_ref().map_or(0, |p| p.bytes_completed);
        let prev_total = self.prev.as_ref().map_or(0, |p| p.total_bytes);

        let bytes_increment = report.bytes_completed.saturating_sub(prev_completed);
        let total_increment = report.total_bytes.saturating_sub(prev_total);

        let update = ProgressUpdate {
            item_updates: vec![ItemProgressUpdate {
                tracking_id: item_id,
                item_name: Arc::from(report.item_name.as_str()),
                total_bytes: report.total_bytes,
                bytes_completed: report.bytes_completed,
                bytes_completion_increment: bytes_increment,
            }],
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

// === CallbackBridge ===

/// Bridges the new polling-based progress model to the old callback-based model.
///
/// Spawns a background task that polls a `ProgressReporter` every 250ms,
/// computes incremental diffs, and sends `ProgressUpdate` structs to a
/// `TrackingProgressUpdater`.
pub struct CallbackBridge {
    stop_signal: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
    #[cfg(debug_assertions)]
    verifier: Option<Arc<super::progress_verification_wrapper::ProgressUpdaterVerificationWrapper>>,
}

impl CallbackBridge {
    #[cfg(debug_assertions)]
    fn wrap(
        updater: Arc<dyn TrackingProgressUpdater>,
    ) -> (
        Arc<dyn TrackingProgressUpdater>,
        Option<Arc<super::progress_verification_wrapper::ProgressUpdaterVerificationWrapper>>,
    ) {
        let v = super::progress_verification_wrapper::ProgressUpdaterVerificationWrapper::new(updater);
        (v.clone(), Some(v))
    }

    #[cfg(not(debug_assertions))]
    fn wrap(updater: Arc<dyn TrackingProgressUpdater>) -> (Arc<dyn TrackingProgressUpdater>, Option<()>) {
        (updater, None)
    }

    /// Start a bridge that polls `reporter` every 250ms and sends group-level
    /// diffs to `updater`. Used for uploads where one updater receives all progress.
    pub fn start(reporter: Arc<dyn ProgressReporter>, updater: Arc<dyn TrackingProgressUpdater>) -> Self {
        let (updater, _verifier) = Self::wrap(updater);

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

    /// Start a bridge that polls a single item from `reporter` every 250ms and sends
    /// per-item diffs to `updater`. Used for downloads where each file has its own updater.
    pub fn start_for_item(
        reporter: Arc<dyn ProgressReporter>,
        item_id: UniqueID,
        updater: Arc<dyn TrackingProgressUpdater>,
    ) -> Self {
        let (updater, _verifier) = Self::wrap(updater);

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
