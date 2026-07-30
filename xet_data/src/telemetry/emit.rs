//! Bridges a finished (or abandoned) session to the telemetry sink in `xet_client`.
//!
//! Everything here is best-effort and infallible by construction: no function returns a `Result`,
//! so a telemetry problem can never be propagated into a transfer.

use std::sync::Arc;

use xet_client::cas_client::{Client, Direction, TransferTelemetry};

use super::payload::{
    CommonInputs, CommonMetrics, DownloadMetrics, ERROR_CLASS_NONE, Outcome, TransferIdentity, UploadMetrics,
    error_class,
};
use crate::deduplication::DeduplicationMetrics;
use crate::error::DataError;
use crate::progress_tracking::GroupProgressReport;

/// Reads the aggregator off a client, if it has one.
///
/// `None` for local, in-memory, and simulation clients, for dry runs, on wasm, and whenever
/// telemetry is disabled - so every call site below is a cheap no-op in tests.
pub(crate) fn telemetry_of(client: &Arc<dyn Client + Send + Sync>) -> Option<Arc<TransferTelemetry>> {
    client.transfer_telemetry()
}

/// Same, for the `Arc<dyn Client>` the download session holds.
pub(crate) fn telemetry_of_download(client: &Arc<dyn Client>) -> Option<Arc<TransferTelemetry>> {
    client.transfer_telemetry()
}

/// Derives the outcome and error class from a session's finalize result.
fn classify<T>(result: &Result<T, DataError>) -> (Outcome, &'static str) {
    match result {
        Ok(_) => (Outcome::Ok, ERROR_CLASS_NONE),
        Err(e) => classify_error(e),
    }
}

/// Derives the outcome and error class from a failed transfer.
///
/// Split out of [`classify`] so callers that have already reduced their error to a class string -
/// notably `xet_pkg`, whose `XetError` has lost the original [`DataError`] by the time a group
/// finishes - can reach the same mapping through [`outcome_for_class`].
pub fn classify_error(error: &DataError) -> (Outcome, &'static str) {
    let class = error_class(error);
    (outcome_for_class(class), class)
}

/// Maps an error class to the outcome that should accompany it.
///
/// Cancellation is a user action, not a failure; keeping it out of `error` stops it from polluting
/// failure-rate alerts.
pub fn outcome_for_class(class: &'static str) -> Outcome {
    if class == "cancelled" {
        Outcome::Cancelled
    } else {
        Outcome::Error
    }
}

/// Everything an upload document needs beyond the transfer's own identity.
pub(crate) struct UploadSnapshot<'a> {
    pub progress: &'a GroupProgressReport,
    pub dedup: &'a DeduplicationMetrics,
    pub n_files: u64,
    /// Chunking, hashing, and xorb upload: session start until `finalize` was called.
    pub ingest_ms: u64,
    /// Shard consolidation, upload, and registration. Zero on the abandoned path, which never
    /// reached finalization.
    pub finalize_ms: u64,
}

/// Builds an upload document.
fn upload_metrics(
    telemetry: &TransferTelemetry,
    snapshot: &UploadSnapshot<'_>,
    outcome: Outcome,
    error_class: &'static str,
) -> serde_json::Value {
    let common = CommonMetrics::new(
        TransferIdentity::from(telemetry),
        CommonInputs {
            direction: Direction::Upload,
            outcome,
            error_class,
            terminal: true,
            seq: 0,
            n_files: snapshot.n_files,
            progress: snapshot.progress,
        },
    );
    to_value(UploadMetrics::new(
        common,
        snapshot.dedup,
        snapshot.progress,
        snapshot.ingest_ms,
        snapshot.finalize_ms,
    ))
}

/// Builds a download document.
fn download_metrics(
    telemetry: &TransferTelemetry,
    progress: &GroupProgressReport,
    n_files: u64,
    outcome: Outcome,
    error_class: &'static str,
) -> serde_json::Value {
    let common = CommonMetrics::new(
        TransferIdentity::from(telemetry),
        CommonInputs {
            direction: Direction::Download,
            outcome,
            error_class,
            terminal: true,
            seq: 0,
            n_files,
            progress,
        },
    );
    to_value(DownloadMetrics::new(common))
}

/// Serializes, falling back to an empty object rather than panicking.
///
/// Unreachable in practice - every field is a scalar - but a telemetry payload must never be able
/// to take down a transfer.
fn to_value<T: serde::Serialize>(metrics: T) -> serde_json::Value {
    serde_json::to_value(metrics).unwrap_or_else(|e| {
        tracing::debug!(target: "xet_telemetry", error = %e, "failed to serialize telemetry metrics");
        serde_json::Value::Object(Default::default())
    })
}

/// Emits an upload session's terminal document, waiting up to `final_flush_timeout`.
pub(crate) async fn emit_upload_terminal<T>(
    client: &Arc<dyn Client + Send + Sync>,
    result: &Result<T, DataError>,
    snapshot: UploadSnapshot<'_>,
) {
    let Some(telemetry) = telemetry_of(client) else {
        return;
    };
    let (outcome, error_class) = classify(result);
    let metrics = upload_metrics(&telemetry, &snapshot, outcome, error_class);
    telemetry.emit_terminal(Direction::Upload.terminal_event(), metrics).await;
}

/// Emits an upload session's terminal document from `Drop`, without waiting.
pub(crate) fn emit_upload_abandoned(client: &Arc<dyn Client + Send + Sync>, snapshot: UploadSnapshot<'_>) {
    let Some(telemetry) = telemetry_of(client) else {
        return;
    };
    let metrics = upload_metrics(&telemetry, &snapshot, Outcome::Aborted, ERROR_CLASS_NONE);
    telemetry.emit_terminal_detached(Direction::Upload.terminal_event(), metrics);
}

/// Emits a download session's terminal document, waiting up to `final_flush_timeout`.
///
/// Takes an already-classified `(outcome, error_class)` rather than a `Result`, because the
/// callers that know how a download ended live in `xet_pkg` and hold a `XetError`, not a
/// [`DataError`]. Use [`classify_error`] or [`outcome_for_class`] to produce the pair.
pub(crate) async fn emit_download_terminal(
    client: &Arc<dyn Client>,
    outcome: Outcome,
    error_class: &'static str,
    progress: &GroupProgressReport,
    n_files: u64,
) {
    let Some(telemetry) = telemetry_of_download(client) else {
        return;
    };
    let metrics = download_metrics(&telemetry, progress, n_files, outcome, error_class);
    telemetry.emit_terminal(Direction::Download.terminal_event(), metrics).await;
}

/// Emits a download session's terminal document from `Drop`, without waiting.
///
/// This is the only coverage for `XetDownloadStreamGroup`, which holds a `FileDownloadSession` and
/// never calls `finalize()`.
pub(crate) fn emit_download_abandoned(client: &Arc<dyn Client>, progress: &GroupProgressReport, n_files: u64) {
    let Some(telemetry) = telemetry_of_download(client) else {
        return;
    };
    let metrics = download_metrics(&telemetry, progress, n_files, Outcome::Dropped, ERROR_CLASS_NONE);
    telemetry.emit_terminal_detached(Direction::Download.terminal_event(), metrics);
}

/// Starts the heartbeat for an upload session.
///
/// `session` is held weakly: a strong reference would keep the session alive and its `Drop`-based
/// terminal report would never fire.
pub(crate) fn start_upload_heartbeat(
    ctx: &xet_runtime::core::XetContext,
    session: &Arc<crate::processing::FileUploadSession>,
) {
    let Some(telemetry) = telemetry_of(&session.client()) else {
        return;
    };
    let weak = Arc::downgrade(session);
    let identity = Arc::clone(&telemetry);

    telemetry.start_heartbeat(ctx, move |seq| {
        let session = weak.upgrade()?;
        let progress = session.report();
        let dedup = session.dedup_snapshot()?;
        let common = CommonMetrics::new(
            TransferIdentity::from(identity.as_ref()),
            CommonInputs {
                direction: Direction::Upload,
                outcome: Outcome::InProgress,
                error_class: ERROR_CLASS_NONE,
                terminal: false,
                seq,
                n_files: session.item_reports().len() as u64,
                progress: &progress,
            },
        );
        // `ingest_ms` is still accruing and `finalize_ms` has not started; zero rather than a
        // half-truth, and `duration_ms` already carries elapsed time.
        Some(to_value(UploadMetrics::new(common, &dedup, &progress, 0, 0)))
    });
}

/// Starts the heartbeat for a download session.
pub(crate) fn start_download_heartbeat(
    ctx: &xet_runtime::core::XetContext,
    session: &Arc<crate::processing::FileDownloadSession>,
) {
    let Some(telemetry) = telemetry_of_download(&session.client()) else {
        return;
    };
    let weak = Arc::downgrade(session);
    let identity = Arc::clone(&telemetry);

    telemetry.start_heartbeat(ctx, move |seq| {
        let session = weak.upgrade()?;
        let progress = session.report();
        let common = CommonMetrics::new(
            TransferIdentity::from(identity.as_ref()),
            CommonInputs {
                direction: Direction::Download,
                outcome: Outcome::InProgress,
                error_class: ERROR_CLASS_NONE,
                terminal: false,
                seq,
                n_files: session.item_reports().len() as u64,
                progress: &progress,
            },
        );
        Some(to_value(DownloadMetrics::new(common)))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_classifies_as_ok_with_no_error_class() {
        let (outcome, class) = classify::<()>(&Ok(()));
        assert_eq!(outcome, Outcome::Ok);
        assert_eq!(class, ERROR_CLASS_NONE);
    }

    #[test]
    fn test_failure_classifies_as_error() {
        let (outcome, class) = classify::<()>(&Err(DataError::InternalError("boom".into())));
        assert_eq!(outcome, Outcome::Error);
        assert_eq!(class, "internal");
    }

    /// Cancellation must not land in the `error` bucket, or user interrupts inflate failure rates.
    #[test]
    fn test_cancellation_classifies_as_cancelled_not_error() {
        let err = DataError::RuntimeError(xet_runtime::error::RuntimeError::KeyboardInterrupt);
        let (outcome, class) = classify::<()>(&Err(err));
        assert_eq!(outcome, Outcome::Cancelled);
        assert_eq!(class, "cancelled");
    }
}
