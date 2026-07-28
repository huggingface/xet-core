//! Transfer performance telemetry payloads.
//!
//! Delivery, identity, and timing live in `xet_client`
//! ([`TransferTelemetry`](xet_client::cas_client::TransferTelemetry)). This module owns the metric
//! *definitions*, because it is the only place that can see [`DeduplicationMetrics`] and
//! [`GroupProgressReport`].
//!
//! [`DeduplicationMetrics`]: crate::deduplication::DeduplicationMetrics
//! [`GroupProgressReport`]: crate::progress_tracking::GroupProgressReport

mod emit;
mod payload;

pub(crate) use emit::{
    UploadSnapshot, emit_download_abandoned, emit_download_terminal, emit_upload_abandoned, emit_upload_terminal,
    start_download_heartbeat, start_upload_heartbeat,
};
pub use payload::{
    CommonInputs, CommonMetrics, DownloadMetrics, ERROR_CLASS_NONE, Outcome, TELEMETRY_SCHEMA_VERSION,
    TransferIdentity, UploadMetrics, error_class,
};
