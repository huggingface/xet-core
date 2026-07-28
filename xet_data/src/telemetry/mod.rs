//! Transfer performance telemetry payloads.
//!
//! Delivery, identity, and timing live in `xet_client`
//! ([`TransferTelemetry`](xet_client::cas_client::TransferTelemetry)). This module owns the metric
//! *definitions*, because it is the only place that can see [`DeduplicationMetrics`] and
//! [`GroupProgressReport`].
//!
//! [`DeduplicationMetrics`]: crate::deduplication::DeduplicationMetrics
//! [`GroupProgressReport`]: crate::progress_tracking::GroupProgressReport

mod payload;

pub use payload::{
    CommonInputs, CommonMetrics, DownloadMetrics, ERROR_CLASS_NONE, Outcome, TELEMETRY_SCHEMA_VERSION,
    TransferIdentity, UploadMetrics, error_class,
};
