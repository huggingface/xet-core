//! Transfer performance telemetry payloads.
//!
//! Delivery, identity, and timing live in `xet_client`
//! ([`TransferTelemetry`](xet_client::cas_client::TransferTelemetry)). This module owns the metric
//! *definitions*, because it is the only place that can see [`DeduplicationMetrics`] and
//! [`GroupProgressReport`].
//!
//! [`DeduplicationMetrics`]: crate::deduplication::DeduplicationMetrics
//! [`GroupProgressReport`]: crate::progress_tracking::GroupProgressReport
//!
//! # Targets
//!
//! Only [`outcome`] compiles everywhere. Everything else depends on `TransferTelemetry`, which
//! does not exist on wasm, so it is gated - matching `xet_client::cas_client::telemetry`. The
//! outcome vocabulary stays ungated because it appears in `FileDownloadSession`'s public
//! signatures, and gating it would push `#[cfg]` onto every caller that merely names an outcome.

mod outcome;
#[cfg(not(target_family = "wasm"))]
mod payload;

pub use outcome::{ERROR_CLASS_NONE, Outcome, classify_error, error_class, outcome_for_class};
#[cfg(not(target_family = "wasm"))]
pub use payload::{
    CommonInputs, CommonMetrics, DownloadMetrics, TELEMETRY_SCHEMA_VERSION, TransferIdentity, UploadMetrics,
};
