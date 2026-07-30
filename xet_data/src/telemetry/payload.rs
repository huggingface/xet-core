//! The metric vocabulary sent to `POST /v1/telemetry`.
//!
//! # Rules for changing anything in this file
//!
//! Consumers assign each property a field type on first sight and cannot change it in place
//! afterwards. That makes the constraints asymmetric:
//!
//! - Adding a key is safe.
//! - Changing an existing key's JSON type is **not**: it breaks ingestion for every document carrying the new type, and
//!   recovering means rebuilding the stored data. If a key's meaning or unit changes, introduce a new key instead
//!   (`duration_ms` never becomes a float; a microsecond variant would be `duration_us`).
//! - Removing a key silently breaks dashboards and alerts.
//!
//! `test_upload_key_set_is_exact` / `test_download_key_set_is_exact` pin the key sets and
//! `test_numeric_types_stable` pins the types, so any of the above fails the build rather than
//! production.
//!
//! Every value is a `u64`, `f64`, `bool`, or `String` - never null, never nested, never an array.
//! No file names, paths, hashes, repository ids, or user ids appear here; the server derives
//! identity from the request's JWT.

use serde::Serialize;
use xet_client::cas_client::{Direction, TransferTelemetry};

use crate::deduplication::DeduplicationMetrics;
use crate::error::DataError;
use crate::progress_tracking::GroupProgressReport;

/// Bumped when keys are added. Query-side branching hangs off this; it is not a wire version.
pub const TELEMETRY_SCHEMA_VERSION: u64 = 1;

/// How a transfer ended.
///
/// A closed set: these strings are grouped on, so they must not drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Finalized successfully.
    Ok,
    /// Finalized with an error.
    Error,
    /// Cancelled by the user, or its task tree was cancelled.
    Cancelled,
    /// An upload session dropped without finalizing.
    Aborted,
    /// A download session dropped without finalizing - notably every `XetDownloadStreamGroup`,
    /// which has no explicit `finish()`.
    Dropped,
    /// A heartbeat from a transfer still running.
    InProgress,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error => "error",
            Outcome::Cancelled => "cancelled",
            Outcome::Aborted => "aborted",
            Outcome::Dropped => "dropped",
            Outcome::InProgress => "in_progress",
        }
    }
}

/// Value of `error_class` when nothing went wrong. Not the empty string, so the field is always
/// groupable without a null-ish bucket.
pub const ERROR_CLASS_NONE: &str = "none";

/// Buckets a [`DataError`] into a small closed vocabulary.
///
/// Deliberately coarse. The point is to answer "are uploads failing more than they were, and is it
/// the network or the server", not to reproduce the error text - which could contain paths.
pub fn error_class(error: &DataError) -> &'static str {
    use xet_client::cas_client::exports::reqwest;
    use xet_client::error::ClientError;
    use xet_runtime::error::RuntimeError;

    /// Status wins over transport: a 429 is the server shedding load, not a network fault.
    fn reqwest_error_class(error: &reqwest::Error) -> &'static str {
        if let Some(status) = error.status() {
            return if status.as_u16() == 429 {
                "rate_limited"
            } else if status.is_server_error() {
                "server_error"
            } else if status.as_u16() == 404 {
                "not_found"
            } else {
                "other"
            };
        }
        if error.is_timeout() { "timeout" } else { "network" }
    }

    fn client_error_class(error: &ClientError) -> &'static str {
        match error {
            ClientError::AuthError(_) => "auth",
            ClientError::IOError(_) => "io",
            ClientError::FormatError(_) => "format",
            ClientError::FileNotFound(_) | ClientError::XORBNotFound(_) => "not_found",
            ClientError::InternalError(_) => "internal",
            ClientError::ReqwestMiddlewareError(_) => "network",
            ClientError::ReqwestError(e, _) => reqwest_error_class(e),
            _ => "other",
        }
    }

    match error {
        DataError::AuthError(_) => "auth",
        DataError::IOError(_) => "io",
        DataError::FormatError(_) | DataError::HashStringParsingFailure(_) | DataError::FileNotCleanedError(_) => {
            "format"
        },
        DataError::HashNotFound => "not_found",
        DataError::RuntimeError(RuntimeError::TaskCanceled(_) | RuntimeError::KeyboardInterrupt) => "cancelled",
        DataError::RuntimeError(_) => "internal",
        DataError::JoinError(e) if e.is_cancelled() => "cancelled",
        DataError::JoinError(_) => "internal",
        DataError::InternalError(_) | DataError::SyncError(_) | DataError::InvalidOperation(_) => "internal",
        DataError::ClientError(e) => client_error_class(e),
        _ => "other",
    }
}

/// Divides, guaranteeing a finite `f64`.
///
/// `serde_json` serializes NaN and infinity as `null`, which would break the type stability the
/// module docs describe - a single such document can poison a consumer's field type. Every ratio and
/// rate in this file goes through here; there are no exceptions.
///
/// Rounded to four decimal places to keep documents small and diffs readable.
#[inline]
pub(crate) fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    finite(numerator as f64 / denominator as f64)
}

/// Bytes per second over a millisecond duration, guaranteed finite.
#[inline]
pub(crate) fn rate_bps(bytes: u64, duration_ms: u64) -> f64 {
    if duration_ms == 0 {
        return 0.0;
    }
    finite(bytes as f64 * 1000.0 / duration_ms as f64)
}

#[inline]
fn finite(value: f64) -> f64 {
    if value.is_finite() {
        (value * 10_000.0).round() / 10_000.0
    } else {
        0.0
    }
}

/// Keys present in every telemetry document, in both directions, always.
#[derive(Debug, Clone, Serialize)]
pub struct CommonMetrics {
    pub schema_version: u64,
    pub direction: &'static str,
    /// Unique per transfer. A single `XetSession` id can cover both an upload commit and a
    /// download group, so `session_id` alone does not identify a transfer.
    pub transfer_id: String,
    /// Exactly one document per `transfer_id` carries `true`. Filter on it to get one row per
    /// transfer without any group-by.
    pub terminal: bool,
    /// 0 for the first document; increments per heartbeat.
    pub seq: u64,

    pub client_version: &'static str,
    pub os: &'static str,
    pub arch: &'static str,
    pub cpu_count: u64,
    /// Host component only.
    pub endpoint_host: String,
    pub dry_run: bool,

    pub duration_ms: u64,
    pub outcome: &'static str,
    pub error_class: &'static str,

    pub n_files: u64,
    /// Logical bytes, before dedup and compression.
    pub total_bytes: u64,
    pub total_bytes_completed: u64,
    /// Bytes actually moved over the wire.
    pub transfer_bytes: u64,
    pub transfer_bytes_completed: u64,
    /// Wire throughput over the whole transfer. Deterministic, unlike the EWMA below.
    pub throughput_bps: f64,
    pub logical_throughput_bps: f64,
    /// The client's own EWMA estimate, for comparison against the wall-clock figures. Zero rather
    /// than absent when the sampler never had enough observations.
    pub ewma_throughput_bps: f64,

    pub peak_concurrency: u64,
}

/// What the caller must supply that cannot be read off the transfer or the progress report.
pub struct CommonInputs<'a> {
    pub direction: Direction,
    pub outcome: Outcome,
    pub error_class: &'static str,
    pub terminal: bool,
    pub seq: u64,
    pub n_files: u64,
    pub progress: &'a GroupProgressReport,
}

/// Identity and timing, snapshotted from [`TransferTelemetry`].
///
/// Taken as plain data rather than a `&TransferTelemetry` so this module stays independently
/// testable: `TransferTelemetry` needs a `XetContext` and a live HTTP client to construct, which a
/// payload unit test has no business setting up.
pub struct TransferIdentity {
    pub transfer_id: String,
    pub endpoint_host: String,
    pub dry_run: bool,
    pub duration_ms: u64,
    pub peak_concurrency: u64,
}

impl From<&TransferTelemetry> for TransferIdentity {
    fn from(telemetry: &TransferTelemetry) -> Self {
        Self {
            transfer_id: telemetry.transfer_id().to_owned(),
            endpoint_host: telemetry.endpoint_host().to_owned(),
            dry_run: telemetry.dry_run(),
            // Saturating: `as u64` on an out-of-range u128 would wrap.
            duration_ms: u64::try_from(telemetry.elapsed().as_millis()).unwrap_or(u64::MAX),
            peak_concurrency: telemetry.peak_concurrency(),
        }
    }
}

impl CommonMetrics {
    pub fn new(identity: TransferIdentity, inputs: CommonInputs<'_>) -> Self {
        let progress = inputs.progress;
        let duration_ms = identity.duration_ms;

        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            direction: inputs.direction.as_str(),
            transfer_id: identity.transfer_id,
            terminal: inputs.terminal,
            seq: inputs.seq,

            client_version: env!("CARGO_PKG_VERSION"),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpu_count: std::thread::available_parallelism().map(|n| n.get() as u64).unwrap_or(0),
            endpoint_host: identity.endpoint_host,
            dry_run: identity.dry_run,

            duration_ms,
            outcome: inputs.outcome.as_str(),
            error_class: inputs.error_class,

            n_files: inputs.n_files,
            total_bytes: progress.total_bytes,
            total_bytes_completed: progress.total_bytes_completed,
            transfer_bytes: progress.total_transfer_bytes,
            transfer_bytes_completed: progress.total_transfer_bytes_completed,
            throughput_bps: rate_bps(progress.total_transfer_bytes_completed, duration_ms),
            logical_throughput_bps: rate_bps(progress.total_bytes_completed, duration_ms),
            ewma_throughput_bps: progress.total_transfer_bytes_completion_rate.map(finite).unwrap_or(0.0),

            peak_concurrency: identity.peak_concurrency,
        }
    }
}

/// Upload documents: [`CommonMetrics`] plus dedup effectiveness and shard finalization.
#[derive(Debug, Clone, Serialize)]
pub struct UploadMetrics {
    #[serde(flatten)]
    pub common: CommonMetrics,

    pub dedup_bytes: u64,
    pub new_bytes: u64,
    pub global_dedup_bytes: u64,
    pub defrag_prevented_dedup_bytes: u64,
    pub total_chunks: u64,
    pub dedup_chunks: u64,
    pub new_chunks: u64,
    pub global_dedup_chunks: u64,
    pub defrag_prevented_dedup_chunks: u64,

    pub xorb_bytes_uploaded: u64,
    pub shard_bytes_uploaded: u64,

    pub shards_total: u64,
    pub shards_completed: u64,
    pub shard_validation_entries: u64,

    /// Share of logical bytes avoided by dedup.
    pub dedup_ratio: f64,
    /// Compressed xorb bytes over the new bytes that produced them.
    pub compression_ratio: f64,

    /// Chunking, hashing, and xorb upload: session start until `finalize` was called.
    pub ingest_ms: u64,
    /// Shard consolidation, upload, and registration.
    pub finalize_ms: u64,
}

impl UploadMetrics {
    pub fn new(
        common: CommonMetrics,
        dedup: &DeduplicationMetrics,
        progress: &GroupProgressReport,
        ingest_ms: u64,
        finalize_ms: u64,
    ) -> Self {
        // `shard` is None for dry runs and for callers that predate the shard progress section;
        // zero is the correct reading in both cases, and keeps the key set fixed.
        let shard = progress.shard.as_ref();

        Self {
            dedup_bytes: dedup.deduped_bytes,
            new_bytes: dedup.new_bytes,
            global_dedup_bytes: dedup.deduped_bytes_by_global_dedup,
            defrag_prevented_dedup_bytes: dedup.defrag_prevented_dedup_bytes,
            total_chunks: dedup.total_chunks,
            dedup_chunks: dedup.deduped_chunks,
            new_chunks: dedup.new_chunks,
            global_dedup_chunks: dedup.deduped_chunks_by_global_dedup,
            defrag_prevented_dedup_chunks: dedup.defrag_prevented_dedup_chunks,

            xorb_bytes_uploaded: dedup.xorb_bytes_uploaded,
            shard_bytes_uploaded: dedup.shard_bytes_uploaded,

            shards_total: shard.map(|s| s.total_shards as u64).unwrap_or(0),
            shards_completed: shard.map(|s| s.total_shards_completed as u64).unwrap_or(0),
            shard_validation_entries: shard.map(|s| s.total_shard_validation_entries).unwrap_or(0),

            dedup_ratio: ratio(dedup.deduped_bytes, dedup.total_bytes),
            compression_ratio: ratio(dedup.xorb_bytes_uploaded, dedup.new_bytes),

            ingest_ms,
            finalize_ms,
            common,
        }
    }
}

/// Download documents: [`CommonMetrics`] plus how much the wire bytes expanded on disk.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadMetrics {
    #[serde(flatten)]
    pub common: CommonMetrics,

    /// Logical bytes produced per wire byte - dedup and compression combined, from the
    /// downloader's side.
    pub expansion_ratio: f64,
}

impl DownloadMetrics {
    pub fn new(common: CommonMetrics) -> Self {
        Self {
            expansion_ratio: ratio(common.total_bytes_completed, common.transfer_bytes_completed),
            common,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    /// The upload key set. Changing this list is a schema change - read the module docs first.
    const UPLOAD_KEYS: &[&str] = &[
        "arch",
        "client_version",
        "compression_ratio",
        "cpu_count",
        "dedup_bytes",
        "dedup_chunks",
        "dedup_ratio",
        "defrag_prevented_dedup_bytes",
        "defrag_prevented_dedup_chunks",
        "direction",
        "dry_run",
        "duration_ms",
        "endpoint_host",
        "error_class",
        "ewma_throughput_bps",
        "finalize_ms",
        "global_dedup_bytes",
        "global_dedup_chunks",
        "ingest_ms",
        "logical_throughput_bps",
        "n_files",
        "new_bytes",
        "new_chunks",
        "os",
        "outcome",
        "peak_concurrency",
        "schema_version",
        "seq",
        "shard_bytes_uploaded",
        "shard_validation_entries",
        "shards_completed",
        "shards_total",
        "terminal",
        "throughput_bps",
        "total_bytes",
        "total_bytes_completed",
        "total_chunks",
        "transfer_bytes",
        "transfer_bytes_completed",
        "transfer_id",
        "xorb_bytes_uploaded",
    ];

    /// The download key set. Changing this list is a schema change - read the module docs first.
    const DOWNLOAD_KEYS: &[&str] = &[
        "arch",
        "client_version",
        "cpu_count",
        "direction",
        "dry_run",
        "duration_ms",
        "endpoint_host",
        "error_class",
        "ewma_throughput_bps",
        "expansion_ratio",
        "logical_throughput_bps",
        "n_files",
        "os",
        "outcome",
        "peak_concurrency",
        "schema_version",
        "seq",
        "terminal",
        "throughput_bps",
        "total_bytes",
        "total_bytes_completed",
        "transfer_bytes",
        "transfer_bytes_completed",
        "transfer_id",
    ];

    /// The JSON type every key must always have. A key that changes type here breaks the
    /// consumer's field type and starts breaking ingestion.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Kind {
        U64,
        F64,
        Bool,
        Str,
    }

    const TYPES: &[(&str, Kind)] = &[
        ("arch", Kind::Str),
        ("client_version", Kind::Str),
        ("compression_ratio", Kind::F64),
        ("cpu_count", Kind::U64),
        ("dedup_bytes", Kind::U64),
        ("dedup_chunks", Kind::U64),
        ("dedup_ratio", Kind::F64),
        ("defrag_prevented_dedup_bytes", Kind::U64),
        ("defrag_prevented_dedup_chunks", Kind::U64),
        ("direction", Kind::Str),
        ("dry_run", Kind::Bool),
        ("duration_ms", Kind::U64),
        ("endpoint_host", Kind::Str),
        ("error_class", Kind::Str),
        ("ewma_throughput_bps", Kind::F64),
        ("expansion_ratio", Kind::F64),
        ("finalize_ms", Kind::U64),
        ("global_dedup_bytes", Kind::U64),
        ("global_dedup_chunks", Kind::U64),
        ("ingest_ms", Kind::U64),
        ("logical_throughput_bps", Kind::F64),
        ("n_files", Kind::U64),
        ("new_bytes", Kind::U64),
        ("new_chunks", Kind::U64),
        ("os", Kind::Str),
        ("outcome", Kind::Str),
        ("peak_concurrency", Kind::U64),
        ("schema_version", Kind::U64),
        ("seq", Kind::U64),
        ("shard_bytes_uploaded", Kind::U64),
        ("shard_validation_entries", Kind::U64),
        ("shards_completed", Kind::U64),
        ("shards_total", Kind::U64),
        ("terminal", Kind::Bool),
        ("throughput_bps", Kind::F64),
        ("total_bytes", Kind::U64),
        ("total_bytes_completed", Kind::U64),
        ("total_chunks", Kind::U64),
        ("transfer_bytes", Kind::U64),
        ("transfer_bytes_completed", Kind::U64),
        ("transfer_id", Kind::Str),
        ("xorb_bytes_uploaded", Kind::U64),
    ];

    fn identity() -> TransferIdentity {
        TransferIdentity {
            transfer_id: "0199-transfer".into(),
            endpoint_host: "cas.example.com".into(),
            dry_run: false,
            duration_ms: 4_000,
            peak_concurrency: 16,
        }
    }

    fn inputs(progress: &GroupProgressReport) -> CommonInputs<'_> {
        CommonInputs {
            direction: Direction::Upload,
            outcome: Outcome::Ok,
            error_class: ERROR_CLASS_NONE,
            terminal: true,
            seq: 3,
            n_files: 7,
            progress,
        }
    }

    /// Values chosen so no field is coincidentally zero: a zero `f64` still serializes as `0.0`,
    /// but distinct values make a mis-mapped field obvious.
    fn common() -> CommonMetrics {
        CommonMetrics {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            direction: Direction::Upload.as_str(),
            transfer_id: "0199-transfer".into(),
            terminal: true,
            seq: 3,
            client_version: "1.2.3",
            os: "linux",
            arch: "x86_64",
            cpu_count: 8,
            endpoint_host: "cas.example.com".into(),
            dry_run: false,
            duration_ms: 4_000,
            outcome: Outcome::Ok.as_str(),
            error_class: ERROR_CLASS_NONE,
            n_files: 7,
            total_bytes: 1_000,
            total_bytes_completed: 900,
            transfer_bytes: 500,
            transfer_bytes_completed: 400,
            throughput_bps: 100.0,
            logical_throughput_bps: 225.0,
            ewma_throughput_bps: 111.5,
            peak_concurrency: 16,
        }
    }

    fn dedup() -> DeduplicationMetrics {
        DeduplicationMetrics {
            total_bytes: 1_000,
            deduped_bytes: 400,
            new_bytes: 600,
            deduped_bytes_by_global_dedup: 100,
            defrag_prevented_dedup_bytes: 10,
            total_chunks: 50,
            deduped_chunks: 20,
            new_chunks: 30,
            deduped_chunks_by_global_dedup: 5,
            defrag_prevented_dedup_chunks: 1,
            xorb_bytes_uploaded: 300,
            shard_bytes_uploaded: 25,
            total_bytes_uploaded: 325,
        }
    }

    fn progress() -> GroupProgressReport {
        GroupProgressReport {
            total_bytes: 1_000,
            total_bytes_completed: 900,
            total_bytes_completion_rate: Some(225.0),
            total_transfer_bytes: 500,
            total_transfer_bytes_completed: 400,
            total_transfer_bytes_completion_rate: Some(111.5),
            shard: Some(crate::progress_tracking::ShardUploadProgressReport {
                total_shard_bytes: 25,
                total_shard_bytes_upload_completed: 25,
                total_shards: 2,
                total_shard_validation_entries: 9,
                total_shard_validation_entries_completed: 9,
                total_shards_uploaded_to_store: 2,
                total_shards_synced: 2,
                total_shards_completed: 2,
            }),
        }
    }

    fn upload_json() -> Value {
        serde_json::to_value(UploadMetrics::new(common(), &dedup(), &progress(), 3_500, 500)).unwrap()
    }

    fn download_json() -> Value {
        let mut c = common();
        c.direction = Direction::Download.as_str();
        serde_json::to_value(DownloadMetrics::new(c)).unwrap()
    }

    fn sorted_keys(v: &Value) -> Vec<String> {
        let mut keys: Vec<_> = v.as_object().expect("metrics must be an object").keys().cloned().collect();
        keys.sort();
        keys
    }

    #[test]
    fn test_upload_key_set_is_exact() {
        assert_eq!(sorted_keys(&upload_json()), UPLOAD_KEYS);
    }

    #[test]
    fn test_download_key_set_is_exact() {
        assert_eq!(sorted_keys(&download_json()), DOWNLOAD_KEYS);
    }

    /// Guards the field-typing hazard described in the module docs.
    #[test]
    fn test_numeric_types_stable() {
        let types: std::collections::HashMap<_, _> = TYPES.iter().copied().collect();

        for doc in [upload_json(), download_json()] {
            for (key, value) in doc.as_object().unwrap() {
                let expected = types
                    .get(key.as_str())
                    .unwrap_or_else(|| panic!("key {key} missing from TYPES"));
                let actual = match value {
                    Value::Bool(_) => Kind::Bool,
                    Value::String(_) => Kind::Str,
                    Value::Number(n) if n.is_f64() => Kind::F64,
                    Value::Number(_) => Kind::U64,
                    other => panic!("{key} serialized as {other:?}; only scalars are allowed"),
                };
                assert_eq!(actual, *expected, "{key} changed JSON type");
            }
        }
    }

    #[test]
    fn test_no_null_arrays_or_nesting() {
        for doc in [upload_json(), download_json()] {
            for (key, value) in doc.as_object().unwrap() {
                assert!(
                    matches!(value, Value::Bool(_) | Value::String(_) | Value::Number(_)),
                    "{key} must be a scalar, got {value:?}"
                );
            }
        }
    }

    /// `serde_json` renders NaN and infinity as `null`, which would poison the field's type.
    #[test]
    fn test_ratios_are_finite_for_degenerate_inputs() {
        assert_eq!(ratio(5, 0), 0.0);
        assert_eq!(ratio(0, 0), 0.0);
        assert_eq!(rate_bps(5, 0), 0.0);
        assert!(ratio(u64::MAX, 1).is_finite());
        assert!(rate_bps(u64::MAX, 1).is_finite());

        // An all-zero transfer must still produce numbers, not nulls. Built through the real
        // constructor so the rates are actually computed rather than taken from a literal.
        let empty = GroupProgressReport::default();
        let common = CommonMetrics::new(
            TransferIdentity {
                duration_ms: 0,
                peak_concurrency: 0,
                ..identity()
            },
            inputs(&empty),
        );
        let doc =
            serde_json::to_value(UploadMetrics::new(common, &DeduplicationMetrics::default(), &empty, 0, 0)).unwrap();

        for (key, value) in doc.as_object().unwrap() {
            assert!(!value.is_null(), "{key} serialized as null");
        }
        assert_eq!(doc["dedup_ratio"], 0.0);
        assert_eq!(doc["compression_ratio"], 0.0);
        assert_eq!(doc["throughput_bps"], 0.0);
    }

    /// Nothing identifying may reach the wire. The server derives repo and user from the JWT.
    #[test]
    fn test_no_pii_in_payload() {
        let doc = serde_json::to_string(&upload_json()).unwrap();
        for sentinel in [
            "/home/",
            "/Users/",
            "C:\\",
            ".safetensors",
            "secret-repo",
            "user@example.com",
            "Bearer ",
        ] {
            assert!(!doc.contains(sentinel), "payload leaked {sentinel}: {doc}");
        }
    }

    #[test]
    fn test_derived_values() {
        let doc = upload_json();
        // 400 deduped of 1000 logical.
        assert_eq!(doc["dedup_ratio"], 0.4);
        // 300 xorb bytes from 600 new bytes.
        assert_eq!(doc["compression_ratio"], 0.5);
        // 400 wire bytes over 4s.
        assert_eq!(doc["throughput_bps"], 100.0);
        assert_eq!(doc["shards_total"], 2);
        assert_eq!(doc["shard_validation_entries"], 9);
    }

    /// Absent shard progress reads as zero rather than dropping the keys.
    #[test]
    fn test_missing_shard_progress_reads_as_zero() {
        let doc = serde_json::to_value(UploadMetrics::new(common(), &dedup(), &GroupProgressReport::default(), 0, 0))
            .unwrap();
        assert_eq!(doc["shards_total"], 0);
        assert_eq!(doc["shards_completed"], 0);
        assert_eq!(doc["shard_validation_entries"], 0);
        assert_eq!(sorted_keys(&doc), UPLOAD_KEYS);
    }

    /// A missing EWMA reads as zero, never as `null`, so the type never varies.
    #[test]
    fn test_absent_ewma_rate_is_zero_not_null() {
        let mut p = progress();
        p.total_transfer_bytes_completion_rate = None;

        let metrics = CommonMetrics::new(identity(), inputs(&p));
        assert_eq!(metrics.ewma_throughput_bps, 0.0);
        assert_eq!(serde_json::to_value(&metrics).unwrap()["ewma_throughput_bps"], 0.0);
    }

    /// The wall-clock rates are computed from the identity's duration, not the EWMA.
    #[test]
    fn test_throughput_computed_from_duration() {
        let metrics = CommonMetrics::new(identity(), inputs(&progress()));
        // 400 wire bytes and 900 logical bytes over 4000ms.
        assert_eq!(metrics.throughput_bps, 100.0);
        assert_eq!(metrics.logical_throughput_bps, 225.0);
    }

    /// A transfer that somehow reports no elapsed time must not divide by zero.
    #[test]
    fn test_zero_duration_yields_zero_rates() {
        let metrics = CommonMetrics::new(
            TransferIdentity {
                duration_ms: 0,
                ..identity()
            },
            inputs(&progress()),
        );
        assert_eq!(metrics.throughput_bps, 0.0);
        assert_eq!(metrics.logical_throughput_bps, 0.0);
    }

    #[test]
    fn test_expansion_ratio_uses_completed_bytes() {
        let doc = download_json();
        // 900 logical bytes produced from 400 wire bytes.
        assert_eq!(doc["expansion_ratio"], 2.25);
    }

    #[test]
    fn test_outcome_strings_are_stable() {
        assert_eq!(Outcome::Ok.as_str(), "ok");
        assert_eq!(Outcome::Error.as_str(), "error");
        assert_eq!(Outcome::Cancelled.as_str(), "cancelled");
        assert_eq!(Outcome::Aborted.as_str(), "aborted");
        assert_eq!(Outcome::Dropped.as_str(), "dropped");
        assert_eq!(Outcome::InProgress.as_str(), "in_progress");
    }

    #[test]
    fn test_error_class_buckets() {
        use std::io::{Error as IoError, ErrorKind};

        assert_eq!(error_class(&DataError::IOError(IoError::new(ErrorKind::Other, "x"))), "io");
        assert_eq!(error_class(&DataError::InternalError("x".into())), "internal");
        assert_eq!(error_class(&DataError::HashNotFound), "not_found");
        assert_eq!(error_class(&DataError::InvalidOperation("x".into())), "internal");
        assert_eq!(error_class(&DataError::ParameterError("x".into())), "other");
        assert_eq!(
            error_class(&DataError::RuntimeError(xet_runtime::error::RuntimeError::KeyboardInterrupt)),
            "cancelled"
        );
        assert_eq!(
            error_class(&DataError::RuntimeError(xet_runtime::error::RuntimeError::TaskCanceled("x".into()))),
            "cancelled"
        );
    }

    /// Every bucket `error_class` can return must be in the documented closed set.
    #[test]
    fn test_error_classes_are_in_the_closed_set() {
        const CLOSED_SET: &[&str] = &[
            "none",
            "auth",
            "network",
            "timeout",
            "rate_limited",
            "server_error",
            "not_found",
            "io",
            "format",
            "cancelled",
            "internal",
            "other",
        ];
        assert!(CLOSED_SET.contains(&ERROR_CLASS_NONE));
        for e in [
            DataError::InternalError("x".into()),
            DataError::HashNotFound,
            DataError::ParameterError("x".into()),
            DataError::SyncError("x".into()),
        ] {
            assert!(CLOSED_SET.contains(&error_class(&e)), "{} escaped the closed set", error_class(&e));
        }
    }
}
