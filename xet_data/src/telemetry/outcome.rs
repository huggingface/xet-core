//! The outcome vocabulary: how a transfer ended, and why.
//!
//! Split out of `payload.rs` because it must compile on **every** target. The rest of the
//! telemetry module depends on `TransferTelemetry`, which does not exist on wasm, but these types
//! appear in `FileDownloadSession`'s public signatures and in `xet_pkg`, so gating them off would
//! force `#[cfg]` onto every call site that merely names an outcome.
//!
//! Nothing here sends anything; it is pure vocabulary.

use crate::error::DataError;

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
    /// A download session dropped without finalizing *and* without completing its transfer.
    ///
    /// This means genuinely abandoned: at least one item never had its size finalized or never
    /// delivered all of its bytes. A session dropped without `finalize()` that nonetheless
    /// transferred everything reports [`Ok`](Self::Ok) instead, so this variant stays a real
    /// failure signal rather than an artifact of a caller that skipped `finish()`.
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

/// Derives the outcome and error class from a failed transfer.
pub fn classify_error(error: &DataError) -> (Outcome, &'static str) {
    let class = error_class(error);
    (outcome_for_class(class), class)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use std::io::Error as IoError;

        assert_eq!(error_class(&DataError::IOError(IoError::other("x"))), "io");
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
