use http::StatusCode;
use thiserror::Error;
use xet_client::ClientError;
use xet_core_structures::CoreError;
use xet_data::DataError;
use xet_data::file_reconstruction::FileReconstructionError;
use xet_runtime::RuntimeError;
use xet_runtime::utils::UniqueId;

/// Unified error type for the Xet public API.
///
/// Variants are grouped into user-facing categories that map naturally to
/// Python exception types, plus session-lifecycle states that the internal
/// code can match on.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum XetError {
    // -- Session lifecycle -----------------------------------------------
    /// SIGINT / runtime shutdown.
    #[error("Keyboard interrupt (SIGINT)")]
    KeyboardInterrupt,

    /// Explicit user abort (session, commit, group, or stream level).
    #[error("User cancelled: {0}")]
    UserCancelled(String),

    /// A previous operation on this task failed; carries the stored error description.
    #[error("Previous task error: {0}")]
    PreviousTaskError(String),

    /// Task-level error captured from a background upload/download handle.
    #[error("Task error: {0}")]
    TaskError(String),

    /// The operation has already completed, is already finalizing, or was already committed/finished.
    #[error("Already completed")]
    AlreadyCompleted,

    /// A task ID that doesn't correspond to any queued file.
    #[error("Invalid task ID: {0}")]
    InvalidTaskID(UniqueId),

    // -- User-facing error categories ------------------------------------
    /// Token refresh or credential failures.
    #[error("Authentication error: {0}")]
    Authentication(String),

    /// Network-level failures: DNS, connection reset, TLS, and any HTTP failure whose status did
    /// not survive to this point.
    #[error("Network error: {0}")]
    Network(String),

    /// The server shed load: HTTP 429.
    ///
    /// Split out from [`Network`](Self::Network) because a 429 is not a network fault - it is the
    /// server asking for less traffic, and it needs to be distinguishable when reading failure
    /// rates. Maps to the same Python exception as `Network`.
    #[error("Rate limited: {0}")]
    RateLimited(String),

    /// The server failed to serve the request: HTTP 5xx.
    ///
    /// Split out from [`Network`](Self::Network) for the same reason as
    /// [`RateLimited`](Self::RateLimited): a server-side failure and a client-side connection
    /// problem call for different responses. Maps to the same Python exception as `Network`.
    #[error("Server error: {0}")]
    ServerError(String),

    /// A network request timed out.
    #[error("Timeout: {0}")]
    Timeout(String),

    /// A requested resource (file, XORB, shard) does not exist.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Data corruption: hash mismatches, invalid shard/xorb format, etc.
    #[error("Data integrity error: {0}")]
    DataIntegrity(String),

    /// Invalid configuration or arguments supplied by the caller.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Local filesystem I/O failures.
    #[error("I/O error: {0}")]
    Io(String),

    /// Generic cancellation from non-user sources (semaphore close, join cancellation).
    #[error("Operation cancelled: {0}")]
    Cancelled(String),

    /// Catch-all for unexpected internal errors (panics, lock poison, bugs).
    #[error("Internal error: {0}")]
    Internal(String),

    /// Caller invoked a method that is incompatible with the session's runtime mode.
    #[error("Wrong runtime mode: {0}")]
    WrongRuntimeMode(String),
}

impl XetError {
    pub fn other(msg: impl std::fmt::Display) -> Self {
        Self::Internal(msg.to_string())
    }

    /// Classifies this error for telemetry as an `(outcome, error_class)` pair.
    ///
    /// By the time a group finishes, the originating [`DataError`] has usually been flattened into
    /// a string variant here, so `xet_data`'s `error_class` cannot be applied directly. This maps
    /// the surviving categories onto the *same* coarse class vocabulary, so documents produced by
    /// this path aggregate together with those classified inside `xet_data`.
    ///
    /// That aggregation depends on HTTP status surviving the flattening, which is why
    /// [`RateLimited`](Self::RateLimited) and [`ServerError`](Self::ServerError) exist as distinct
    /// variants. Collapsing them into [`Network`](Self::Network) - as this type used to - made
    /// `rate_limited` and `server_error` unreachable for downloads while uploads still reported
    /// them, so a 429 meant two different things depending on the direction.
    ///
    /// One gap remains on purpose: a 404 that arrives as a `reqwest` status still classifies as
    /// `network` here, whereas `xet_data` maps it to `not_found`. Routing it to
    /// [`NotFound`](Self::NotFound) would change the Python exception type callers see, which is a
    /// user-visible change rather than a telemetry fix.
    ///
    /// Deliberately coarse, matching `xet_data::telemetry::error_class`: the question is "are
    /// downloads failing more than they were, and is it the network or the server", and error text
    /// can contain paths, so none of it is carried.
    pub fn telemetry_class(&self) -> (xet_data::telemetry::Outcome, &'static str) {
        use xet_data::telemetry::outcome_for_class;

        let class = match self {
            // Cancellation is a user action, not a failure. `outcome_for_class` maps these to
            // `Outcome::Cancelled` so they stay out of failure-rate alerts.
            XetError::KeyboardInterrupt | XetError::UserCancelled(_) | XetError::Cancelled(_) => "cancelled",
            XetError::Authentication(_) => "auth",
            XetError::Network(_) => "network",
            XetError::RateLimited(_) => "rate_limited",
            XetError::ServerError(_) => "server_error",
            XetError::Timeout(_) => "timeout",
            XetError::NotFound(_) => "not_found",
            XetError::DataIntegrity(_) => "format",
            XetError::Io(_) => "io",
            XetError::Configuration(_) | XetError::InvalidTaskID(_) | XetError::WrongRuntimeMode(_) => "internal",
            XetError::Internal(_) => "internal",
            // A task failed and its cause was reduced to a message; the class is genuinely
            // unknown. Matched exhaustively on purpose - a new variant should not silently
            // become "other" without someone deciding that is right.
            XetError::TaskError(_) | XetError::PreviousTaskError(_) | XetError::AlreadyCompleted => "other",
        };

        (outcome_for_class(class), class)
    }

    pub fn wrong_mode(msg: impl std::fmt::Display) -> Self {
        Self::WrongRuntimeMode(msg.to_string())
    }

    fn from_runtime_error_ref(re: &RuntimeError) -> Self {
        match re {
            RuntimeError::KeyboardInterrupt => XetError::KeyboardInterrupt,
            RuntimeError::TaskCanceled(msg) => XetError::Cancelled(format!("Task cancelled: {msg}")),
            RuntimeError::InvalidRuntime(_) => XetError::WrongRuntimeMode(re.to_string()),
            _ => XetError::Internal(re.to_string()),
        }
    }

    fn from_core_error_ref(fe: &CoreError) -> Self {
        match fe {
            CoreError::Io(_) => XetError::Io(fe.to_string()),
            CoreError::ShardNotFound(_) | CoreError::FileNotFound(_) => XetError::NotFound(fe.to_string()),
            CoreError::HashMismatch
            | CoreError::TruncatedHashCollision(_)
            | CoreError::InvalidShard(_)
            | CoreError::ShardVersion(_)
            | CoreError::ChunkHeaderParse
            | CoreError::HashParsing(_)
            | CoreError::MalformedData(_)
            | CoreError::CompressionError(_) => XetError::DataIntegrity(fe.to_string()),
            CoreError::InvalidRange | CoreError::InvalidArguments | CoreError::BadFilename(_) => {
                XetError::Configuration(fe.to_string())
            },
            CoreError::RuntimeError(re) => XetError::from_runtime_error_ref(re),
            _ => XetError::Internal(fe.to_string()),
        }
    }

    fn from_client_error_ref(ce: &ClientError) -> Self {
        match ce {
            ClientError::AuthError(_) | ClientError::PresignedUrlExpirationError | ClientError::CredentialHelper(_) => {
                XetError::Authentication(ce.to_string())
            },
            ClientError::ReqwestError(e, _) if e.is_timeout() => XetError::Timeout(ce.to_string()),
            // Status wins over transport, matching `xet_data::telemetry::error_class`. Without
            // this, every HTTP failure flattened to `Network` and a download could never report
            // `rate_limited` or `server_error` - the two classes that distinguish "the server is
            // shedding load" from "the network is broken". `ClientError::status()` covers the
            // middleware variant too, where the status is otherwise unreachable.
            ClientError::ReqwestError(_, _) | ClientError::ReqwestMiddlewareError(_) => match ce.status() {
                Some(StatusCode::TOO_MANY_REQUESTS) => XetError::RateLimited(ce.to_string()),
                Some(status) if status.is_server_error() => XetError::ServerError(ce.to_string()),
                _ => XetError::Network(ce.to_string()),
            },
            ClientError::FileNotFound(_) | ClientError::XORBNotFound(_) => XetError::NotFound(ce.to_string()),
            ClientError::ConfigurationError(_)
            | ClientError::InvalidArguments
            | ClientError::InvalidRange
            | ClientError::InvalidShardKey(_)
            | ClientError::InvalidKey(_)
            | ClientError::InvalidRepoType(_) => XetError::Configuration(ce.to_string()),
            ClientError::IOError(_) => XetError::Io(ce.to_string()),
            ClientError::FormatError(fe) => XetError::from_core_error_ref(fe),
            _ => XetError::Internal(ce.to_string()),
        }
    }

    fn from_file_reconstruction_error_ref(fre: &FileReconstructionError) -> Self {
        match fre {
            FileReconstructionError::ClientError(ce) => XetError::from_client_error_ref(ce),
            FileReconstructionError::IoError(_) => XetError::Io(fre.to_string()),
            FileReconstructionError::RuntimeError(re) => XetError::from_runtime_error_ref(re),
            FileReconstructionError::TaskJoinError(je) if je.is_cancelled() => {
                XetError::Cancelled(format!("Task cancelled: {je}"))
            },
            FileReconstructionError::TaskJoinError(je) => XetError::Internal(format!("Task join error: {je}")),
            FileReconstructionError::ConfigurationError(_) => XetError::Configuration(fre.to_string()),
            FileReconstructionError::CorruptedReconstruction(_) => XetError::DataIntegrity(fre.to_string()),
            _ => XetError::Internal(fre.to_string()),
        }
    }

    fn from_data_error_ref(de: &DataError) -> Self {
        match de {
            DataError::AuthError(_) => XetError::Authentication(de.to_string()),
            DataError::ClientError(ce) => XetError::from_client_error_ref(ce),
            DataError::FormatError(fe) => XetError::from_core_error_ref(fe),
            DataError::IOError(_) => XetError::Io(de.to_string()),
            DataError::RuntimeError(re) => XetError::from_runtime_error_ref(re),
            DataError::FileQueryPolicyError(_)
            | DataError::CASConfigError(_)
            | DataError::ShardConfigError(_)
            | DataError::DedupConfigError(_)
            | DataError::ParameterError(_)
            | DataError::DeprecatedError(_) => XetError::Configuration(de.to_string()),
            DataError::HashNotFound => XetError::NotFound(de.to_string()),
            DataError::HashStringParsingFailure(_) => XetError::DataIntegrity(de.to_string()),
            DataError::InvalidOperation(_) => XetError::Configuration(de.to_string()),
            DataError::FileReconstructionError(fre) => XetError::from_file_reconstruction_error_ref(fre),
            _ => XetError::Internal(de.to_string()),
        }
    }
}

// -- From impls for package-level errors ---------------------------------

impl From<RuntimeError> for XetError {
    fn from(e: RuntimeError) -> Self {
        XetError::from_runtime_error_ref(&e)
    }
}

impl From<CoreError> for XetError {
    fn from(e: CoreError) -> Self {
        XetError::from_core_error_ref(&e)
    }
}

impl From<ClientError> for XetError {
    fn from(e: ClientError) -> Self {
        XetError::from_client_error_ref(&e)
    }
}

impl From<DataError> for XetError {
    fn from(e: DataError) -> Self {
        XetError::from_data_error_ref(&e)
    }
}

impl From<FileReconstructionError> for XetError {
    fn from(e: FileReconstructionError) -> Self {
        XetError::from_file_reconstruction_error_ref(&e)
    }
}

// -- Convenience From impls for common error types -----------------------

impl From<std::io::Error> for XetError {
    fn from(e: std::io::Error) -> Self {
        XetError::Io(e.to_string())
    }
}

impl From<tokio::task::JoinError> for XetError {
    fn from(e: tokio::task::JoinError) -> Self {
        if e.is_cancelled() {
            XetError::Cancelled(format!("Task cancelled: {e}"))
        } else {
            XetError::Internal(format!("Task join error: {e}"))
        }
    }
}

#[cfg(target_family = "wasm")]
impl From<tokio_with_wasm::task::JoinError> for XetError {
    fn from(e: tokio_with_wasm::task::JoinError) -> Self {
        if e.is_cancelled() {
            XetError::Cancelled(format!("Task cancelled: {e}"))
        } else {
            XetError::Internal(format!("Task join error: {e}"))
        }
    }
}

impl From<tokio::sync::AcquireError> for XetError {
    fn from(e: tokio::sync::AcquireError) -> Self {
        XetError::Cancelled(format!("Semaphore closed: {e}"))
    }
}

impl<T> From<std::sync::PoisonError<std::sync::MutexGuard<'_, T>>> for XetError {
    fn from(e: std::sync::PoisonError<std::sync::MutexGuard<'_, T>>) -> Self {
        XetError::Internal(format!("Mutex poisoned: {e}"))
    }
}

impl<T> From<std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, T>>> for XetError {
    fn from(e: std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, T>>) -> Self {
        XetError::Internal(format!("RwLock write poisoned: {e}"))
    }
}

impl<T> From<std::sync::PoisonError<std::sync::RwLockReadGuard<'_, T>>> for XetError {
    fn from(e: std::sync::PoisonError<std::sync::RwLockReadGuard<'_, T>>) -> Self {
        XetError::Internal(format!("RwLock read poisoned: {e}"))
    }
}

// -- Python exception classes & conversion --------------------------------

#[cfg(feature = "python")]
mod py_exceptions {
    // Inherits from Python's PermissionError so `except PermissionError` still catches it.
    pyo3::create_exception!(hf_xet, XetAuthenticationError, pyo3::exceptions::PyPermissionError);

    // Inherits from Python's FileNotFoundError so `except FileNotFoundError` still catches it.
    pyo3::create_exception!(hf_xet, XetObjectNotFoundError, pyo3::exceptions::PyFileNotFoundError);

    /// Register the custom exception classes on a Python module.
    ///
    /// Call this from the `#[pymodule]` init function so that the exceptions
    /// are importable as `hf_xet.XetAuthenticationError`, etc.
    pub fn register_exceptions(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
        use pyo3::types::PyModuleMethods;

        m.add("XetAuthenticationError", m.py().get_type::<XetAuthenticationError>())?;
        m.add("XetObjectNotFoundError", m.py().get_type::<XetObjectNotFoundError>())?;
        Ok(())
    }
}

#[cfg(feature = "python")]
pub use py_exceptions::{XetAuthenticationError, XetObjectNotFoundError, register_exceptions};

#[cfg(feature = "python")]
impl From<XetError> for pyo3::PyErr {
    fn from(err: XetError) -> pyo3::PyErr {
        use pyo3::exceptions::{
            PyConnectionError, PyKeyboardInterrupt, PyOSError, PyRuntimeError, PyTimeoutError, PyValueError,
        };

        let msg = err.to_string();
        #[allow(unreachable_patterns)] // XetError is #[non_exhaustive]
        match err {
            XetError::KeyboardInterrupt => PyKeyboardInterrupt::new_err(msg),
            XetError::Authentication(_) => XetAuthenticationError::new_err(msg),
            XetError::NotFound(_) => XetObjectNotFoundError::new_err(msg),
            // 429 and 5xx deliberately raise the same Python exception as a plain network failure:
            // they are split out for telemetry classification, and remapping them would change the
            // exception type callers already catch.
            XetError::Network(_) | XetError::RateLimited(_) | XetError::ServerError(_) => {
                PyConnectionError::new_err(msg)
            },
            XetError::Timeout(_) => PyTimeoutError::new_err(msg),
            XetError::Io(_) => PyOSError::new_err(msg),
            XetError::Configuration(_) | XetError::InvalidTaskID(_) => PyValueError::new_err(msg),
            XetError::DataIntegrity(_)
            | XetError::Internal(_)
            | XetError::WrongRuntimeMode(_)
            | XetError::AlreadyCompleted
            | XetError::UserCancelled(_)
            | XetError::PreviousTaskError(_)
            | XetError::TaskError(_)
            | XetError::Cancelled(_) => PyRuntimeError::new_err(msg),
            _ => PyRuntimeError::new_err(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use xet_client::cas_client::auth::AuthError;
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    #[test]
    fn runtime_cancelled_maps_to_cancelled() {
        let err = XetError::from(RuntimeError::TaskCanceled("worker stopped".to_string()));
        assert!(matches!(err, XetError::Cancelled(_)));
    }

    #[test]
    fn runtime_keyboard_interrupt_maps_to_keyboard_interrupt() {
        let err = XetError::from(RuntimeError::KeyboardInterrupt);
        assert!(matches!(err, XetError::KeyboardInterrupt));
    }

    #[test]
    fn format_not_found_maps_to_not_found() {
        let err = XetError::from(CoreError::ShardNotFound(MerkleHash::default()));
        assert!(matches!(err, XetError::NotFound(_)));
    }

    #[test]
    fn format_invalid_args_maps_to_configuration() {
        let err = XetError::from(CoreError::InvalidArguments);
        assert!(matches!(err, XetError::Configuration(_)));
    }

    #[test]
    fn client_auth_maps_to_authentication() {
        let err = XetError::from(ClientError::AuthError(AuthError::TokenRefreshFailure("bad token".to_string())));
        assert!(matches!(err, XetError::Authentication(_)));
    }

    #[test]
    fn client_nested_format_maps_using_format_rules() {
        let err = XetError::from(ClientError::FormatError(CoreError::InvalidRange));
        assert!(matches!(err, XetError::Configuration(_)));
    }

    /// Builds a `ClientError` carrying a real `reqwest` error with `status`, which is the only way
    /// the status-based classification can be exercised - `reqwest::Error` cannot be constructed
    /// directly, so it has to come from a response.
    fn client_error_with_status(status: u16) -> ClientError {
        use xet_client::cas_client::exports::reqwest;

        let response = http::Response::builder().status(status).body(String::new()).unwrap();
        let err = reqwest::Response::from(response)
            .error_for_status()
            .expect_err("a 4xx/5xx response must produce an error");
        ClientError::ReqwestError(err, "cas.example.invalid".to_string())
    }

    /// A 429 is the server shedding load, not a network fault, and it has to stay distinguishable
    /// all the way to the telemetry class - see `telemetry_class`.
    #[test]
    fn client_429_maps_to_rate_limited() {
        let err = XetError::from(client_error_with_status(429));
        assert!(matches!(err, XetError::RateLimited(_)), "got {err:?}");
        assert_eq!(err.telemetry_class(), (xet_data::telemetry::Outcome::Error, "rate_limited"));
    }

    #[test]
    fn client_5xx_maps_to_server_error() {
        for status in [500, 502, 503] {
            let err = XetError::from(client_error_with_status(status));
            assert!(matches!(err, XetError::ServerError(_)), "status {status} gave {err:?}");
            assert_eq!(err.telemetry_class(), (xet_data::telemetry::Outcome::Error, "server_error"));
        }
    }

    /// The deliberate remaining gap, pinned so it is a decision rather than a surprise: a 404
    /// arriving as a `reqwest` status stays `network`, because routing it to `NotFound` would change
    /// the Python exception type callers already catch.
    #[test]
    fn client_404_status_stays_network() {
        let err = XetError::from(client_error_with_status(404));
        assert!(matches!(err, XetError::Network(_)), "got {err:?}");
        assert_eq!(err.telemetry_class().1, "network");
    }

    /// The whole point of A4: every class the upload path can report for an HTTP failure is now
    /// reachable from the download path too, so a 429 does not mean two different things depending
    /// on direction.
    #[test]
    fn http_failure_classes_match_the_upload_side_vocabulary() {
        use xet_data::telemetry::classify_error;

        for status in [429, 500] {
            let via_download = XetError::from(client_error_with_status(status)).telemetry_class();
            let via_upload = classify_error(&DataError::ClientError(client_error_with_status(status)));
            assert_eq!(via_download, via_upload, "status {status} classifies differently per direction");
        }
    }

    #[test]
    fn data_nested_client_maps_using_client_rules() {
        let err = XetError::from(DataError::ClientError(ClientError::FileNotFound(MerkleHash::default())));
        assert!(matches!(err, XetError::NotFound(_)));
    }

    #[test]
    fn data_runtime_cancelled_maps_to_cancelled() {
        let err = XetError::from(DataError::RuntimeError(RuntimeError::TaskCanceled("cancelled".to_string())));
        assert!(matches!(err, XetError::Cancelled(_)));
    }

    #[test]
    fn presigned_url_expiration_maps_to_authentication() {
        let err = XetError::from(ClientError::PresignedUrlExpirationError);
        assert!(matches!(err, XetError::Authentication(_)));
    }

    #[test]
    fn credential_helper_maps_to_authentication() {
        let err = XetError::from(ClientError::credential_helper_error(std::io::Error::other("cred fail")));
        assert!(matches!(err, XetError::Authentication(_)));
    }

    #[test]
    fn client_not_found_maps_to_not_found() {
        let err = XetError::from(ClientError::FileNotFound(MerkleHash::default()));
        assert!(matches!(err, XetError::NotFound(_)));
    }

    #[test]
    fn client_xorb_not_found_maps_to_not_found() {
        let err = XetError::from(ClientError::XORBNotFound(MerkleHash::default()));
        assert!(matches!(err, XetError::NotFound(_)));
    }

    #[test]
    fn client_io_maps_to_io() {
        let err = XetError::from(ClientError::IOError(std::io::Error::new(std::io::ErrorKind::NotFound, "gone")));
        assert!(matches!(err, XetError::Io(_)));
    }
}
