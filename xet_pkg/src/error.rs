use thiserror::Error;
use xet_client::ClientError;
use xet_core_structures::FormatError;
use xet_data::DataError;
use xet_data::progress_tracking::UniqueID;
use xet_runtime::RuntimeError;

/// Unified error type for the Xet public API.
///
/// Variants are grouped into user-facing categories that map naturally to
/// Python exception types, plus session-lifecycle states that the internal
/// code can match on.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum XetError {
    // -- Session lifecycle -----------------------------------------------
    /// The session (or its parent commit/group) has been aborted.
    #[error("Session aborted")]
    Aborted,

    /// `commit()` was called more than once.
    #[error("Upload commit already committed")]
    AlreadyCommitted,

    /// `finish()` was called more than once.
    #[error("Download group already finished")]
    AlreadyFinished,

    /// A task ID that doesn't correspond to any queued file.
    #[error("Invalid task ID: {0}")]
    InvalidTaskID(UniqueID),

    // -- User-facing error categories ------------------------------------
    /// Token refresh or credential failures.
    #[error("Authentication error: {0}")]
    Authentication(String),

    /// Network-level failures: DNS, timeouts, HTTP 5xx, etc.
    #[error("Network error: {0}")]
    Network(String),

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

    /// Cancellation: SIGINT, session abort, semaphore closed.
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

    pub fn wrong_mode(msg: impl std::fmt::Display) -> Self {
        Self::WrongRuntimeMode(msg.to_string())
    }

    fn from_runtime_error_ref(re: &RuntimeError) -> Self {
        match re {
            RuntimeError::TaskCanceled(_) => XetError::Cancelled(re.to_string()),
            _ => XetError::Internal(re.to_string()),
        }
    }

    fn from_format_error_ref(fe: &FormatError) -> Self {
        match fe {
            FormatError::Io(_) => XetError::Io(fe.to_string()),
            FormatError::ShardNotFound(_) | FormatError::FileNotFound(_) => XetError::NotFound(fe.to_string()),
            FormatError::HashMismatch
            | FormatError::TruncatedHashCollision(_)
            | FormatError::InvalidShard(_)
            | FormatError::ShardVersion(_)
            | FormatError::ChunkHeaderParse
            | FormatError::FormatError(_)
            | FormatError::Compression(_) => XetError::DataIntegrity(fe.to_string()),
            FormatError::InvalidRange | FormatError::InvalidArguments | FormatError::BadFilename(_) => {
                XetError::Configuration(fe.to_string())
            },
            FormatError::Runtime(re) => XetError::from_runtime_error_ref(re),
            _ => XetError::Internal(fe.to_string()),
        }
    }

    fn from_client_error_ref(ce: &ClientError) -> Self {
        match ce {
            ClientError::AuthError(_) => XetError::Authentication(ce.to_string()),
            ClientError::ReqwestError(_, _)
            | ClientError::ReqwestMiddlewareError(_)
            | ClientError::PresignedUrlExpirationError => XetError::Network(ce.to_string()),
            ClientError::FileNotFound(_) | ClientError::XORBNotFound(_) => XetError::NotFound(ce.to_string()),
            ClientError::ConfigurationError(_)
            | ClientError::InvalidArguments
            | ClientError::InvalidRange
            | ClientError::InvalidShardKey(_)
            | ClientError::InvalidKey(_)
            | ClientError::InvalidRepoType(_) => XetError::Configuration(ce.to_string()),
            ClientError::IOError(_) => XetError::Io(ce.to_string()),
            ClientError::FormatError(fe) => XetError::from_format_error_ref(fe),
            _ => XetError::Internal(ce.to_string()),
        }
    }

    fn from_data_error_ref(de: &DataError) -> Self {
        match de {
            DataError::AuthError(_) => XetError::Authentication(de.to_string()),
            DataError::ClientError(ce) => XetError::from_client_error_ref(ce),
            DataError::FormatError(fe) => XetError::from_format_error_ref(fe),
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

impl From<FormatError> for XetError {
    fn from(e: FormatError) -> Self {
        XetError::from_format_error_ref(&e)
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
    fn format_not_found_maps_to_not_found() {
        let err = XetError::from(FormatError::ShardNotFound(MerkleHash::default()));
        assert!(matches!(err, XetError::NotFound(_)));
    }

    #[test]
    fn format_invalid_args_maps_to_configuration() {
        let err = XetError::from(FormatError::InvalidArguments);
        assert!(matches!(err, XetError::Configuration(_)));
    }

    #[test]
    fn client_auth_maps_to_authentication() {
        let err = XetError::from(ClientError::AuthError(AuthError::TokenRefreshFailure("bad token".to_string())));
        assert!(matches!(err, XetError::Authentication(_)));
    }

    #[test]
    fn client_nested_format_maps_using_format_rules() {
        let err = XetError::from(ClientError::FormatError(FormatError::InvalidRange));
        assert!(matches!(err, XetError::Configuration(_)));
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
}
