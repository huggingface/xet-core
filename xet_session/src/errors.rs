//! Error types for xet-session

use thiserror::Error;

/// Errors that can occur during session operations.
///
/// Most API methods return `Result<_, SessionError>`.  The variants cover the
/// full lifecycle of a session, from runtime initialisation failures through
/// network I/O errors to logical misuse (e.g. committing twice).
#[derive(Debug, Error)]
pub enum SessionError {
    /// The session (or its parent commit/group) has been aborted.
    ///
    /// Returned when [`XetSession::abort`](crate::XetSession::abort) was called
    /// before this operation started.
    #[error("Session is already aborted")]
    Aborted,

    /// [`UploadCommit::commit`](crate::UploadCommit::commit) was called more than once.
    #[error("Upload commit already committed")]
    AlreadyCommitted,

    /// [`DownloadGroup::finish`](crate::DownloadGroup::finish) was called more than once.
    #[error("Download group already finished")]
    AlreadyFinished,

    /// A background task panicked or was cancelled by the runtime.
    #[error("Task join error: {0}")]
    TaskJoinError(#[from] tokio::task::JoinError),

    /// The tokio runtime could not be initialised.
    #[error("Runtime error: {0}")]
    Runtime(#[from] xet_runtime::errors::MultithreadedRuntimeError),

    /// An error occurred in the underlying data-processing layer (chunking,
    /// deduplication, CAS communication, etc.).
    #[error("Data processing error: {0}")]
    DataProcessing(#[from] data::errors::DataProcessingError),

    /// A `std::sync::Mutex` or `RwLock` was poisoned by a panicking thread.
    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),

    /// An I/O error occurred (e.g. reading a source file or writing a
    /// destination file).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A catch-all for errors that don't fit a more specific variant.
    #[error("Other error: {0}")]
    Other(String),
}

impl SessionError {
    /// Create an [`Other`](SessionError::Other) variant from any [`Display`](std::fmt::Display) value.
    pub fn other(msg: impl std::fmt::Display) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<String> for SessionError {
    fn from(s: String) -> Self {
        SessionError::Other(s)
    }
}

impl From<&str> for SessionError {
    fn from(s: &str) -> Self {
        SessionError::Other(s.to_string())
    }
}

// Helper to convert PoisonError from Mutex
impl<T> From<std::sync::PoisonError<std::sync::MutexGuard<'_, T>>> for SessionError {
    fn from(e: std::sync::PoisonError<std::sync::MutexGuard<'_, T>>) -> Self {
        SessionError::LockPoisoned(format!("Mutex poisoned: {}", e))
    }
}

// Helper to convert PoisonError from RwLock write
impl<T> From<std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, T>>> for SessionError {
    fn from(e: std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, T>>) -> Self {
        SessionError::LockPoisoned(format!("RwLock write poisoned: {}", e))
    }
}

// Helper to convert PoisonError from RwLock read
impl<T> From<std::sync::PoisonError<std::sync::RwLockReadGuard<'_, T>>> for SessionError {
    fn from(e: std::sync::PoisonError<std::sync::RwLockReadGuard<'_, T>>) -> Self {
        SessionError::LockPoisoned(format!("RwLock read poisoned: {}", e))
    }
}
