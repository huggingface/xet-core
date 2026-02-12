//! Error types for xet-session

use thiserror::Error;

/// Errors that can occur during session operations
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("Session is already executing tasks")]
    AlreadyExecuting,

    #[error("Session is not accepting new tasks")]
    NotAcceptingTasks,

    #[error("Upload commit already committed")]
    AlreadyCommitted,

    #[error("Download group already finished")]
    AlreadyFinished,

    #[error("Task not found: {0}")]
    TaskNotFound(ulid::Ulid),

    #[error("Task join error: {0}")]
    TaskJoinError(#[from] tokio::task::JoinError),

    #[error("Runtime error: {0}")]
    Runtime(#[from] xet_runtime::errors::MultithreadedRuntimeError),

    #[error("Data processing error: {0}")]
    DataProcessing(#[from] data::errors::DataProcessingError),

    #[error("Session already ended")]
    SessionEnded,

    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Other error: {0}")]
    Other(String),
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
