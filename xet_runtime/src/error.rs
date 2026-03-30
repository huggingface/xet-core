use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RuntimeError {
    #[error("Error initializing runtime: {0:?}")]
    RuntimeInit(std::io::Error),

    #[error("Invalid runtime: {0}")]
    InvalidRuntime(String),

    #[error("Task panic: {0:?}")]
    TaskPanic(String),

    #[error("Task cancelled; possible runtime shutdown in progress ({0}).")]
    TaskCanceled(String),

    #[error("Reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("Keyboard interrupt (SIGINT)")]
    KeyboardInterrupt,

    #[error("{0}")]
    Other(String),
}

// PoisonError<T> is generic over the lock-guard type, so #[from] cannot be used directly.
// This blanket impl gives the same `?`-propagation ergonomics for any poisoned std lock.
impl<T> From<std::sync::PoisonError<T>> for RuntimeError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        RuntimeError::LockPoisoned(e.to_string())
    }
}

impl From<tokio::task::JoinError> for RuntimeError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_panic() {
            tracing::error!("Panic reported on xet worker task: {err:?}");
            RuntimeError::TaskPanic(format!("{err:?}"))
        } else if err.is_cancelled() {
            RuntimeError::TaskCanceled(format!("{err}"))
        } else {
            RuntimeError::Other(format!("task join error: {err}"))
        }
    }
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
