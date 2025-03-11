use thiserror::Error;

/// Define an error time for spawning external threads.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MultithreadedRuntimeError {
    #[error("Error Initializing Multithreaded Runtime: {0:?}")]
    RuntimeInitializationError(std::io::Error),

    #[error("Task Panic: {0:?}.")]
    TaskPanic(tokio::task::JoinError),

    #[error("Task cancelled; possible runtime shutdown in progress ({0}).")]
    TaskCanceled(String),

    #[error("Unknown task runtime error: {0}")]
    Other(String),
}

impl From<tokio::task::JoinError> for MultithreadedRuntimeError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_panic() {
            // The task panic'd.  Pass this exception on.
            tracing::error!("Panic reported on xet worker task: {err:?}");
            MultithreadedRuntimeError::TaskPanic(err)
        } else if err.is_cancelled() {
            // Likely caused by the runtime shutting down (e.g. with a keyboard CTRL-C).
            MultithreadedRuntimeError::TaskCanceled(format!("{err}"))
        } else {
            MultithreadedRuntimeError::Other(format!("task join error: {err}"))
        }
    }
}
