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
