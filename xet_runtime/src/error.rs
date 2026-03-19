use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RuntimeError {
    #[error("Error initializing runtime: {0:?}")]
    RuntimeInit(std::io::Error),

    #[error("Task panic: {0:?}")]
    TaskPanic(String),

    #[error("Task cancelled; possible runtime shutdown in progress ({0}).")]
    TaskCanceled(String),

    #[error("{0}")]
    Other(String),
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

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;
