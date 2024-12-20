use tracing::error;
use xet_error::Error;

/// An error class for everyone to wrap that indicates cancellation.
#[derive(Debug, Error)]
pub struct RuntimeCancellation {}

impl std::fmt::Display for RuntimeCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeCancellation")
    }
}

/// Define an error time for spawning external threads.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum XetRuntimeError {
    #[error("Error Initializing Async Runtime: {0:?}")]
    AsyncRuntimeInitializationError(#[from] std::io::Error),

    #[error("Error Initializing Compute Runtime: {0:?}")]
    ComputeRuntimeInitializationError(String),

    #[error("Task Panic: {0:?}.")]
    TaskPanic(String),

    #[error("Task cancelled; possible runtime shutdown in progress ({0}).")]
    TaskCanceled(String),

    #[error("Async Task Join Error: {0}")]
    OtherJoinError(tokio::task::JoinError),

    #[error("RuntimeCancellation")]
    RuntimeCancellation(#[from] RuntimeCancellation),

    #[error("ask runtime error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, XetRuntimeError>;

pub(crate) fn map_join_error(e: tokio::task::JoinError) -> XetRuntimeError {
    if e.is_panic() {
        // The task panic'd.  Pass this exception on.
        error!("Panic reported on xet worker task: {e:?}");
        XetRuntimeError::TaskPanic(format!("{e:?}"))
    } else if e.is_cancelled() {
        // Likely caused by the runtime shutting down (e.g. with a keyboard CTRL-C).
        XetRuntimeError::TaskCanceled(format!("{e}"))
    } else {
        XetRuntimeError::OtherJoinError(e)
    }
}
