use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KeyError {
    #[error("Key parsing failure: {0}")]
    UnparsableKey(String),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SingleflightError<E>
where
    E: Send + std::fmt::Debug + Sync,
{
    #[error("BUG: singleflight waiter was notified before result was updated")]
    NoResult,

    #[error("BUG: call was removed before singleflight owner could update it")]
    CallMissing,

    #[error("BUG: call didn't create a Notifier for the initial task")]
    NoNotifierCreated,

    #[error(transparent)]
    InternalError(#[from] E),

    #[error("Real call failed: {0}")]
    WaiterInternalError(String),

    #[error("JoinError inside singleflight owner task: {0}")]
    JoinError(String),

    #[error("Owner task panicked")]
    OwnerPanicked,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AuthError {
    #[error("Refresh function: {0} is not callable")]
    RefreshFunctionNotCallable(String),

    #[error("Token refresh failed: {0}")]
    TokenRefreshFailure(String),
}

impl AuthError {
    pub fn token_refresh_failure(err: impl ToString) -> Self {
        Self::TokenRefreshFailure(err.to_string())
    }
}

impl<E: Send + std::fmt::Debug + Sync> Clone for SingleflightError<E> {
    fn clone(&self) -> Self {
        match self {
            SingleflightError::NoResult => SingleflightError::NoResult,
            SingleflightError::CallMissing => SingleflightError::CallMissing,
            SingleflightError::NoNotifierCreated => SingleflightError::NoNotifierCreated,
            SingleflightError::InternalError(e) => SingleflightError::WaiterInternalError(format!("{e:?}")),
            SingleflightError::WaiterInternalError(s) => SingleflightError::WaiterInternalError(s.clone()),
            SingleflightError::JoinError(e) => SingleflightError::JoinError(e.clone()),
            SingleflightError::OwnerPanicked => SingleflightError::OwnerPanicked,
        }
    }
}
