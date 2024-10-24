use std::fmt::{Debug, Display};

use tracing::{debug, error, info, warn};

/// A helper trait to log errors.
/// The logging functions will track the caller's callsite.
/// For a chain of calls A -> B -> C -> ErrorPrinter, the
/// topmost function without #[track_caller] is deemed the callsite.
pub trait ErrorPrinter {
    fn log_error<M: Display>(self, message: M) -> Self;

    fn warn_error<M: Display>(self, message: M) -> Self;

    fn debug_error<M: Display>(self, message: M) -> Self;

    fn info_error<M: Display>(self, message: M) -> Self;
}

impl<T, E: Debug> ErrorPrinter for Result<T, E> {
    /// If self is an Err(e), prints out the given string to tracing::error,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn log_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {},
            Err(e) => {
                let caller = get_caller();
                error!(caller, "{message}, error: {e:?}")
            },
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::warn,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn warn_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {},
            Err(e) => {
                let caller = get_caller();
                warn!(caller, "{message}, error: {e:?}")
            },
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::debug,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn debug_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {},
            Err(e) => {
                let caller = get_caller();
                debug!(caller, "{message}, error: {e:?}")
            },
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::info,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn info_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {},
            Err(e) => {
                let caller = get_caller();
                info!(caller, "{message}, error: {e:?}")
            },
        }
        self
    }
}

/// A helper trait to log when an option is None.
/// The logging functions will track the caller's callsite.
/// For a chain of calls A -> B -> C -> OptionPrinter, the
/// topmost function without #[track_caller] is deemed the callsite.
pub trait OptionPrinter {
    fn error_none<M: Display>(self, message: M) -> Self;

    fn warn_none<M: Display>(self, message: M) -> Self;

    fn debug_none<M: Display>(self, message: M) -> Self;

    fn info_none<M: Display>(self, message: M) -> Self;
}

impl<T> OptionPrinter for Option<T> {
    /// If self is None, prints out the message to tracing::error.
    #[track_caller]
    fn error_none<M: Display>(self, message: M) -> Self {
        match &self {
            Some(_) => {},
            None => {
                let caller = get_caller();
                error!(caller, "{message}")
            },
        }
        self
    }

    /// If self is None, prints out the message to tracing::warn.
    #[track_caller]
    fn warn_none<M: Display>(self, message: M) -> Self {
        match &self {
            Some(_) => {},
            None => {
                let caller = get_caller();
                warn!(caller, "{message}")
            },
        }
        self
    }

    /// If self is None, prints out the message to tracing::debug.
    #[track_caller]
    fn debug_none<M: Display>(self, message: M) -> Self {
        match &self {
            Some(_) => {},
            None => {
                let caller = get_caller();
                debug!(caller, "{message}")
            },
        }
        self
    }

    /// If self is None, prints out the message to tracing::info.
    #[track_caller]
    fn info_none<M: Display>(self, message: M) -> Self {
        match &self {
            Some(_) => {},
            None => {
                let caller = get_caller();
                info!(caller, "{message}")
            },
        }
        self
    }
}

/// gets caller information for the top of the `#[track_caller]` stack as a formatted string:
/// "<file>:<line>"
#[track_caller]
fn get_caller() -> String {
    let location = std::panic::Location::caller();
    format!("{}:{}", location.file(), location.line())
}
