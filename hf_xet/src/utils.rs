//! Shared display helpers used across Python binding modules.

use pyo3::prelude::*;
use xet_pkg::XetError;
use xet_pkg::xet_session::{ItemProgressReport, XetTaskState};

// ── Error conversion ──────────────────────────────────────────────────────────

pub(crate) fn convert_xet_error(e: impl Into<XetError>) -> PyErr {
    PyErr::from(e.into())
}

// ── Signal-checked blocking call ──────────────────────────────────────────────

/// Run `f` on a background thread while periodically calling `py.check_signals()`
/// so that Ctrl-C is delivered to Python promptly during long-running operations.
///
/// Background: Python handles SIGINT by setting a flag that is only checked when
/// control returns to the interpreter.  While a blocking Rust call holds the GIL
/// (even via `py.detach()`), that flag is never observed.  By running the blocking
/// work on a separate thread and polling `py.check_signals()` every 100 ms, we
/// give CPython a chance to raise `KeyboardInterrupt` during calls like
/// `commit_blocking()` and `finish_blocking()` that can run for many seconds.
///
/// Reference: <https://pyo3.rs/latest/faq.html> — "Ctrl-C doesn't do anything
/// while my Rust code is executing"
///
/// When `KeyboardInterrupt` is raised here:
/// - It propagates to the Python caller's `except KeyboardInterrupt:` block.
/// - The caller calls `session.sigint_abort()` → sets `sigint_shutdown = true`.
/// - The background thread's next await checkpoint returns immediately.
/// - The thread exits cleanly.
///
/// This is the most Pythonic approach: `KeyboardInterrupt` is a standard exception
/// that propagates through `try/except` like any other.  The cleanup
/// (`sigint_abort()`) lives in Python where it is visible and auditable — no
/// hidden global state, no surprise interactions with `signal.signal()`.
pub(crate) fn blocking_call_with_signal_check<T, E, F>(py: Python<'_>, f: F) -> PyResult<T>
where
    T: Send + 'static,
    E: Into<XetError> + Send + 'static,
    F: FnOnce() -> Result<T, E> + Send + 'static,
{
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Duration;

    let (tx, mut rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        tx.send(f()).ok();
    });
    loop {
        // Release the GIL while waiting so that other threads that need it
        // (e.g. the progress-callback thread, other Python threads) can run.
        // After `detach` returns we re-hold the GIL for the signal check.
        //
        // `Receiver<T>: Send` but `!Sync`, so `&Receiver` is `!Ungil`.  We
        // satisfy the `Ungil` bound by moving `rx` into a `move` closure and
        // returning it alongside the result so it can be rebound for the next
        // iteration.
        let (rx_back, recv_result) = py.detach(move || {
            let result = rx.recv_timeout(Duration::from_millis(100));
            (rx, result)
        });
        rx = rx_back;
        match recv_result {
            Ok(result) => return result.map_err(|e| convert_xet_error(e)),
            Err(RecvTimeoutError::Timeout) => py.check_signals()?,
            // The sender was dropped without sending — the background thread panicked.
            // Return a recoverable error rather than panicking a second time, which
            // would crash the Python interpreter in a PyO3 context.
            Err(RecvTimeoutError::Disconnected) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "blocking operation panicked on background thread",
                ));
            },
        }
    }
}

// ── Task-state helpers ────────────────────────────────────────────────────────

/// Convert a Rust `status()` result to a Python [`crate::PyXetTaskState`], raising on error.
pub(crate) fn task_state_to_pystate(
    result: Result<XetTaskState, xet_pkg::XetError>,
) -> PyResult<crate::PyXetTaskState> {
    match result {
        Ok(XetTaskState::Running) => Ok(crate::PyXetTaskState::Running),
        Ok(XetTaskState::Finalizing) => Ok(crate::PyXetTaskState::Finalizing),
        Ok(XetTaskState::Completed) => Ok(crate::PyXetTaskState::Completed),
        Ok(XetTaskState::UserCancelled) => Ok(crate::PyXetTaskState::UserCancelled),
        Ok(XetTaskState::Error(msg)) => Err(convert_xet_error(xet_pkg::XetError::TaskError(msg))),
        Err(e) => Err(convert_xet_error(e)),
    }
}

/// Convert a `status()` result to a display string for use in `__repr__`.
///
/// Never discards information: the error message inside `XetTaskState::Error(msg)`
/// and any `XetError` from the `status()` call itself are both forwarded as the string.
pub(crate) fn task_state_display(result: Result<XetTaskState, xet_pkg::XetError>) -> String {
    match result {
        Ok(XetTaskState::Running) => "Running".to_string(),
        Ok(XetTaskState::Finalizing) => "Finalizing".to_string(),
        Ok(XetTaskState::Completed) => "Completed".to_string(),
        Ok(XetTaskState::UserCancelled) => "UserCancelled".to_string(),
        Ok(XetTaskState::Error(msg)) => msg,
        Err(e) => e.to_string(),
    }
}

// ── Progress helpers ──────────────────────────────────────────────────────────

/// Format an `Option<ItemProgressReport>` as `"bytes_completed/total_bytes"`,
/// or `"?/?"` if no report is available yet.
pub(crate) fn progress_display(progress: Option<ItemProgressReport>) -> String {
    match progress {
        Some(r) => format!("{}/{}", r.bytes_completed, r.total_bytes),
        None => "?/?".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::*;

    #[test]
    fn test_task_state_error_raises() {
        Python::attach(|_py| {
            let result = task_state_to_pystate(Ok(XetTaskState::Error("something went wrong".into())));
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("something went wrong"));
        });
    }

    #[test]
    fn test_task_state_outer_error_raises() {
        Python::attach(|_py| {
            let result = task_state_to_pystate(Err(xet_pkg::XetError::TaskError("outer error".into())));
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_progress_display_some() {
        let report = ItemProgressReport {
            item_name: "f".into(),
            total_bytes: 100,
            bytes_completed: 42,
        };
        assert_eq!(progress_display(Some(report)), "42/100");
    }

    #[test]
    fn test_progress_display_none() {
        assert_eq!(progress_display(None), "?/?");
    }
}
