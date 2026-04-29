//! Shared display helpers used across Python binding modules.

use pyo3::PyResult;
use xet_pkg::xet_session::{ItemProgressReport, XetTaskState};

use crate::convert_xet_error;

// ── Task-state helpers ────────────────────────────────────────────────────────

/// Map a [`XetTaskState`] to its Python-facing string, or raise on error state.
pub(crate) fn task_state_to_str(state: XetTaskState) -> PyResult<&'static str> {
    match state {
        XetTaskState::Running => Ok("Running"),
        XetTaskState::Finalizing => Ok("Finalizing"),
        XetTaskState::Completed => Ok("Completed"),
        XetTaskState::UserCancelled => Ok("UserCancelled"),
        XetTaskState::Error(msg) => Err(convert_xet_error(xet_pkg::XetError::TaskError(msg))),
    }
}

/// Convert a `status()` result to a display string for use in `__repr__`.
///
/// Unlike [`task_state_to_str`], this never discards information: the error
/// message inside `XetTaskState::Error(msg)` and any `XetError` from the
/// `status()` call itself are both forwarded as the returned string.
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
    use super::*;

    #[test]
    fn test_task_state_error_returns_err() {
        let msg = task_state_to_str(XetTaskState::Error("something went wrong".into()))
            .unwrap_err()
            .to_string();
        assert!(msg.contains("something went wrong"));
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
