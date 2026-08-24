//! Python bindings for XetRangeUploadCommit.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, XetRangeUploadCommit, XetRangeUploadEdit, XetRangeUploadReport, XetTaskState,
};
use xet_runtime::utils::UniqueId;

use super::py_range_upload_edit::PyXetRangeUploadEdit;
use crate::background_progress::BackgroundProgress;
use crate::headers::build_header_map;
use crate::utils::{blocking_call_with_signal_check, convert_xet_error, task_state_display, task_state_to_pystate};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn item_reports_from_edit_handles(handles: &Arc<RwLock<Vec<XetRangeUploadEdit>>>) -> HashMap<UniqueId, ItemProgressReport> {
    // XetRangeUploadEdit doesn't expose progress directly like XetFileUpload does.
    // For now, return an empty map.
    handles
        .read()
        .map(|g| g.iter().filter_map(|_| None).collect())
        .unwrap_or_default()
}

// ── build_range_upload_commit ────────────────────────────────────────────────

pub(crate) fn build_range_upload_commit(
    py: Python<'_>,
    session: &xet_pkg::xet_session::XetSession,
    original_hash: String,
    original_size: u64,
    endpoint: Option<String>,
    token: Option<String>,
    token_expiry_unix_secs: Option<u64>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    custom_headers: Option<HashMap<String, String>>,
    progress_callback: Option<Py<PyAny>>,
    progress_interval_ms: u64,
) -> PyResult<PyXetRangeUploadCommit> {
    let mut builder = session
        .new_range_upload()
        .map_err(convert_xet_error)?;

    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(&ep);
    }
    if let (Some(tok), Some(exp)) = (token, token_expiry_unix_secs) {
        builder = builder.with_token_info(tok, exp);
    }
    if let Some(url) = token_refresh_url {
        let headers = build_header_map(token_refresh_headers.unwrap_or_default())?;
        builder = builder.with_token_refresh_url(url, headers);
    }

    // custom_headers are already merged by PyXetSession via with_custom_headers
    if let Some(headers) = custom_headers {
        let hm = build_header_map(headers)?;
        builder = builder.with_custom_headers(hm);
    }

    let commit = py.detach(move || {
        builder
            .build_blocking(original_hash, original_size)
            .map_err(convert_xet_error)
    })?;

    let (edit_handles, progress) = if let Some(callback) = progress_callback {
        let handles: Arc<RwLock<Vec<XetRangeUploadEdit>>> = Arc::new(RwLock::new(Vec::new()));
        let inner = commit.clone();
        let handles_for_thread = handles.clone();
        let progress = BackgroundProgress::spawn(py, callback, progress_interval_ms, move || {
            let is_terminal = !matches!(inner.status(), Ok(XetTaskState::Running) | Ok(XetTaskState::Finalizing));
            let item_reports = item_reports_from_edit_handles(&handles_for_thread);
            (inner.progress(), item_reports, is_terminal)
        });
        (Some(handles), Some(progress))
    } else {
        (None, None)
    };

    Ok(PyXetRangeUploadCommit {
        inner: commit,
        edit_handles,
        progress,
    })
}

// ── PyXetRangeUploadCommit ───────────────────────────────────────────────────

/// A commit that edits an existing file by uploading only changed byte ranges.
///
/// Implements the context-manager protocol.
///
/// ```text
/// with session.new_range_upload() as commit:
///     commit.edit(1000, 2000).write(b"new data")
///     commit.append(500)
/// report = commit.commit()
/// # on normal exit: wait_to_finish() is called automatically
/// # on exception:   abort() is called automatically
/// ```
#[pyclass(name = "XetRangeUploadCommit")]
pub struct PyXetRangeUploadCommit {
    pub(crate) inner: XetRangeUploadCommit,
    /// Per-edit handles shared with the progress thread; None when no callback was registered.
    edit_handles: Option<Arc<RwLock<Vec<XetRangeUploadEdit>>>>,
    /// Background thread that polls progress and invokes the Python callback.
    progress: Option<BackgroundProgress>,
}

#[pymethods]
impl PyXetRangeUploadCommit {
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        format!("XetRangeUploadCommit(status=\"{}\")", status)
    }

    // ── Context manager ──────────────────────────────────────────────────

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Bound<'_, pyo3::PyAny>,
        _exc_val: Bound<'_, pyo3::PyAny>,
        _exc_tb: Bound<'_, pyo3::PyAny>,
    ) -> PyResult<bool> {
        if exc_type.is_none() {
            // Normal exit: commit (signal-interruptible).
            self.commit(py)?;
        } else {
            if let Err(e) = self.abort(py) {
                tracing::warn!("abort() failed during __exit__ exception path: {e}");
            }
        }
        Ok(false)
    }

    // ── Edit methods ─────────────────────────────────────────────────────

    /// Start a new edit: replace ``original_range`` with ``new_length`` bytes.
    ///
    /// Returns an :class:`XetRangeUploadEdit` handle.  Feed data incrementally
    /// with :meth:`XetRangeUploadEdit.write`, then call :meth:`XetRangeUploadEdit.finish`
    /// before committing.
    ///
    /// The ``original_range`` parameter accepts a tuple ``(start, end)``.
    pub fn edit(
        &self,
        original_range: (u64, u64),
        new_length: u64,
    ) -> PyResult<PyXetRangeUploadEdit> {
        let inner = self.inner.clone();
        let edit = inner.edit(original_range.0..original_range.1, new_length);
        if let Some(ref handles) = self.edit_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(edit.clone());
        }
        Ok(PyXetRangeUploadEdit { inner: edit })
    }

    /// Convenience: insert ``new_length`` bytes at position ``pos``.
    ///
    /// Equivalent to ``edit((pos, pos), new_length)``.
    pub fn insert(&self, pos: u64, new_length: u64) -> PyResult<PyXetRangeUploadEdit> {
        let inner = self.inner.clone();
        let edit = inner.insert(pos, new_length);
        if let Some(ref handles) = self.edit_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(edit.clone());
        }
        Ok(PyXetRangeUploadEdit { inner: edit })
    }

    /// Convenience: delete bytes at ``start..end``.
    ///
    /// Equivalent to ``edit((start, end), 0)``.
    pub fn delete(&self, start: u64, end: u64) -> PyResult<PyXetRangeUploadEdit> {
        let inner = self.inner.clone();
        let edit = inner.delete(start, end);
        if let Some(ref handles) = self.edit_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(edit.clone());
        }
        Ok(PyXetRangeUploadEdit { inner: edit })
    }

    /// Convenience: append ``new_length`` bytes at the end of the file.
    ///
    /// Equivalent to ``edit((original_size, original_size), new_length)``.
    pub fn append(&self, new_length: u64) -> PyResult<PyXetRangeUploadEdit> {
        let inner = self.inner.clone();
        let edit = inner.append(new_length);
        if let Some(ref handles) = self.edit_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(edit.clone());
        }
        Ok(PyXetRangeUploadEdit { inner: edit })
    }

    // ── Commit / abort ───────────────────────────────────────────────────

    /// Wait for all edits to be committed and return the result.
    ///
    /// Returns an :class:`XetRangeUploadReport` containing the composed file's
    /// :class:`XetFileInfo`.  Also called automatically when exiting a ``with``
    /// block without an exception.
    ///
    /// Releases the GIL while waiting, polling for ``KeyboardInterrupt`` every
    /// 100 ms so that Ctrl-C is delivered promptly.
    pub fn commit(&self, py: Python<'_>) -> PyResult<XetRangeUploadReport> {
        let inner = self.inner.clone();
        let result = blocking_call_with_signal_check(py, move || inner.commit_blocking());
        if let (Some(handles), Some(progress)) = (&self.edit_handles, &self.progress) {
            let progress_join_ret = if result.is_ok() {
                progress.stop_and_emit(py, || {
                    let _item_reports = item_reports_from_edit_handles(handles);
                    (self.inner.progress(), _item_reports)
                })
            } else {
                progress.stop_and_join(py)
            };
            if let Err(e) = progress_join_ret {
                tracing::warn!(error = ?e, "PyXetRangeUploadCommit progress thread join failed");
            }
        }
        result
    }

    /// Cancel all pending edits.
    pub fn abort(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(progress) = &self.progress {
            let _ = progress.stop_and_join(py);
        }
        self.inner.abort().map_err(convert_xet_error)
    }

    // ── Progress / status ────────────────────────────────────────────────

    /// Aggregate progress for this commit.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner.progress()
    }

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<crate::PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
    }
}