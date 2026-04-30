use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, Sha256Policy, UniqueID, XetCommitReport, XetFileUpload, XetSession,
    XetTaskState, XetUploadCommit,
};

// ── SHA-256 policy sentinels ──────────────────────────────────────────────────

/// Sentinel: compute SHA-256 from the file data (default behaviour).
///
/// Pass this as the ``sha256`` argument to :meth:`XetUploadCommit.start_upload_file`,
/// :meth:`XetUploadCommit.start_upload_bytes`, or :meth:`XetUploadCommit.start_upload_stream`.
#[pyclass(frozen, name = "_ComputeSha256Type")]
pub struct PyComputeSha256;

#[pymethods]
impl PyComputeSha256 {
    fn __repr__(&self) -> &'static str {
        "COMPUTE_SHA256"
    }
}

/// Sentinel: skip SHA-256 computation entirely.
///
/// Pass this as the ``sha256`` argument to :meth:`XetUploadCommit.start_upload_file`,
/// :meth:`XetUploadCommit.start_upload_bytes`, or :meth:`XetUploadCommit.start_upload_stream`.
#[pyclass(frozen, name = "_SkipSha256Type")]
pub struct PySkipSha256;

#[pymethods]
impl PySkipSha256 {
    fn __repr__(&self) -> &'static str {
        "SKIP_SHA256"
    }
}

/// Convert the Python ``sha256`` argument to a :type:`Sha256Policy`.
///
/// Accepts:
/// - ``None`` or :data:`COMPUTE_SHA256` → compute from data
/// - :data:`SKIP_SHA256` → skip
/// - ``str`` → treat as a pre-computed hex digest
fn parse_sha256(py: Python<'_>, sha256: Option<Py<PyAny>>) -> PyResult<Sha256Policy> {
    match sha256 {
        None => Ok(Sha256Policy::Compute),
        Some(obj) => {
            let obj = obj.bind(py);
            if obj.is_instance_of::<PyComputeSha256>() {
                Ok(Sha256Policy::Compute)
            } else if obj.is_instance_of::<PySkipSha256>() {
                Ok(Sha256Policy::Skip)
            } else if let Ok(hex) = obj.extract::<String>() {
                Ok(Sha256Policy::from_hex(&hex))
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err("sha256 must be a str, COMPUTE_SHA256, or SKIP_SHA256"))
            }
        },
    }
}

use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_file_upload_handle::PyXetFileUpload;
use crate::py_stream_upload_handle::PyXetStreamUpload;
use crate::utils::{progress_display, task_state_display, task_state_to_pystate};
use crate::{PyXetTaskState, blocking_call_with_signal_check, convert_xet_error};

// ── build_upload_commit ───────────────────────────────────────────────────────

/// Create an :class:`XetUploadCommit` from a session and optional configuration.
///
/// Called by :meth:`XetSession.new_upload_commit`.  The Rust builder type is
/// created and consumed entirely here — it never surfaces in any public API.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_upload_commit(
    py: Python<'_>,
    session: &XetSession,
    endpoint: Option<String>,
    token: Option<String>,
    token_expiry_unix_secs: Option<u64>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    custom_headers: Option<HashMap<String, String>>,
    progress_callback: Option<Py<PyAny>>,
    progress_interval_ms: u64,
) -> PyResult<PyXetUploadCommit> {
    let mut builder = session.new_upload_commit().map_err(convert_xet_error)?;
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }
    if let (Some(tok), Some(exp)) = (token, token_expiry_unix_secs) {
        builder = builder.with_token_info(tok, exp);
    }
    if let Some(url) = token_refresh_url {
        let headers = build_header_map(token_refresh_headers.unwrap_or_default())?;
        builder = builder.with_token_refresh_url(url, headers);
    }
    let merged_headers = build_headers_with_user_agent(custom_headers)?;
    let commit = py.detach(move || {
        builder
            .with_custom_headers(merged_headers)
            .build_blocking()
            .map_err(convert_xet_error)
    })?;

    let upload_handles = if let Some(callback) = progress_callback {
        let handles: Arc<RwLock<Vec<XetFileUpload>>> = Arc::new(RwLock::new(Vec::new()));
        let inner = commit.clone();
        let handles_for_thread = handles.clone();
        let interval = Duration::from_millis(progress_interval_ms);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let is_terminal = !matches!(inner.status(), Ok(XetTaskState::Running) | Ok(XetTaskState::Finalizing));
                let group_report = inner.progress();
                let item_reports: HashMap<UniqueID, ItemProgressReport> = handles_for_thread
                    .read()
                    .map(|g| g.iter().filter_map(|h| h.progress().map(|p| (h.task_id(), p))).collect())
                    .unwrap_or_default();
                let result = Python::attach(|py| callback.call1(py, (group_report, item_reports)));
                if let Err(e) = result {
                    Python::attach(|py| e.print(py));
                    break;
                }
                if is_terminal {
                    break;
                }
            }
        });
        Some(handles)
    } else {
        None
    };

    Ok(PyXetUploadCommit {
        inner: commit,
        upload_handles,
    })
}

// ── PyXetUploadCommit ─────────────────────────────────────────────────────────

/// A group of related file uploads.
///
/// Implements the context-manager protocol.
///
/// ```text
/// with session.new_upload_commit(endpoint="...") as commit:
///     h = commit.start_upload_file("/path/to/file.bin")
/// # on normal exit: wait_to_finish() is called automatically
/// # on exception:   abort() is called automatically
/// ```
#[pyclass(name = "XetUploadCommit")]
pub struct PyXetUploadCommit {
    pub(crate) inner: XetUploadCommit,
    /// Per-file handles shared with the progress thread; None when no callback was registered.
    upload_handles: Option<Arc<RwLock<Vec<XetFileUpload>>>>,
}

#[pymethods]
impl PyXetUploadCommit {
    // Example output:
    //   XetUploadCommit(status="Running", uploads=[(1, "/path/model.bin", bytes_completed=1024/4096), (2, None,
    // bytes_completed=?/?)])
    //
    // Each upload entry is (task_id, path_or_None, bytes_completed/total_bytes).
    // Path is None for uploads started from bytes rather than a file path.
    // Progress shows "?/?" before the first report arrives.
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        let uploads: Vec<String> = self
            .inner
            .active_upload_info()
            .into_iter()
            .map(|(id, path, progress)| {
                let p = match path {
                    Some(pb) => format!("\"{}\"", pb.display()),
                    None => "None".to_string(),
                };
                let prog = progress_display(progress);
                format!("({id}, {p}, bytes_completed={prog})")
            })
            .collect();
        format!("XetUploadCommit(status=\"{}\", uploads=[{}])", status, uploads.join(", "))
    }

    // ── Context manager ──────────────────────────────────────────────────────

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
            // Normal exit: commit all uploads (signal-interruptible).
            self.wait_to_finish(py)?;
        } else {
            // Exception: cancel uploads.
            if let Err(e) = self.inner.abort() {
                tracing::warn!("abort() failed during __exit__ exception path: {e}");
            }
        }
        Ok(false) // do not suppress the exception
    }

    // ── Upload methods ───────────────────────────────────────────────────────

    /// Queue a file from disk for upload.
    ///
    /// Returns immediately with a :class:`XetFileUpload` handle.  The upload
    /// runs in the background.  Call :meth:`XetUploadCommit.commit` (or exit
    /// the ``with`` block) to wait for all uploads to complete.
    ///
    /// ``sha256`` controls how the SHA-256 digest is handled:
    ///
    /// - ``sha256="f2358d9a…"`` — pre-computed hex string (most common for models/datasets)
    /// - ``sha256=hf_xet.COMPUTE_SHA256`` — compute from file data (default when omitted)
    /// - ``sha256=hf_xet.SKIP_SHA256`` — skip SHA-256 entirely
    #[pyo3(signature = (path, sha256=None))]
    pub fn start_upload_file(
        &self,
        py: Python<'_>,
        path: String,
        sha256: Option<Py<PyAny>>,
    ) -> PyResult<PyXetFileUpload> {
        let policy = parse_sha256(py, sha256)?;
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_from_path_blocking(path.into(), policy).map_err(convert_xet_error))?;
        if let Some(ref handles) = self.upload_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(handle.clone());
        }
        Ok(PyXetFileUpload { inner: handle })
    }

    /// Queue raw bytes for upload.
    ///
    /// ``name`` is an optional display name used for progress reporting.
    /// ``sha256`` accepts the same values as :meth:`start_upload_file`.
    #[pyo3(signature = (data, sha256=None, name=None))]
    pub fn start_upload_bytes(
        &self,
        py: Python<'_>,
        data: Vec<u8>,
        sha256: Option<Py<PyAny>>,
        name: Option<String>,
    ) -> PyResult<PyXetFileUpload> {
        let policy = parse_sha256(py, sha256)?;
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_bytes_blocking(data, policy, name).map_err(convert_xet_error))?;
        if let Some(ref handles) = self.upload_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(handle.clone());
        }
        Ok(PyXetFileUpload { inner: handle })
    }

    /// Open a streaming upload channel.
    ///
    /// Returns a :class:`XetStreamUpload` handle.  Feed data incrementally
    /// with :meth:`XetStreamUpload.write`, then call :meth:`XetStreamUpload.finish`
    /// **before** calling :meth:`XetUploadCommit.commit`.
    ///
    /// ``name`` is an optional display name used for progress reporting.
    /// ``sha256`` accepts the same values as :meth:`start_upload_file`.
    ///
    /// Example:
    ///
    /// ```text
    /// stream = commit.start_upload_stream(name="model.bin")
    /// for chunk in produce_chunks():
    ///     stream.write(chunk)
    /// result = stream.finish()   # must be called before wait_to_finish()
    /// print(result.xet_info.hash, result.xet_info.file_size)
    /// ```
    #[pyo3(signature = (name=None, sha256=None))]
    pub fn start_upload_stream(
        &self,
        py: Python<'_>,
        name: Option<String>,
        sha256: Option<Py<PyAny>>,
    ) -> PyResult<PyXetStreamUpload> {
        let policy = parse_sha256(py, sha256)?;
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_stream_blocking(name, policy).map_err(convert_xet_error))?;
        Ok(PyXetStreamUpload { inner: handle })
    }

    // ── Commit / abort ───────────────────────────────────────────────────────

    /// Wait for all uploads to finish and push metadata to the CAS server.
    ///
    /// Returns a :class:`XetCommitReport`.  Also called automatically when
    /// exiting a ``with`` block without an exception.
    ///
    /// Releases the GIL while waiting, polling for ``KeyboardInterrupt`` every
    /// 100 ms so that Ctrl-C is delivered promptly.
    pub fn wait_to_finish(&self, py: Python<'_>) -> PyResult<XetCommitReport> {
        let inner = self.inner.clone();
        blocking_call_with_signal_check(py, move || inner.commit_blocking())
    }

    /// Cancel all active uploads in this commit.
    pub fn abort(&self) -> PyResult<()> {
        self.inner.abort().map_err(convert_xet_error)
    }

    // ── Progress / status ────────────────────────────────────────────────────

    /// Aggregate progress for all uploads in this commit.
    ///
    /// Returns a :class:`GroupProgressReport`.  Lock-free — safe to call from
    /// any thread without holding the GIL.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner.progress()
    }

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use xet_pkg::xet_session::Sha256Policy;

    use super::*;

    // ── parse_sha256 ──────────────────────────────────────────────────────────

    #[test]
    fn test_parse_sha256_none_gives_compute() {
        Python::attach(|py| {
            let policy = parse_sha256(py, None).unwrap();
            assert!(matches!(policy, Sha256Policy::Compute));
        });
    }

    #[test]
    fn test_parse_sha256_compute_sentinel() {
        Python::attach(|py| {
            let sentinel: Py<PyAny> = Py::new(py, PyComputeSha256).unwrap().into();
            let policy = parse_sha256(py, Some(sentinel)).unwrap();
            assert!(matches!(policy, Sha256Policy::Compute));
        });
    }

    #[test]
    fn test_parse_sha256_skip_sentinel() {
        Python::attach(|py| {
            let sentinel: Py<PyAny> = Py::new(py, PySkipSha256).unwrap().into();
            let policy = parse_sha256(py, Some(sentinel)).unwrap();
            assert!(matches!(policy, Sha256Policy::Skip));
        });
    }

    #[test]
    fn test_parse_sha256_provided_hex_string() {
        Python::attach(|py| {
            let hex = "a".repeat(64);
            let obj: Py<PyAny> = hex.into_pyobject(py).unwrap().into_any().unbind();
            let policy = parse_sha256(py, Some(obj)).unwrap();
            assert!(matches!(policy, Sha256Policy::Provided(_)));
        });
    }

    #[test]
    fn test_parse_sha256_invalid_type_returns_type_error() {
        Python::attach(|py| {
            let obj: Py<PyAny> = 42i64.into_pyobject(py).unwrap().into_any().unbind();
            match parse_sha256(py, Some(obj)) {
                Ok(_) => panic!("expected TypeError"),
                Err(e) => assert!(e.is_instance_of::<pyo3::exceptions::PyTypeError>(py)),
            }
        });
    }

    // ── PyXetUploadCommit ─────────────────────────────────────────────────────

    #[test]
    fn test_start_upload_bytes_and_commit_report() {
        use tempfile::tempdir;
        use xet_pkg::xet_session::XetSessionBuilder;

        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = PyXetUploadCommit {
            inner: session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap(),
            upload_handles: None,
        };

        Python::attach(|py| {
            let handle = commit.start_upload_bytes(py, b"hello world".to_vec(), None, None).unwrap();
            let task_id = handle.task_id();
            let report = commit.wait_to_finish(py).unwrap();
            assert!(report.uploads.contains_key(&task_id));
            let meta = &report.uploads[&task_id];
            assert_eq!(meta.xet_info.file_size, Some(11));
            assert!(!meta.xet_info.hash.is_empty());
        });
    }

    #[test]
    fn test_abort_makes_commit_fail() {
        use tempfile::tempdir;
        use xet_pkg::xet_session::XetSessionBuilder;

        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = PyXetUploadCommit {
            inner: session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap(),
            upload_handles: None,
        };

        Python::attach(|py| {
            commit.abort().unwrap();
            assert!(commit.wait_to_finish(py).is_err());
        });
    }
}
