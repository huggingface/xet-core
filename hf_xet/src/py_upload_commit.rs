use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, Sha256Policy, XetCommitReport, XetFileUpload, XetTaskState,
    XetUploadCommit, XetUploadCommitBuilder,
};

use crate::convert_xet_error;
use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_file_upload_handle::{PyXetFileUpload, PyXetFileUploadResult};
use crate::py_stream_upload_handle::PyXetStreamUpload;
use crate::py_xet_session::task_state_to_str;

// ── PyXetUploadCommitBuilder ──────────────────────────────────────────────────

/// Fluent builder for :class:`XetUploadCommit`.
///
/// Obtain via :meth:`XetSession.new_upload_commit`.  Chain configuration
/// methods, then call :meth:`build` to create the commit.
///
/// Example — context manager (commit called automatically on exit):
///
/// ```text
/// with (session.new_upload_commit()
///       .with_endpoint("https://cas.xethub.hf.co")
///       .with_token_info("jwt", 9999999999)
///       .with_token_refresh_url("https://…/xet-write-token/main", {"Authorization": "Bearer hf_…"})
///       .build()) as commit:
///     commit.upload_file("/path/to/file.bin")
/// ```
///
/// Example — explicit commit to retrieve the report and per-file metadata:
///
/// ```text
/// commit = (session.new_upload_commit()
///           .with_endpoint("https://cas.xethub.hf.co")
///           .with_token_info("jwt", 9999999999)
///           .with_token_refresh_url("https://…/xet-write-token/main", {"Authorization": "Bearer hf_…"})
///           .build())
///
/// h1 = commit.upload_file("/path/to/model.bin")
/// h2 = commit.upload_file("/path/to/config.json")
///
/// report = commit.commit()   # blocks until all uploads are committed
///
/// for task_id, result in report.uploads.items():
///     print(task_id, result.hash, result.file_size, result.sha256)
///
/// # or look up a specific file's result via its handle:
/// result = report.uploads[h1.task_id()]
/// print(result.hash, result.file_size)
/// ```
#[pyclass(name = "XetUploadCommitBuilder")]
pub struct PyXetUploadCommitBuilder {
    pub(crate) inner: Option<XetUploadCommitBuilder>,
    pub(crate) progress_callback: Option<Py<PyAny>>,
    pub(crate) progress_interval_ms: u64,
}

#[pymethods]
impl PyXetUploadCommitBuilder {
    fn __repr__(&self) -> &'static str {
        "XetUploadCommitBuilder()"
    }

    /// Set the CAS server endpoint URL.
    ///
    /// If omitted and a token refresh URL is provided, the endpoint is read
    /// from the first refresh response.
    pub fn with_endpoint<'py>(mut slf: PyRefMut<'py, Self>, endpoint: String) -> PyRefMut<'py, Self> {
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_endpoint(endpoint));
        }
        slf
    }

    /// Seed an initial CAS access token and its Unix expiry timestamp.
    pub fn with_token_info<'py>(
        mut slf: PyRefMut<'py, Self>,
        token: String,
        expiry_unix_secs: u64,
    ) -> PyRefMut<'py, Self> {
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_token_info(token, expiry_unix_secs));
        }
        slf
    }

    /// Set a URL for automatic token refresh.
    ///
    /// ``headers`` should contain authentication credentials for the refresh
    /// endpoint (e.g. ``{"Authorization": "Bearer hf_…"}``).
    ///
    /// The endpoint must return JSON:
    /// ``{"accessToken": "…", "exp": <unix_secs>, "casUrl": "…"}``
    pub fn with_token_refresh_url<'py>(
        mut slf: PyRefMut<'py, Self>,
        url: String,
        headers: HashMap<String, String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let header_map = build_header_map(headers)?;
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_token_refresh_url(url, header_map));
        }
        Ok(slf)
    }

    /// Attach custom HTTP headers forwarded with every CAS request.
    ///
    /// A ``User-Agent: hf_xet/<version>`` header is automatically merged in
    /// (appended to any existing ``User-Agent`` value you supply).
    pub fn with_custom_headers<'py>(
        mut slf: PyRefMut<'py, Self>,
        headers: HashMap<String, String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let header_map = build_headers_with_user_agent(Some(headers))?;
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_custom_headers(header_map));
        }
        Ok(slf)
    }

    /// Register a Python callable to receive periodic progress updates.
    ///
    /// The callable is invoked approximately every 100 ms during the ``with``
    /// block with two positional arguments:
    ///
    /// 1. :class:`GroupProgressReport` — aggregate bytes processed / transferred.
    /// 2. ``list[ItemProgressReport]`` — one entry per queued file, with ``item_name``, ``total_bytes``, and
    ///    ``bytes_completed``.
    ///
    /// The thread exits automatically once the commit reaches a terminal state.
    ///
    /// ``interval_ms`` controls the polling interval in milliseconds (default: 100).
    ///
    /// Example:
    ///
    /// ```text
    /// def on_progress(group, items):
    ///     bar.n = group.total_bytes_completed
    ///     bar.total = group.total_bytes
    ///     bar.refresh()
    ///
    /// with builder.with_progress_callback(on_progress, interval_ms=50).build() as commit:
    ///     commit.upload_file("model.bin")
    /// ```
    #[pyo3(signature = (callback, interval_ms = 100))]
    pub fn with_progress_callback<'py>(
        mut slf: PyRefMut<'py, Self>,
        callback: Py<PyAny>,
        interval_ms: u64,
    ) -> PyRefMut<'py, Self> {
        slf.progress_callback = Some(callback);
        slf.progress_interval_ms = interval_ms;
        slf
    }

    /// Build the :class:`XetUploadCommit`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.  If a progress
    /// callback was registered, the polling thread is started immediately so
    /// that progress is reported for the full lifetime of the commit.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetUploadCommit> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("builder already consumed by build()"))?;
        let commit = py.detach(|| builder.build_blocking().map_err(convert_xet_error))?;

        let upload_handles = if self.progress_callback.is_some() {
            let handles: Arc<Mutex<Vec<XetFileUpload>>> = Arc::new(Mutex::new(Vec::new()));
            let callback = self.progress_callback.take().unwrap().clone_ref(py);
            let inner = commit.clone();
            let handles_for_thread = Arc::clone(&handles);
            let interval = Duration::from_millis(self.progress_interval_ms);
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let group_report = inner.progress();
                    let item_reports: Vec<ItemProgressReport> =
                        handles_for_thread.lock().unwrap().iter().filter_map(|h| h.progress()).collect();
                    let is_terminal =
                        !matches!(inner.status(), Ok(XetTaskState::Running) | Ok(XetTaskState::Finalizing));
                    let result = Python::attach(|py| callback.call1(py, (group_report, item_reports)));
                    if result.is_err() || is_terminal {
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
}

// ── PyXetUploadCommit ─────────────────────────────────────────────────────────

/// A group of related file uploads.
///
/// Implements the context-manager protocol.
///
/// ```text
/// with commit_builder.build() as commit:
///     h = commit.upload_file("/path/to/file.bin")
/// # on normal exit: commit() is called automatically
/// # on exception:   abort() is called automatically
/// ```
#[pyclass(name = "XetUploadCommit")]
pub struct PyXetUploadCommit {
    pub(crate) inner: XetUploadCommit,
    /// Per-file handles shared with the progress thread; None when no callback was registered.
    upload_handles: Option<Arc<Mutex<Vec<XetFileUpload>>>>,
}

#[pymethods]
impl PyXetUploadCommit {
    fn __repr__(&self) -> &'static str {
        "XetUploadCommit()"
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
            // Normal exit: commit all uploads.
            let inner = self.inner.clone();
            py.detach(|| inner.commit_blocking().map_err(convert_xet_error))?;
        } else {
            // Exception: cancel uploads.
            let _ = self.inner.abort();
        }
        Ok(false) // do not suppress the exception
    }

    // ── Upload methods ───────────────────────────────────────────────────────

    /// Queue a file from disk for upload.
    ///
    /// Returns immediately with a :class:`XetFileUpload` handle.  The upload
    /// runs in the background.  Call :meth:`XetUploadCommit.commit` (or exit
    /// the ``with`` block) to wait for all uploads to complete.
    #[pyo3(signature = (path, sha256=None))]
    pub fn upload_file(
        &self,
        py: Python<'_>,
        path: String,
        sha256: Option<PySha256Policy>,
    ) -> PyResult<PyXetFileUpload> {
        let policy = sha256.map(|p| p.inner).unwrap_or(Sha256Policy::Compute);
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_from_path_blocking(path.into(), policy).map_err(convert_xet_error))?;
        if let Some(ref handles) = self.upload_handles {
            handles.lock().unwrap().push(handle.clone());
        }
        Ok(PyXetFileUpload { inner: handle })
    }

    /// Queue raw bytes for upload.
    ///
    /// ``name`` is an optional display name used for progress reporting and
    /// telemetry.
    #[pyo3(signature = (data, name=None, sha256=None))]
    pub fn upload_bytes(
        &self,
        py: Python<'_>,
        data: Vec<u8>,
        name: Option<String>,
        sha256: Option<PySha256Policy>,
    ) -> PyResult<PyXetFileUpload> {
        let policy = sha256.map(|p| p.inner).unwrap_or(Sha256Policy::Compute);
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_bytes_blocking(data, policy, name).map_err(convert_xet_error))?;
        if let Some(ref handles) = self.upload_handles {
            handles.lock().unwrap().push(handle.clone());
        }
        Ok(PyXetFileUpload { inner: handle })
    }

    /// Open a streaming upload channel.
    ///
    /// Returns a :class:`XetStreamUpload` handle.  Feed data incrementally
    /// with :meth:`XetStreamUpload.write`, then call :meth:`XetStreamUpload.finish`
    /// **before** calling :meth:`XetUploadCommit.commit`.
    ///
    /// ``name`` is an optional display name used for progress reporting and
    /// telemetry.
    ///
    /// Example:
    ///
    /// ```text
    /// stream = commit.upload_stream(name="model.bin")
    /// for chunk in produce_chunks():
    ///     stream.write(chunk)
    /// result = stream.finish()   # must be called before commit()
    /// print(result.hash, result.file_size)
    /// ```
    #[pyo3(signature = (name=None, sha256=None))]
    pub fn upload_stream(&self, name: Option<String>, sha256: Option<PySha256Policy>) -> PyResult<PyXetStreamUpload> {
        let policy = sha256.map(|p| p.inner).unwrap_or(Sha256Policy::Compute);
        let handle = self.inner.upload_stream_blocking(name, policy).map_err(convert_xet_error)?;
        Ok(PyXetStreamUpload { inner: handle })
    }

    // ── Commit / abort ───────────────────────────────────────────────────────

    /// Wait for all uploads to finish and push metadata to the CAS server.
    ///
    /// Returns a :class:`XetCommitReport`.  Also called automatically when
    /// exiting a ``with`` block without an exception.
    ///
    /// Releases the GIL while waiting.
    pub fn commit(&self, py: Python<'_>) -> PyResult<PyXetCommitReport> {
        let inner = self.inner.clone();
        let report = py.detach(|| inner.commit_blocking().map_err(convert_xet_error))?;
        Ok(PyXetCommitReport::from(report))
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

    /// Current task state: ``"Running"``, ``"Finalizing"``, ``"Completed"``, or
    /// ``"UserCancelled"``.  Raises on error state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }
}

// ── PyXetCommitReport ─────────────────────────────────────────────────────────

/// Summary returned by :meth:`XetUploadCommit.commit`.
#[pyclass(name = "XetCommitReport", get_all)]
pub struct PyXetCommitReport {
    /// Final aggregate progress at the time the commit completed.
    pub progress: GroupProgressReport,
    /// Per-file upload results, keyed by an internal task ID string.
    pub uploads: HashMap<String, PyXetFileUploadResult>,
}

#[pymethods]
impl PyXetCommitReport {
    fn __repr__(&self) -> String {
        format!("XetCommitReport({} uploads)", self.uploads.len())
    }
}

impl From<XetCommitReport> for PyXetCommitReport {
    fn from(r: XetCommitReport) -> Self {
        let uploads = r
            .uploads
            .into_iter()
            .map(|(id, meta)| (id.to_string(), PyXetFileUploadResult::from(meta)))
            .collect();
        Self {
            progress: r.progress,
            uploads,
        }
    }
}

// ── PySha256Policy ────────────────────────────────────────────────────────────

/// Controls how SHA-256 is handled during upload.
///
/// Pass one of the three factory methods to ``upload_file`` or
/// ``upload_bytes``:
///
/// ```text
/// commit.upload_file("model.bin", sha256=hf_xet.Sha256Policy.compute())
/// commit.upload_file("model.bin", sha256=hf_xet.Sha256Policy.provided("a1b2…"))
/// commit.upload_file("model.bin", sha256=hf_xet.Sha256Policy.skip())
/// ```
#[pyclass(name = "Sha256Policy")]
#[derive(Clone)]
pub struct PySha256Policy {
    pub(crate) inner: Sha256Policy,
}

#[pymethods]
impl PySha256Policy {
    /// Compute SHA-256 from the file data (default).
    #[staticmethod]
    pub fn compute() -> Self {
        Self {
            inner: Sha256Policy::Compute,
        }
    }

    /// Use a pre-computed SHA-256 hex string.
    #[staticmethod]
    pub fn provided(hex: &str) -> Self {
        Self {
            inner: Sha256Policy::from_hex(hex),
        }
    }

    /// Skip SHA-256 entirely.
    #[staticmethod]
    pub fn skip() -> Self {
        Self {
            inner: Sha256Policy::Skip,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pyo3::Python;
    use xet_pkg::xet_session::{GroupProgressReport, Sha256Policy};

    use super::*;

    // ── PySha256Policy ────────────────────────────────────────────────────────

    #[test]
    fn test_sha256_policy_compute() {
        assert!(matches!(PySha256Policy::compute().inner, Sha256Policy::Compute));
    }

    #[test]
    fn test_sha256_policy_skip() {
        assert!(matches!(PySha256Policy::skip().inner, Sha256Policy::Skip));
    }

    #[test]
    fn test_sha256_policy_provided() {
        // Must be a valid 64-char sha256 hex string.
        let valid_hex = "a".repeat(64);
        assert!(matches!(PySha256Policy::provided(&valid_hex).inner, Sha256Policy::Provided(_)));
    }

    // ── PyXetUploadCommitBuilder ──────────────────────────────────────────────

    #[test]
    fn test_builder_repr() {
        let builder = PyXetUploadCommitBuilder {
            inner: None,
            progress_callback: None,
            progress_interval_ms: 100,
        };
        assert_eq!(builder.__repr__(), "XetUploadCommitBuilder()");
    }

    #[test]
    fn test_builder_build_when_consumed_returns_error() {
        let mut builder = PyXetUploadCommitBuilder {
            inner: None,
            progress_callback: None,
            progress_interval_ms: 100,
        };
        let err = Python::attach(|py| builder.build(py).err().expect("expected error").to_string());
        assert!(err.contains("already consumed"));
    }

    // ── PyXetCommitReport ─────────────────────────────────────────────────────

    #[test]
    fn test_commit_report_repr_empty() {
        let report = PyXetCommitReport {
            progress: GroupProgressReport::default(),
            uploads: HashMap::new(),
        };
        assert_eq!(report.__repr__(), "XetCommitReport(0 uploads)");
    }

    #[test]
    fn test_commit_report_repr_with_uploads() {
        let mut uploads = HashMap::new();
        uploads.insert(
            "task-1".into(),
            PyXetFileUploadResult {
                hash: "h".into(),
                file_size: 1,
                sha256: None,
            },
        );
        let report = PyXetCommitReport {
            progress: GroupProgressReport::default(),
            uploads,
        };
        assert_eq!(report.__repr__(), "XetCommitReport(1 uploads)");
    }
}
