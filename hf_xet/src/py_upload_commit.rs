use std::collections::HashMap;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, Sha256Policy, XetCommitReport, XetFileMetadata, XetFileUpload,
    XetUploadCommit, XetUploadCommitBuilder,
};

use crate::convert_xet_error;
use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_xet_session::task_state_to_str;

// ── PyXetUploadCommitBuilder ──────────────────────────────────────────────────

/// Fluent builder for :class:`XetUploadCommit`.
///
/// Obtain via :meth:`XetSession.new_upload_commit`.  Chain configuration
/// methods, then call :meth:`build` to create the commit.
///
/// Example (Python):
///
/// ```text
/// with (session.new_upload_commit()
///       .with_endpoint("https://cas.xethub.hf.co")
///       .with_token_info("jwt", 9999999999)
///       .with_token_refresh_url("https://…/xet-write-token/main", {"Authorization": "Bearer hf_…"})
///       .build()) as commit:
///     commit.upload_file("/path/to/file.bin")
/// ```
#[pyclass(name = "XetUploadCommitBuilder")]
pub struct PyXetUploadCommitBuilder {
    pub(crate) inner: Option<XetUploadCommitBuilder>,
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

    /// Build the :class:`XetUploadCommit`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetUploadCommit> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("builder already consumed by build()"))?;
        let commit = py.detach(|| builder.build_blocking().map_err(convert_xet_error))?;
        Ok(PyXetUploadCommit { inner: commit })
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
///
/// Cloning is cheap — all clones share the same underlying state.
#[pyclass(name = "XetUploadCommit")]
#[derive(Clone)]
pub struct PyXetUploadCommit {
    pub(crate) inner: XetUploadCommit,
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
    ///
    /// ``sha256`` — optional pre-computed hex SHA-256 of the file.  Pass
    /// ``None`` to compute it automatically or ``""`` to skip SHA-256.
    #[pyo3(signature = (path, sha256=None))]
    pub fn upload_file(&self, py: Python<'_>, path: String, sha256: Option<String>) -> PyResult<PyXetFileUpload> {
        let policy = sha256_policy_from_opt(sha256)?;
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_from_path_blocking(path.into(), policy).map_err(convert_xet_error))?;
        Ok(PyXetFileUpload { inner: handle })
    }

    /// Queue raw bytes for upload.
    ///
    /// ``name`` is an optional display name used for progress reporting and
    /// telemetry.  ``sha256`` behaves the same as in :meth:`upload_file`.
    #[pyo3(signature = (data, name=None, sha256=None))]
    pub fn upload_bytes(
        &self,
        py: Python<'_>,
        data: Vec<u8>,
        name: Option<String>,
        sha256: Option<String>,
    ) -> PyResult<PyXetFileUpload> {
        let policy = sha256_policy_from_opt(sha256)?;
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.upload_bytes_blocking(data, policy, name).map_err(convert_xet_error))?;
        Ok(PyXetFileUpload { inner: handle })
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

// ── PyXetFileUpload ───────────────────────────────────────────────────────────

/// Handle for a background file-upload task.
///
/// Returned by :meth:`XetUploadCommit.upload_file` and
/// :meth:`XetUploadCommit.upload_bytes`.
#[pyclass(name = "XetFileUpload")]
pub struct PyXetFileUpload {
    pub(crate) inner: XetFileUpload,
}

#[pymethods]
impl PyXetFileUpload {
    fn __repr__(&self) -> String {
        format!("XetFileUpload(task_id={:?})", self.inner.task_id().to_string())
    }

    /// Per-file progress, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state (same values as :meth:`XetUploadCommit.status`).
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// Wait for ingestion to complete and return upload metadata.
    ///
    /// Releases the GIL.  Call after :meth:`XetUploadCommit.commit` to get
    /// the final :class:`XetFileUploadResult`.
    pub fn result(&self, py: Python<'_>) -> PyResult<PyXetFileUploadResult> {
        let inner = self.inner.clone();
        let meta = py.detach(|| inner.finalize_ingestion_blocking().map_err(convert_xet_error))?;
        Ok(PyXetFileUploadResult::from(meta))
    }

    /// Return upload metadata without blocking, or ``None`` if not yet done.
    pub fn try_result(&self) -> Option<PyXetFileUploadResult> {
        self.inner.try_finish().map(PyXetFileUploadResult::from)
    }

    /// The unique task ID for this upload, as a string.
    ///
    /// Matches the keys in :attr:`XetCommitReport.uploads`.
    pub fn task_id(&self) -> String {
        self.inner.task_id().to_string()
    }
}

// ── PyXetFileUploadResult ─────────────────────────────────────────────────────

/// Metadata returned after a successful file upload.
#[pyclass(name = "XetFileUploadResult", get_all)]
#[derive(Clone)]
pub struct PyXetFileUploadResult {
    /// Content-addressed hash identifying this file in the Xet storage system.
    pub hash: String,
    /// Total size of the file in bytes.
    pub file_size: u64,
    /// Hex-encoded SHA-256 digest of the original file, or ``None`` if skipped.
    pub sha256: Option<String>,
}

#[pymethods]
impl PyXetFileUploadResult {
    fn __repr__(&self) -> String {
        format!("XetFileUploadResult(hash={:?}, file_size={}, sha256={:?})", self.hash, self.file_size, self.sha256)
    }
}

impl From<XetFileMetadata> for PyXetFileUploadResult {
    fn from(meta: XetFileMetadata) -> Self {
        Self {
            hash: meta.xet_info.hash().to_owned(),
            file_size: meta.xet_info.file_size().unwrap_or(0),
            sha256: meta.xet_info.sha256().map(str::to_owned),
        }
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn sha256_policy_from_opt(sha256: Option<String>) -> PyResult<Sha256Policy> {
    match sha256 {
        None => Ok(Sha256Policy::Compute),
        Some(s) if s.is_empty() => Ok(Sha256Policy::Skip),
        Some(s) => Ok(Sha256Policy::from_hex(&s)),
    }
}
