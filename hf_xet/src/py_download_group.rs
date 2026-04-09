use std::collections::HashMap;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, XetDownloadGroupReport, XetDownloadReport, XetFileDownload,
    XetFileDownloadGroup, XetFileDownloadGroupBuilder, XetFileInfo,
};

use crate::convert_xet_error;
use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_xet_session::task_state_to_str;
use crate::PyXetDownloadInfo;

// ── PyXetFileDownloadGroupBuilder ─────────────────────────────────────────────

/// Fluent builder for :class:`XetFileDownloadGroup`.
///
/// Obtain via :meth:`XetSession.new_file_download_group`.  Chain configuration
/// methods, then call :meth:`build` to create the group.
///
/// Example (Python):
///
/// ```text
/// with (session.new_file_download_group()
///       .with_token_info("jwt", 9999999999)
///       .with_token_refresh_url("https://…/xet-read-token/main", {"Authorization": "Bearer hf_…"})
///       .build()) as group:
///     group.download_file(info, "/tmp/out.bin")
/// ```
#[pyclass(name = "XetFileDownloadGroupBuilder")]
pub struct PyXetFileDownloadGroupBuilder {
    pub(crate) inner: Option<XetFileDownloadGroupBuilder>,
}

#[pymethods]
impl PyXetFileDownloadGroupBuilder {
    fn __repr__(&self) -> &'static str {
        "XetFileDownloadGroupBuilder()"
    }

    /// Set the CAS server endpoint URL.
    pub fn with_endpoint<'py>(mut slf: PyRefMut<'py, Self>, endpoint: String) -> PyRefMut<'py, Self> {
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_endpoint(endpoint));
        }
        slf
    }

    /// Seed an initial CAS access token and its Unix expiry timestamp.
    pub fn with_token_info<'py>(mut slf: PyRefMut<'py, Self>, token: String, expiry_unix_secs: u64) -> PyRefMut<'py, Self> {
        if let Some(b) = slf.inner.take() {
            slf.inner = Some(b.with_token_info(token, expiry_unix_secs));
        }
        slf
    }

    /// Set a URL for automatic token refresh.
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

    /// Build the :class:`XetFileDownloadGroup`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetFileDownloadGroup> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("builder already consumed by build()"))?;
        let group = py.detach(|| builder.build_blocking().map_err(convert_xet_error))?;
        Ok(PyXetFileDownloadGroup { inner: group })
    }
}

// ── PyXetFileDownloadGroup ────────────────────────────────────────────────────

/// A group of related file downloads.
///
/// Implements the context-manager protocol.
///
/// ```text
/// with group_builder.build() as group:
///     h = group.download_file(info, "/tmp/out.bin")
/// # on normal exit: finish() is called automatically
/// # on exception:   abort() is called automatically
/// ```
#[pyclass(name = "XetFileDownloadGroup")]
#[derive(Clone)]
pub struct PyXetFileDownloadGroup {
    pub(crate) inner: XetFileDownloadGroup,
}

#[pymethods]
impl PyXetFileDownloadGroup {
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
            // Normal exit: wait for all downloads.
            let group = self.inner.clone();
            py.detach(|| group.finish_blocking().map_err(convert_xet_error))?;
        } else {
            let _ = self.inner.abort();
        }
        Ok(false)
    }

    // ── Download methods ─────────────────────────────────────────────────────

    /// Queue a file for download to ``dest_path``.
    ///
    /// Accepts a :class:`PyXetDownloadInfo` (the existing download-info type)
    /// whose ``hash`` and ``file_size`` fields identify the file, and whose
    /// ``destination_path`` is used as the local output path unless
    /// ``dest_path`` overrides it.
    ///
    /// Returns immediately with a :class:`XetFileDownload` handle.
    #[pyo3(signature = (file_info, dest_path=None))]
    pub fn download_file(
        &self,
        py: Python<'_>,
        file_info: PyRef<'_, PyXetDownloadInfo>,
        dest_path: Option<String>,
    ) -> PyResult<PyXetFileDownload> {
        let xet_info = xet_info_from_download_info(&file_info);
        let path: std::path::PathBuf = dest_path.as_deref().unwrap_or(&file_info.destination_path).into();
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.download_file_to_path_blocking(xet_info, path).map_err(convert_xet_error))?;
        Ok(PyXetFileDownload { inner: handle })
    }

    // ── Finish / abort ───────────────────────────────────────────────────────

    /// Wait for all downloads to complete and return a summary report.
    ///
    /// Returns a :class:`XetDownloadGroupReport`.  Also called automatically
    /// when exiting a ``with`` block without an exception.
    ///
    /// Releases the GIL while waiting.
    pub fn finish(&self, py: Python<'_>) -> PyResult<PyXetDownloadGroupReport> {
        let group = self.inner.clone();
        let report = py.detach(|| group.finish_blocking().map_err(convert_xet_error))?;
        Ok(PyXetDownloadGroupReport::from(report))
    }

    /// Cancel all active downloads in this group.
    pub fn abort(&self) -> PyResult<()> {
        self.inner.abort().map_err(convert_xet_error)
    }

    // ── Progress / status ────────────────────────────────────────────────────

    /// Aggregate progress for all downloads in this group.
    ///
    /// Returns a :class:`GroupProgressReport`.  Lock-free.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner.progress()
    }

    /// Current task state (same values as :meth:`XetSession.status`).
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }
}

// ── PyXetFileDownload ─────────────────────────────────────────────────────────

/// Handle for a background file-download task.
///
/// Returned by :meth:`XetFileDownloadGroup.download_file`.
#[pyclass(name = "XetFileDownload")]
pub struct PyXetFileDownload {
    pub(crate) inner: XetFileDownload,
}

#[pymethods]
impl PyXetFileDownload {
    fn __repr__(&self) -> String {
        format!("XetFileDownload(task_id={:?})", self.inner.task_id().to_string())
    }

    /// Per-file progress, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// Wait for this download to complete and return its report.
    ///
    /// Releases the GIL.
    pub fn result(&self, py: Python<'_>) -> PyResult<PyXetDownloadReport> {
        let inner = self.inner.clone();
        let report = py.detach(|| inner.finish_blocking().map_err(convert_xet_error))?;
        Ok(PyXetDownloadReport::from(report))
    }

    /// Return the download report without blocking.
    ///
    /// Returns ``None`` if the download has not yet completed.
    /// Raises if the download completed with an error.
    pub fn try_result(&self) -> PyResult<Option<PyXetDownloadReport>> {
        match self.inner.result() {
            Some(Ok(r)) => Ok(Some(PyXetDownloadReport::from(r))),
            Some(Err(e)) => Err(convert_xet_error(e)),
            None => Ok(None),
        }
    }

    /// The unique task ID for this download, as a string.
    ///
    /// Matches the keys in :attr:`XetDownloadGroupReport.downloads`.
    pub fn task_id(&self) -> String {
        self.inner.task_id().to_string()
    }

    /// Cancel this individual download.
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

// ── PyXetDownloadReport ───────────────────────────────────────────────────────

/// Per-file result from a completed download.
#[pyclass(name = "XetDownloadReport", get_all)]
#[derive(Clone)]
pub struct PyXetDownloadReport {
    /// Local path where the file was written.
    pub path: Option<String>,
    /// Content-addressed hash of the downloaded file.
    pub hash: String,
    /// File size in bytes.
    pub file_size: Option<u64>,
}

#[pymethods]
impl PyXetDownloadReport {
    fn __repr__(&self) -> String {
        format!("XetDownloadReport(hash={:?}, path={:?}, file_size={:?})", self.hash, self.path, self.file_size)
    }
}

impl From<XetDownloadReport> for PyXetDownloadReport {
    fn from(r: XetDownloadReport) -> Self {
        Self {
            path: r.path.and_then(|p| p.to_str().map(str::to_owned)),
            hash: r.file_info.hash().to_owned(),
            file_size: r.file_info.file_size(),
        }
    }
}

// ── PyXetDownloadGroupReport ──────────────────────────────────────────────────

/// Summary returned by :meth:`XetFileDownloadGroup.finish`.
#[pyclass(name = "XetDownloadGroupReport", get_all)]
pub struct PyXetDownloadGroupReport {
    /// Final aggregate progress at the time the group completed.
    pub progress: GroupProgressReport,
    /// Per-file download reports, keyed by an internal task ID string.
    pub downloads: HashMap<String, PyXetDownloadReport>,
}

#[pymethods]
impl PyXetDownloadGroupReport {
    fn __repr__(&self) -> String {
        format!("XetDownloadGroupReport({} downloads)", self.downloads.len())
    }
}

impl From<XetDownloadGroupReport> for PyXetDownloadGroupReport {
    fn from(r: XetDownloadGroupReport) -> Self {
        let downloads = r.downloads.into_iter().map(|(id, dr)| (id.to_string(), PyXetDownloadReport::from(dr))).collect();
        Self { progress: r.progress, downloads }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn xet_info_from_download_info(info: &PyXetDownloadInfo) -> XetFileInfo {
    match info.file_size {
        Some(size) => XetFileInfo::new(info.hash.clone(), size),
        None => XetFileInfo::new_hash_only(info.hash.clone()),
    }
}
