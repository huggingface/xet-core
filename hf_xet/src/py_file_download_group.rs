use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, XetFileDownload, XetFileDownloadGroup, XetFileDownloadGroupBuilder,
    XetFileInfo, XetTaskState,
};

use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_file_download_handle::{PyXetDownloadGroupReport, PyXetFileDownload};
use crate::py_xet_session::task_state_to_str;
use crate::{PyXetDownloadInfo, convert_xet_error};

// ── PyXetFileDownloadGroupBuilder ─────────────────────────────────────────────

/// Fluent builder for :class:`XetFileDownloadGroup`.
///
/// Obtain via :meth:`XetSession.new_file_download_group`.  Chain configuration
/// methods, then call :meth:`build` to create the group.
///
/// Example — context manager (finish called automatically on exit):
///
/// ```text
/// with (session.new_file_download_group()
///       .with_token_info("jwt", 9999999999)
///       .with_token_refresh_url("https://…/xet-read-token/main", {"Authorization": "Bearer hf_…"})
///       .build()) as group:
///     group.download_file(info, "/tmp/out.bin")
/// ```
///
/// Example — explicit finish to retrieve the report and per-file metadata:
///
/// ```text
/// group = (session.new_file_download_group()
///          .with_token_info("jwt", 9999999999)
///          .with_token_refresh_url("https://…/xet-read-token/main", {"Authorization": "Bearer hf_…"})
///          .build())
///
/// h1 = group.download_file(info1, "/tmp/model.bin")
/// h2 = group.download_file(info2, "/tmp/config.json")
///
/// report = group.finish()   # blocks until all downloads complete
///
/// for task_id, result in report.downloads.items():
///     print(task_id, result.hash, result.path, result.file_size)
///
/// # or look up a specific file's result via its handle:
/// result = report.downloads[h1.task_id()]
/// print(result.hash, result.path)
/// ```
#[pyclass(name = "XetFileDownloadGroupBuilder")]
pub struct PyXetFileDownloadGroupBuilder {
    pub(crate) inner: Option<XetFileDownloadGroupBuilder>,
    pub(crate) progress_callback: Option<Py<PyAny>>,
    pub(crate) progress_interval_ms: u64,
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
    /// The callable is invoked approximately every 100 ms for the lifetime of
    /// the group with two positional arguments:
    ///
    /// 1. :class:`GroupProgressReport` — aggregate bytes downloaded.
    /// 2. ``list[ItemProgressReport]`` — one entry per queued file, with ``item_name``, ``total_bytes``, and
    ///    ``bytes_completed``.
    ///
    /// The thread exits automatically once the group reaches a terminal state.
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
    /// with builder.with_progress_callback(on_progress, interval_ms=50).build() as group:
    ///     group.download_file(info, "/tmp/out.bin")
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

    /// Build the :class:`XetFileDownloadGroup`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.  If a progress
    /// callback was registered, the polling thread is started immediately so
    /// that progress is reported for the full lifetime of the group.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetFileDownloadGroup> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("builder already consumed by build()"))?;
        let group = py.detach(|| builder.build_blocking().map_err(convert_xet_error))?;

        let download_handles = if self.progress_callback.is_some() {
            let handles: Arc<Mutex<Vec<XetFileDownload>>> = Arc::new(Mutex::new(Vec::new()));
            let callback = self.progress_callback.take().unwrap().clone_ref(py);
            let inner = group.clone();
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

        Ok(PyXetFileDownloadGroup {
            inner: group,
            download_handles,
        })
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
pub struct PyXetFileDownloadGroup {
    pub(crate) inner: XetFileDownloadGroup,
    /// Per-file handles shared with the progress thread; None when no callback was registered.
    download_handles: Option<Arc<Mutex<Vec<XetFileDownload>>>>,
}

#[pymethods]
impl PyXetFileDownloadGroup {
    fn __repr__(&self) -> &'static str {
        "XetFileDownloadGroup()"
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
        if let Some(ref handles) = self.download_handles {
            handles.lock().unwrap().push(handle.clone());
        }
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn xet_info_from_download_info(info: &PyXetDownloadInfo) -> XetFileInfo {
    match info.file_size {
        Some(size) => XetFileInfo::new(info.hash.clone(), size),
        None => XetFileInfo::new_hash_only(info.hash.clone()),
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::*;

    #[test]
    fn test_builder_repr() {
        let builder = PyXetFileDownloadGroupBuilder {
            inner: None,
            progress_callback: None,
            progress_interval_ms: 100,
        };
        assert_eq!(builder.__repr__(), "XetFileDownloadGroupBuilder()");
    }

    #[test]
    fn test_builder_build_when_consumed_returns_error() {
        Python::attach(|py| {
            let mut builder = PyXetFileDownloadGroupBuilder {
                inner: None,
                progress_callback: None,
                progress_interval_ms: 100,
            };
            let err = builder.build(py).err().expect("expected error");
            assert!(err.to_string().contains("already consumed"));
        });
    }

    #[test]
    fn test_xet_info_from_download_info_with_size() {
        let info = PyXetDownloadInfo {
            destination_path: "/tmp/out.bin".to_owned(),
            hash: "abc123".to_owned(),
            file_size: Some(1024),
        };
        let xet_info = xet_info_from_download_info(&info);
        assert_eq!(xet_info.hash(), "abc123");
        assert_eq!(xet_info.file_size(), Some(1024));
    }

    #[test]
    fn test_xet_info_from_download_info_without_size() {
        let info = PyXetDownloadInfo {
            destination_path: "/tmp/out.bin".to_owned(),
            hash: "abc123".to_owned(),
            file_size: None,
        };
        let xet_info = xet_info_from_download_info(&info);
        assert_eq!(xet_info.hash(), "abc123");
        assert_eq!(xet_info.file_size(), None);
    }
}
