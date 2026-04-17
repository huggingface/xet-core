use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use http::HeaderMap;
use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, UniqueID, XetDownloadGroupReport, XetFileDownload, XetFileDownloadGroup,
    XetFileDownloadGroupBuilder, XetFileInfo, XetTaskState,
};

use crate::headers::{build_header_map, build_headers_with_user_agent, default_headers};
use crate::py_file_download_handle::PyXetFileDownload;
use crate::py_xet_session::task_state_to_str;
use crate::{blocking_call_with_signal_check, convert_xet_error};

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
/// # look up a specific file's result via its task id:
/// result = report.downloads[h1.task_id()]
/// print(result.hash, result.path, result.file_size)
///
/// # or get a specific file's result via its handle:
/// result = h1.result()
/// print(result.hash, result.path, result.file_size)
/// ```
#[pyclass(name = "XetFileDownloadGroupBuilder")]
pub struct PyXetFileDownloadGroupBuilder {
    pub(crate) inner: Option<XetFileDownloadGroupBuilder>,
    pub(crate) progress_callback: Option<Py<PyAny>>,
    pub(crate) progress_interval_ms: u64,
    pub(crate) custom_headers: Option<HeaderMap>,
}

#[pymethods]
impl PyXetFileDownloadGroupBuilder {
    fn __repr__(&self) -> &'static str {
        "XetFileDownloadGroupBuilder()"
    }

    /// Set the CAS server endpoint URL.
    pub fn with_endpoint<'py>(mut slf: PyRefMut<'py, Self>, endpoint: String) -> PyRefMut<'py, Self> {
        slf.inner = slf.inner.take().map(|b| b.with_endpoint(endpoint));
        slf
    }

    /// Seed an initial CAS access token and its Unix expiry timestamp.
    pub fn with_token_info<'py>(
        mut slf: PyRefMut<'py, Self>,
        token: String,
        expiry_unix_secs: u64,
    ) -> PyRefMut<'py, Self> {
        slf.inner = slf.inner.take().map(|b| b.with_token_info(token, expiry_unix_secs));
        slf
    }

    /// Set a URL for automatic token refresh.
    pub fn with_token_refresh_url<'py>(
        mut slf: PyRefMut<'py, Self>,
        url: String,
        headers: HashMap<String, String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let header_map = build_header_map(headers)?;
        slf.inner = slf.inner.take().map(|b| b.with_token_refresh_url(url, header_map));
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
        slf.custom_headers = Some(build_headers_with_user_agent(Some(headers))?);
        Ok(slf)
    }

    /// Register a Python callable to receive periodic progress updates.
    ///
    /// The callable is invoked on a configurable interval for the lifetime of
    /// the group with two positional arguments:
    ///
    /// 1. :class:`GroupProgressReport` — aggregate bytes downloaded.
    /// 2. ``dict[UniqueId, ItemProgressReport]`` — one entry per queued file, keyed by task ID.
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
    ///     bar.set_postfix({item.item_name: f"{item.bytes_completed}/{item.total_bytes}"
    ///                      for item in items.values()})
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
        let custom_headers = self.custom_headers.take().unwrap_or_else(default_headers);
        let group = py.detach(|| {
            builder
                .with_custom_headers(custom_headers)
                .build_blocking()
                .map_err(convert_xet_error)
        })?;

        let download_handles = if let Some(callback) = self.progress_callback.take() {
            let handles: Arc<RwLock<Vec<XetFileDownload>>> = Arc::new(RwLock::new(Vec::new()));
            let inner = group.clone();
            let handles_for_thread = Arc::clone(&handles);
            let interval = Duration::from_millis(self.progress_interval_ms);
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let group_report = inner.progress();
                    let item_reports: HashMap<UniqueID, ItemProgressReport> = handles_for_thread
                        .read()
                        .map(|g| g.iter().filter_map(|h| h.progress().map(|p| (h.task_id(), p))).collect())
                        .unwrap_or_default();
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
    download_handles: Option<Arc<RwLock<Vec<XetFileDownload>>>>,
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
            // Normal exit: wait for all downloads (signal-interruptible).
            self.finish(py)?;
        } else {
            let _ = self.inner.abort();
        }
        Ok(false)
    }

    // ── Download methods ─────────────────────────────────────────────────────

    /// Queue a file for download.
    ///
    /// ``file_info`` — a :class:`XetFileInfo` identifying the file (hash and size).
    ///
    /// ``dest_path`` — local filesystem path to write the file to.
    ///
    /// Returns immediately with a :class:`XetFileDownload` handle.  Call
    /// :meth:`finish` (or exit the ``with`` block) to wait for completion.
    pub fn download_file(
        &self,
        py: Python<'_>,
        file_info: XetFileInfo,
        dest_path: String,
    ) -> PyResult<PyXetFileDownload> {
        let path: std::path::PathBuf = dest_path.into();
        let inner = self.inner.clone();
        let handle = py.detach(|| inner.download_file_to_path_blocking(file_info, path).map_err(convert_xet_error))?;
        if let Some(ref handles) = self.download_handles {
            handles
                .write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .push(handle.clone());
        }
        Ok(PyXetFileDownload { inner: handle })
    }

    // ── Finish / abort ───────────────────────────────────────────────────────

    /// Wait for all downloads to complete and return a summary report.
    ///
    /// Returns a :class:`XetDownloadGroupReport`.  Also called automatically
    /// when exiting a ``with`` block without an exception.
    ///
    /// Releases the GIL while waiting, polling for ``KeyboardInterrupt`` every
    /// 100 ms so that Ctrl-C is delivered promptly.
    pub fn finish(&self, py: Python<'_>) -> PyResult<XetDownloadGroupReport> {
        let group = self.inner.clone();
        blocking_call_with_signal_check(py, move || group.finish_blocking())
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

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::XetSessionBuilder;

    use super::*;

    // ── PyXetFileDownloadGroup ────────────────────────────────────────────────

    #[test]
    fn test_finish_empty_group() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let group = PyXetFileDownloadGroup {
            inner: session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap(),
            download_handles: None,
        };

        Python::attach(|py| {
            let report = group.finish(py).unwrap();
            assert!(report.downloads.is_empty());
        });
    }

    #[test]
    fn test_abort_makes_finish_fail() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let group = PyXetFileDownloadGroup {
            inner: session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap(),
            download_handles: None,
        };

        Python::attach(|py| {
            group.abort().unwrap();
            assert!(group.finish(py).is_err());
        });
    }

    // ── PyXetFileDownloadGroupBuilder ─────────────────────────────────────────

    #[test]
    fn test_builder_build_when_consumed_returns_error() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let mut builder = PyXetFileDownloadGroupBuilder {
            inner: Some(session.new_file_download_group().unwrap().with_endpoint(&endpoint)),
            progress_callback: None,
            progress_interval_ms: 100,
            custom_headers: None,
        };

        Python::attach(|py| {
            builder.build(py).expect("first build should succeed");
            let err = builder.build(py).err().expect("expected error").to_string();
            assert!(err.contains("already consumed"));
        });
    }
}
