use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, UniqueID, XetDownloadGroupReport, XetFileDownload, XetFileDownloadGroup,
    XetFileInfo, XetSession, XetTaskState,
};

use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_file_download_handle::PyXetFileDownload;
use crate::utils::{progress_display, task_state_display, task_state_to_pystate};
use crate::{PyXetTaskState, blocking_call_with_signal_check, convert_xet_error};

// ── build_file_download_group ─────────────────────────────────────────────────

/// Create an :class:`XetFileDownloadGroup` from a session and optional configuration.
///
/// Called by :meth:`XetSession.new_file_download_group`.  The Rust builder type is
/// created and consumed entirely here — it never surfaces in any public API.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_file_download_group(
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
) -> PyResult<PyXetFileDownloadGroup> {
    let mut builder = session.new_file_download_group().map_err(convert_xet_error)?;
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
    let group = py.detach(move || {
        builder
            .with_custom_headers(merged_headers)
            .build_blocking()
            .map_err(convert_xet_error)
    })?;

    let download_handles = if let Some(callback) = progress_callback {
        let handles: Arc<RwLock<Vec<XetFileDownload>>> = Arc::new(RwLock::new(Vec::new()));
        let inner = group.clone();
        let handles_for_thread = Arc::clone(&handles);
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

// ── PyXetFileDownloadGroup ────────────────────────────────────────────────────

/// A group of related file downloads.
///
/// Implements the context-manager protocol.
///
/// ```text
/// with session.new_file_download_group(endpoint="...") as group:
///     h = group.start_download_file(info, "/tmp/out.bin")
/// # on normal exit: wait_to_finish() is called automatically
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
    // Example output:
    //   XetFileDownloadGroup(status="Running", downloads=[(3, "/tmp/model.bin", bytes_completed=1024/4096), (4,
    // "/tmp/data.bin", bytes_completed=?/?)])
    //
    // Each download entry is (task_id, dest_path, bytes_completed/total_bytes).
    // Progress shows "?/?" before the first report arrives.
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        let downloads: Vec<String> = self
            .inner
            .active_download_info()
            .into_iter()
            .map(|(id, path, progress)| {
                let prog = progress_display(progress);
                format!("({id}, \"{}\", bytes_completed={prog})", path.display())
            })
            .collect();
        format!("XetFileDownloadGroup(status=\"{}\", downloads=[{}])", status, downloads.join(", "))
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
            self.wait_to_finish(py)?;
        } else {
            if let Err(e) = self.inner.abort() {
                tracing::warn!("abort() failed during __exit__ exception path: {e}");
            }
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
    pub fn start_download_file(
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
    pub fn wait_to_finish(&self, py: Python<'_>) -> PyResult<XetDownloadGroupReport> {
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

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
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
            let report = group.wait_to_finish(py).unwrap();
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
            assert!(group.wait_to_finish(py).is_err());
        });
    }
}
