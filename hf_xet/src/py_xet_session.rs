use std::collections::HashMap;

use pyo3::prelude::*;
use xet_pkg::xet_session::{XetSession, XetSessionBuilder};

use crate::convert_xet_error;
use crate::py_download_stream_group::{build_download_stream_group, PyXetDownloadStreamGroup};
use crate::py_file_download_group::{build_file_download_group, PyXetFileDownloadGroup};
use crate::py_upload_commit::{build_upload_commit, PyXetUploadCommit};
use crate::utils::{task_state_display, task_state_to_str};

// ── PyXetSession ─────────────────────────────────────────────────────────────

/// Manages a Xet runtime context and connection pool.
///
/// Session objects are cheap to clone — all clones share the same underlying state.
#[pyclass(name = "XetSession")]
#[derive(Clone)]
pub struct PyXetSession {
    pub(crate) inner: XetSession,
}

#[pymethods]
impl PyXetSession {
    // Example output:
    //   XetSession(id="01JBQW...", status="Running", config={data.max_concurrent_file_ingestion=4, ...})
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let status = task_state_display(self.inner.status());
        let id = self.inner.id();
        let items = self.inner.config().all_items_to_python(py)?;
        let config_str = items
            .into_iter()
            .map(|(k, v)| {
                let repr = v.bind(py).repr().map(|r| r.to_string()).unwrap_or_else(|_| "?".to_string());
                format!("{k}={repr}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("XetSession(id=\"{id}\", status=\"{status}\", config={{{config_str}}})"))
    }

    /// Create a new XetSession.
    #[new]
    pub fn new() -> PyResult<Self> {
        let session = XetSessionBuilder::new().build().map_err(PyErr::from)?;
        Ok(Self { inner: session })
    }

    /// Create a new :class:`XetUploadCommit` and establish the CAS connection.
    ///
    /// All parameters are optional.  Releases the GIL during the blocking
    /// network handshake.
    ///
    /// ``token`` and ``token_expiry_unix_secs`` must be provided together; if
    /// either is absent the token is not seeded.
    ///
    /// ``token_refresh_headers`` defaults to ``{}`` when ``token_refresh_url``
    /// is provided but headers are omitted.
    ///
    /// Example:
    ///
    /// ```text
    /// with session.new_upload_commit(
    ///         endpoint="https://cas.xethub.hf.co",
    ///         token="jwt", token_expiry_unix_secs=9999999999,
    ///         token_refresh_url="https://…/xet-write-token/main",
    ///         token_refresh_headers={"Authorization": "Bearer hf_…"},
    ///         progress_callback=on_progress,
    ///     ) as commit:
    ///     commit.upload_file("/path/to/model.bin")
    /// ```
    #[pyo3(signature = (
        endpoint=None, token=None, token_expiry_unix_secs=None,
        token_refresh_url=None, token_refresh_headers=None,
        custom_headers=None, progress_callback=None, progress_interval_ms=100
    ))]
    pub fn new_upload_commit(
        &self,
        py: Python<'_>,
        endpoint: Option<String>,
        token: Option<String>,
        token_expiry_unix_secs: Option<u64>,
        token_refresh_url: Option<String>,
        token_refresh_headers: Option<HashMap<String, String>>,
        custom_headers: Option<HashMap<String, String>>,
        progress_callback: Option<Py<PyAny>>,
        progress_interval_ms: u64,
    ) -> PyResult<PyXetUploadCommit> {
        build_upload_commit(
            py,
            &self.inner,
            endpoint,
            token,
            token_expiry_unix_secs,
            token_refresh_url,
            token_refresh_headers,
            custom_headers,
            progress_callback,
            progress_interval_ms,
        )
    }

    /// Create a new :class:`XetFileDownloadGroup` and establish the CAS connection.
    ///
    /// All parameters are optional.  Releases the GIL during the blocking
    /// network handshake.  See :meth:`new_upload_commit` for parameter details.
    ///
    /// Example:
    ///
    /// ```text
    /// with session.new_file_download_group(
    ///         endpoint="https://cas.xethub.hf.co",
    ///         token_refresh_url="https://…/xet-read-token/main",
    ///         token_refresh_headers={"Authorization": "Bearer hf_…"},
    ///     ) as group:
    ///     group.download_file(file_info, "/tmp/out.bin")
    /// ```
    #[pyo3(signature = (
        endpoint=None, token=None, token_expiry_unix_secs=None,
        token_refresh_url=None, token_refresh_headers=None,
        custom_headers=None, progress_callback=None, progress_interval_ms=100
    ))]
    pub fn new_file_download_group(
        &self,
        py: Python<'_>,
        endpoint: Option<String>,
        token: Option<String>,
        token_expiry_unix_secs: Option<u64>,
        token_refresh_url: Option<String>,
        token_refresh_headers: Option<HashMap<String, String>>,
        custom_headers: Option<HashMap<String, String>>,
        progress_callback: Option<Py<PyAny>>,
        progress_interval_ms: u64,
    ) -> PyResult<PyXetFileDownloadGroup> {
        build_file_download_group(
            py,
            &self.inner,
            endpoint,
            token,
            token_expiry_unix_secs,
            token_refresh_url,
            token_refresh_headers,
            custom_headers,
            progress_callback,
            progress_interval_ms,
        )
    }

    /// Create a new :class:`XetDownloadStreamGroup` and establish the CAS connection.
    ///
    /// All parameters are optional.  Releases the GIL during the blocking
    /// network handshake.  See :meth:`new_upload_commit` for parameter details.
    ///
    /// Example:
    ///
    /// ```text
    /// group = session.new_download_stream_group(
    ///     endpoint="https://cas.xethub.hf.co",
    ///     token_refresh_url="https://…/xet-read-token/main",
    ///     token_refresh_headers={"Authorization": "Bearer hf_…"},
    /// )
    /// for chunk in group.download_stream(file_info):
    ///     process(chunk)
    /// ```
    #[pyo3(signature = (
        endpoint=None, token=None, token_expiry_unix_secs=None,
        token_refresh_url=None, token_refresh_headers=None,
        custom_headers=None
    ))]
    pub fn new_download_stream_group(
        &self,
        py: Python<'_>,
        endpoint: Option<String>,
        token: Option<String>,
        token_expiry_unix_secs: Option<u64>,
        token_refresh_url: Option<String>,
        token_refresh_headers: Option<HashMap<String, String>>,
        custom_headers: Option<HashMap<String, String>>,
    ) -> PyResult<PyXetDownloadStreamGroup> {
        build_download_stream_group(
            py,
            &self.inner,
            endpoint,
            token,
            token_expiry_unix_secs,
            token_refresh_url,
            token_refresh_headers,
            custom_headers,
        )
    }

    /// Current task state: ``"Running"``, ``"Finalizing"``, ``"Completed"``, or
    /// ``"UserCancelled"``.  Raises on error state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// SIGINT-style abort: shuts down the runtime and cancels all tasks.
    pub fn sigint_abort(&self) -> PyResult<()> {
        self.inner.sigint_abort().map_err(convert_xet_error)
    }
}

