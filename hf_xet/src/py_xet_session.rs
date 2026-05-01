use std::collections::HashMap;

use pyo3::prelude::*;
use xet_pkg::xet_session::{XetSession, XetSessionBuilder};
use xet_runtime::config::XetConfig;

use crate::py_download_stream_group::{PyXetDownloadStreamGroup, build_download_stream_group};
use crate::py_file_download_group::{PyXetFileDownloadGroup, build_file_download_group};
use crate::py_upload_commit::{PyXetUploadCommit, build_upload_commit};
use crate::utils::{task_state_display, task_state_to_pystate};
use crate::{PyXetTaskState, convert_xet_error};

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
            .map(|(k, v): (String, Py<PyAny>)| {
                let repr = v.bind(py).repr().map(|r| r.to_string()).unwrap_or_else(|_| "?".to_string());
                format!("{k}={repr}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("XetSession(id=\"{id}\", status=\"{status}\", config={{{config_str}}})"))
    }

    /// Create a new XetSession.
    ///
    /// ``config`` is an optional :class:`XetConfig` instance.  When omitted, a
    /// default config (with environment-variable overrides applied) is used.
    #[new]
    #[pyo3(signature = (config=None))]
    pub fn new(config: Option<crate::config::PyXetConfig>) -> PyResult<Self> {
        #[allow(clippy::unwrap_or_default)]
        // XetConfig::new starts from default() and applies HF_XET_* environment variable overrides
        let xet_config = config.map(|c| c.into_inner()).unwrap_or_else(XetConfig::new);
        let session = XetSessionBuilder::new_with_config(xet_config).build().map_err(PyErr::from)?;
        Ok(Self { inner: session })
    }

    /// Create a new :class:`XetUploadCommit` and establish the CAS connection.
    ///
    /// All parameters are optional.  Releases the GIL during the blocking
    /// network handshake.
    ///
    /// ``endpoint`` — Xet CAS server URL (e.g. ``"https://cas.xethub.hf.co"``).  If
    /// omitted but ``token_refresh_url`` is provided, the endpoint is fetched
    /// automatically from the token refresh response.
    ///
    /// ``token`` and ``token_expiry_unix_secs`` — seed an initial CAS access token and
    /// its expiry as a Unix timestamp (seconds).  Both must be supplied together; if
    /// either is absent the token is not pre-seeded.  When ``token_refresh_url`` is also
    /// provided, the refresh response's token is used only if no token was pre-seeded here.
    ///
    /// ``token_refresh_url`` — URL called with an HTTP GET whenever the current CAS token
    /// is about to expire.  The response must be JSON:
    /// ``{"accessToken": "…", "exp": <unix_secs>, "casUrl": "…"}``.
    ///
    /// ``token_refresh_headers`` — HTTP headers sent with every token refresh request
    /// (e.g. ``{"Authorization": "Bearer hf_…"}``).  Defaults to ``{}`` when
    /// ``token_refresh_url`` is set but headers are omitted.
    ///
    /// ``custom_headers`` — additional HTTP headers forwarded with every CAS request.
    ///
    /// ``progress_callback`` — callable invoked every ``progress_interval_ms``
    /// milliseconds with ``(GroupProgressReport, dict[UniqueID, ItemProgressReport])``.
    ///
    /// ``progress_interval_ms`` — milliseconds between progress callbacks (default ``100``).
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
    ///     commit.start_upload_file("/path/to/model.bin")
    /// ```
    #[allow(clippy::too_many_arguments)]
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
    /// network handshake.
    ///
    /// ``endpoint`` — Xet CAS server URL (e.g. ``"https://cas.xethub.hf.co"``).  If
    /// omitted but ``token_refresh_url`` is provided, the endpoint is fetched
    /// automatically from the token refresh response.
    ///
    /// ``token`` and ``token_expiry_unix_secs`` — seed an initial CAS access token and
    /// its expiry as a Unix timestamp (seconds).  Both must be supplied together; if
    /// either is absent the token is not pre-seeded.  When ``token_refresh_url`` is also
    /// provided, the refresh response's token is used only if no token was pre-seeded here.
    ///
    /// ``token_refresh_url`` — URL called with an HTTP GET whenever the current CAS token
    /// is about to expire.  The response must be JSON:
    /// ``{"accessToken": "…", "exp": <unix_secs>, "casUrl": "…"}``.
    ///
    /// ``token_refresh_headers`` — HTTP headers sent with every token refresh request
    /// (e.g. ``{"Authorization": "Bearer hf_…"}``).  Defaults to ``{}`` when
    /// ``token_refresh_url`` is set but headers are omitted.
    ///
    /// ``custom_headers`` — additional HTTP headers forwarded with every CAS request.
    ///
    /// ``progress_callback`` — callable invoked every ``progress_interval_ms``
    /// milliseconds with ``(GroupProgressReport, dict[UniqueID, ItemProgressReport])``.
    ///
    /// ``progress_interval_ms`` — milliseconds between progress callbacks (default ``100``).
    ///
    /// Example:
    ///
    /// ```text
    /// with session.new_file_download_group(
    ///         endpoint="https://cas.xethub.hf.co",
    ///         token_refresh_url="https://…/xet-read-token/main",
    ///         token_refresh_headers={"Authorization": "Bearer hf_…"},
    ///     ) as group:
    ///     group.start_download_file(file_info, "/tmp/out.bin")
    /// ```
    #[allow(clippy::too_many_arguments)]
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
    /// network handshake.
    ///
    /// ``endpoint`` — Xet CAS server URL (e.g. ``"https://cas.xethub.hf.co"``).  If
    /// omitted but ``token_refresh_url`` is provided, the endpoint is fetched
    /// automatically from the token refresh response.
    ///
    /// ``token`` and ``token_expiry_unix_secs`` — seed an initial CAS access token and
    /// its expiry as a Unix timestamp (seconds).  Both must be supplied together; if
    /// either is absent the token is not pre-seeded.  When ``token_refresh_url`` is also
    /// provided, the refresh response's token is used only if no token was pre-seeded here.
    ///
    /// ``token_refresh_url`` — URL called with an HTTP GET whenever the current CAS token
    /// is about to expire.  The response must be JSON:
    /// ``{"accessToken": "…", "exp": <unix_secs>, "casUrl": "…"}``.
    ///
    /// ``token_refresh_headers`` — HTTP headers sent with every token refresh request
    /// (e.g. ``{"Authorization": "Bearer hf_…"}``).  Defaults to ``{}`` when
    /// ``token_refresh_url`` is set but headers are omitted.
    ///
    /// ``custom_headers`` — additional HTTP headers forwarded with every CAS request.
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
    #[allow(clippy::too_many_arguments)]
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

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
    }

    /// Cancel all in-progress operations and shut down the underlying runtime.
    ///
    /// Unlike :meth:`XetUploadCommit.abort` or :meth:`XetFileDownloadGroup.abort`,
    /// which cancel a single operation while leaving the session usable, this
    /// method destroys the session's runtime entirely.  The :class:`XetSession`
    /// object must be discarded and a new one created before issuing further
    /// uploads or downloads.
    ///
    /// Intended for use in ``except KeyboardInterrupt:`` handlers.
    pub fn sigint_abort(&self) -> PyResult<()> {
        self.inner.sigint_abort().map_err(convert_xet_error)
    }
}
