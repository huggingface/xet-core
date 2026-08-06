use std::collections::HashMap;
use std::ops::Range;

use pyo3::prelude::*;
use xet_pkg::xet_session::{XetDownloadStreamGroup, XetFileInfo, XetSession};

use crate::convert_xet_error;
use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::py_download_stream_handle::{PyXetDownloadStream, PyXetUnorderedDownloadStream};

// ── build_download_stream_group ───────────────────────────────────────────────

/// Create an :class:`XetDownloadStreamGroup` from a session and optional configuration.
///
/// Called by :meth:`XetSession.new_download_stream_group`.  The Rust builder type is
/// created and consumed entirely here — it never surfaces in any public API.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_download_stream_group(
    py: Python<'_>,
    session: &XetSession,
    endpoint: Option<String>,
    token: Option<String>,
    token_expiry_unix_secs: Option<u64>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    custom_headers: Option<HashMap<String, String>>,
) -> PyResult<PyXetDownloadStreamGroup> {
    let mut builder = session.new_download_stream_group().map_err(convert_xet_error)?;
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
    Ok(PyXetDownloadStreamGroup { inner: group })
}

// ── PyXetDownloadStreamGroup ──────────────────────────────────────────────────

/// A group of streaming file downloads sharing a single CAS connection pool.
///
/// Each call to :meth:`download_stream` or :meth:`download_unordered_stream`
/// returns an independent Python iterator.  Multiple streams can be active
/// concurrently from the same group.
///
/// Cloning is cheap — all clones share the same underlying state.
#[pyclass(name = "XetDownloadStreamGroup", from_py_object)]
#[derive(Clone)]
pub struct PyXetDownloadStreamGroup {
    pub(crate) inner: XetDownloadStreamGroup,
}

#[pymethods]
impl PyXetDownloadStreamGroup {
    fn __repr__(&self) -> &'static str {
        "XetDownloadStreamGroup()"
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
            // Normal exit: the caller is done, so report a clean finish.
            self.finish(py)?;
        } else {
            // Exception: cancel the streams and leave the session unfinalized, so its `Drop`
            // derives the outcome from what actually transferred. Calling `finish` here would
            // report `ok` and record a failed transfer as a successful one.
            if let Err(e) = self.abort(py) {
                tracing::warn!("abort() failed during __exit__ exception path: {e}");
            }
        }
        Ok(false) // do not suppress the exception
    }

    /// Mark the group as finished, reporting transfer telemetry.
    ///
    /// Streams are consumed independently, so the group cannot tell on its own when the caller is
    /// done; this says so.
    ///
    /// **Optional.** A group that is never finished behaves exactly as before and still reports
    /// when it is collected — as a dropped transfer rather than a clean one. Existing code needs
    /// no change.
    ///
    /// Open every stream you intend to open first: the group is **closed** afterwards, so
    /// :meth:`download_stream` and :meth:`download_unordered_stream` raise once it has been
    /// called. Streams already returned stay usable.
    ///
    /// Called automatically when exiting a ``with`` block. Calling it twice is a no-op.
    pub fn finish(&self, py: Python<'_>) -> PyResult<()> {
        let group = self.inner.clone();
        py.detach(|| group.finish_blocking().map_err(convert_xet_error))
    }

    /// Cancel every active stream in this group, abandoning the transfer.
    ///
    /// The counterpart to :meth:`finish` for a caller giving up rather than completing. Called
    /// automatically when a ``with`` block exits on an exception.
    ///
    /// Unlike :meth:`finish`, this reports no outcome of its own: the transfer is recorded from
    /// what actually transferred once the group is collected, so an abandoned partial download is
    /// not counted as a success.
    pub fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let group = self.inner.clone();
        py.detach(|| group.abort().map_err(convert_xet_error))
    }

    // ── Stream constructors ──────────────────────────────────────────────────

    /// Open an ordered byte stream for a file.
    ///
    /// ``file_info`` — a :class:`XetFileInfo` identifying the file.
    ///
    /// ``start`` / ``end`` — optional byte offsets (exclusive end).  Both
    /// default to ``None``, meaning the full file.  Either may be omitted
    /// independently:
    ///
    /// ```python
    /// group.download_stream(info)                    # whole file
    /// group.download_stream(info, start=3)           # 3 .. EOF
    /// group.download_stream(info, end=100)           # 0 .. 100
    /// group.download_stream(info, start=3, end=100)  # 3 .. 100
    /// ```
    ///
    /// Returns a :class:`XetDownloadStream` iterator that yields ``bytes``
    /// chunks in order.  Iterate it directly or call :meth:`cancel`.
    ///
    /// Releases the GIL during setup.
    #[pyo3(signature = (file_info, start=None, end=None))]
    pub fn download_stream(
        &self,
        py: Python<'_>,
        file_info: XetFileInfo,
        start: Option<u64>,
        end: Option<u64>,
    ) -> PyResult<PyXetDownloadStream> {
        let byte_range: Option<Range<u64>> = match (start, end) {
            (None, None) => None,
            (s, e) => Some(s.unwrap_or(0)..e.unwrap_or(u64::MAX)),
        };
        let inner = self.inner.clone();
        let stream = py.detach(|| inner.download_stream_blocking(file_info, byte_range).map_err(convert_xet_error))?;
        Ok(PyXetDownloadStream { inner: stream })
    }

    /// Open an unordered byte stream for a file.
    ///
    /// Yields ``(offset, bytes)`` tuples in completion order — chunks may
    /// arrive out of order relative to their position in the file.  Use this
    /// when you want to process or write chunks as they arrive without waiting
    /// for prior chunks.
    ///
    /// ``start`` / ``end`` behave the same as in :meth:`download_stream`.
    ///
    /// Releases the GIL during setup.
    #[pyo3(signature = (file_info, start=None, end=None))]
    pub fn download_unordered_stream(
        &self,
        py: Python<'_>,
        file_info: XetFileInfo,
        start: Option<u64>,
        end: Option<u64>,
    ) -> PyResult<PyXetUnorderedDownloadStream> {
        let byte_range: Option<Range<u64>> = match (start, end) {
            (None, None) => None,
            (s, e) => Some(s.unwrap_or(0)..e.unwrap_or(u64::MAX)),
        };
        let inner = self.inner.clone();
        let stream = py.detach(|| {
            inner
                .download_unordered_stream_blocking(file_info, byte_range)
                .map_err(convert_xet_error)
        })?;
        Ok(PyXetUnorderedDownloadStream { inner: stream })
    }
}
