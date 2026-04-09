use std::collections::HashMap;
use std::ops::Range;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use xet_pkg::xet_session::{
    ItemProgressReport, XetDownloadStream, XetDownloadStreamGroup, XetDownloadStreamGroupBuilder, XetFileInfo,
    XetUnorderedDownloadStream,
};

use crate::headers::{build_header_map, build_headers_with_user_agent};
use crate::{PyXetDownloadInfo, convert_xet_error};

// ── PyXetDownloadStreamGroupBuilder ──────────────────────────────────────────

/// Fluent builder for :class:`XetDownloadStreamGroup`.
///
/// Obtain via :meth:`XetSession.new_download_stream_group`.  Chain
/// configuration methods, then call :meth:`build` to create the group.
///
/// Example (Python):
///
/// ```text
/// group = (session.new_download_stream_group()
///          .with_token_refresh_url("https://…/xet-read-token/main", {"Authorization": "Bearer hf_…"})
///          .build())
/// for chunk in group.download_stream(file_info):
///     process(chunk)
/// ```
#[pyclass(name = "XetDownloadStreamGroupBuilder")]
pub struct PyXetDownloadStreamGroupBuilder {
    pub(crate) inner: Option<XetDownloadStreamGroupBuilder>,
}

#[pymethods]
impl PyXetDownloadStreamGroupBuilder {
    fn __repr__(&self) -> &'static str {
        "XetDownloadStreamGroupBuilder()"
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

    /// Build the :class:`XetDownloadStreamGroup`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetDownloadStreamGroup> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("builder already consumed by build()"))?;
        let group = py.detach(|| builder.build_blocking().map_err(convert_xet_error))?;
        Ok(PyXetDownloadStreamGroup { inner: group })
    }
}

// ── PyXetDownloadStreamGroup ──────────────────────────────────────────────────

/// A group of streaming file downloads sharing a single CAS connection pool.
///
/// Each call to :meth:`download_stream` or :meth:`download_unordered_stream`
/// returns an independent Python iterator.  Multiple streams can be active
/// concurrently from the same group.
///
/// Cloning is cheap — all clones share the same underlying state.
#[pyclass(name = "XetDownloadStreamGroup")]
#[derive(Clone)]
pub struct PyXetDownloadStreamGroup {
    pub(crate) inner: XetDownloadStreamGroup,
}

#[pymethods]
impl PyXetDownloadStreamGroup {
    fn __repr__(&self) -> &'static str {
        "XetDownloadStreamGroup()"
    }

    // ── Stream constructors ──────────────────────────────────────────────────

    /// Open an ordered byte stream for a file.
    ///
    /// ``file_info`` — a :class:`PyXetDownloadInfo` identifying the file.
    ///
    /// ``range`` — optional ``(start, end)`` byte range (exclusive end).
    /// Omit or pass ``None`` to stream the entire file.
    ///
    /// Returns a :class:`XetDownloadStream` iterator that yields ``bytes``
    /// chunks in order.  Iterate it directly or call :meth:`cancel`.
    ///
    /// Releases the GIL during setup.
    #[pyo3(signature = (file_info, range=None))]
    pub fn download_stream(
        &self,
        py: Python<'_>,
        file_info: PyRef<'_, PyXetDownloadInfo>,
        range: Option<(u64, u64)>,
    ) -> PyResult<PyXetDownloadStream> {
        let xet_info = xet_info_from_download_info(&file_info);
        let byte_range: Option<Range<u64>> = range.map(|(s, e)| s..e);
        let inner = self.inner.clone();
        let stream = py.detach(|| inner.download_stream_blocking(xet_info, byte_range).map_err(convert_xet_error))?;
        Ok(PyXetDownloadStream { inner: stream })
    }

    /// Open an unordered byte stream for a file.
    ///
    /// Yields ``(offset, bytes)`` tuples in completion order — chunks may
    /// arrive out of order relative to their position in the file.  Use this
    /// when you want to process or write chunks as they arrive without waiting
    /// for prior chunks.
    ///
    /// ``range`` behaves the same as in :meth:`download_stream`.
    ///
    /// Releases the GIL during setup.
    #[pyo3(signature = (file_info, range=None))]
    pub fn download_unordered_stream(
        &self,
        py: Python<'_>,
        file_info: PyRef<'_, PyXetDownloadInfo>,
        range: Option<(u64, u64)>,
    ) -> PyResult<PyXetUnorderedDownloadStream> {
        let xet_info = xet_info_from_download_info(&file_info);
        let byte_range: Option<Range<u64>> = range.map(|(s, e)| s..e);
        let inner = self.inner.clone();
        let stream = py.detach(|| {
            inner
                .download_unordered_stream_blocking(xet_info, byte_range)
                .map_err(convert_xet_error)
        })?;
        Ok(PyXetUnorderedDownloadStream { inner: stream })
    }
}

// ── PyXetDownloadStream ───────────────────────────────────────────────────────

/// Ordered byte stream iterator for a single file.
///
/// Returned by :meth:`XetDownloadStreamGroup.download_stream`.
///
/// Usage:
///
/// ```text
/// for chunk in group.download_stream(file_info):
///     f.write(chunk)  # chunk is bytes, in file order
/// ```
///
/// Or with a byte range:
///
/// ```text
/// for chunk in group.download_stream(file_info, range=(0, 1024)):
///     process(chunk)
/// ```
#[pyclass(name = "XetDownloadStream")]
pub struct PyXetDownloadStream {
    inner: XetDownloadStream,
}

#[pymethods]
impl PyXetDownloadStream {
    fn __repr__(&self) -> &'static str {
        "XetDownloadStream()"
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Return the next ``bytes`` chunk, or raise ``StopIteration`` when done.
    ///
    /// Note: the GIL is held while waiting for the next chunk.
    /// ``XetDownloadStream`` is not ``Clone``, so ``py.detach()`` cannot be
    /// used here.  In practice chunks arrive quickly from the background task,
    /// so this is not expected to cause significant contention.
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        match self.inner.blocking_next().map_err(convert_xet_error)? {
            Some(bytes) => Ok(Some(PyBytes::new(py, &bytes).unbind())),
            None => Ok(None),
        }
    }

    /// Cancel this stream.  Subsequent iteration will stop immediately.
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }

    /// Current download progress for this stream.
    pub fn progress(&self) -> ItemProgressReport {
        self.inner.progress()
    }
}

// ── PyXetUnorderedDownloadStream ──────────────────────────────────────────────

/// Unordered byte stream iterator for a single file.
///
/// Returned by :meth:`XetDownloadStreamGroup.download_unordered_stream`.
///
/// Each iteration yields a ``(offset: int, data: bytes)`` tuple where
/// ``offset`` is the byte position of ``data`` within the file (or range).
/// Chunks may arrive in any order.
///
/// Usage:
///
/// ```text
/// buf = bytearray(file_size)
/// for offset, chunk in group.download_unordered_stream(file_info):
///     buf[offset:offset + len(chunk)] = chunk
/// ```
#[pyclass(name = "XetUnorderedDownloadStream")]
pub struct PyXetUnorderedDownloadStream {
    inner: XetUnorderedDownloadStream,
}

#[pymethods]
impl PyXetUnorderedDownloadStream {
    fn __repr__(&self) -> &'static str {
        "XetUnorderedDownloadStream()"
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Return the next ``(offset, bytes)`` chunk, or raise ``StopIteration``
    /// when done.
    ///
    /// Note: the GIL is held while waiting for the next chunk.
    /// ``XetUnorderedDownloadStream`` is not ``Clone``, so ``py.detach()``
    /// cannot be used here.  In practice chunks arrive quickly from the
    /// background task, so this is not expected to cause significant
    /// contention.
    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(u64, Bound<'py, PyBytes>)>> {
        match self.inner.blocking_next().map_err(convert_xet_error)? {
            Some((offset, bytes)) => Ok(Some((offset, PyBytes::new(py, &bytes)))),
            None => Ok(None),
        }
    }

    /// Cancel this stream.  Subsequent iteration will stop immediately.
    pub fn cancel(&mut self) {
        self.inner.cancel();
    }

    /// Current download progress for this stream.
    pub fn progress(&self) -> ItemProgressReport {
        self.inner.progress()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn xet_info_from_download_info(info: &PyXetDownloadInfo) -> XetFileInfo {
    match info.file_size {
        Some(size) => XetFileInfo::new(info.hash.clone(), size),
        None => XetFileInfo::new_hash_only(info.hash.clone()),
    }
}
