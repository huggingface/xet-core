use std::collections::HashMap;
use std::ops::Range;

use http::HeaderMap;
use pyo3::prelude::*;
use xet_pkg::xet_session::{XetDownloadStreamGroup, XetDownloadStreamGroupBuilder, XetFileInfo};

use crate::convert_xet_error;
use crate::headers::{build_header_map, build_headers_with_user_agent, default_headers};
use crate::py_download_stream_handle::{PyXetDownloadStream, PyXetUnorderedDownloadStream};

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
///
/// # stream the whole file
/// for chunk in group.download_stream(file_info):
///     process(chunk)
///
/// # stream a byte range (start and end are both optional)
/// for chunk in group.download_stream(file_info, start=1024, end=2048):
///     process(chunk)
/// ```
#[pyclass(name = "XetDownloadStreamGroupBuilder")]
pub struct PyXetDownloadStreamGroupBuilder {
    pub(crate) inner: Option<XetDownloadStreamGroupBuilder>,
    pub(crate) custom_headers: Option<HeaderMap>,
}

#[pymethods]
impl PyXetDownloadStreamGroupBuilder {
    fn __repr__(&self) -> &'static str {
        "XetDownloadStreamGroupBuilder()"
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
    ///
    /// The endpoint must return JSON:
    /// ``{"accessToken": "…", "exp": <unix_secs>, "casUrl": "…"}``
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

    /// Build the :class:`XetDownloadStreamGroup`, establishing the CAS connection.
    ///
    /// Releases the GIL during the blocking network handshake.
    pub fn build(&mut self, py: Python<'_>) -> PyResult<PyXetDownloadStreamGroup> {
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

// ── Helpers ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::XetSessionBuilder;

    use super::*;

    // ── PyXetDownloadStreamGroupBuilder ───────────────────────────────────────

    #[test]
    fn test_builder_build_when_consumed_returns_error() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let mut builder = PyXetDownloadStreamGroupBuilder {
            inner: Some(session.new_download_stream_group().unwrap().with_endpoint(&endpoint)),
            custom_headers: None,
        };

        Python::attach(|py| {
            builder.build(py).expect("first build should succeed");
            let err = builder.build(py).err().expect("expected error").to_string();
            assert!(err.contains("already consumed"));
        });
    }
}
