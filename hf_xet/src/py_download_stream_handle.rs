use pyo3::prelude::*;
use pyo3::types::PyBytes;
use xet_pkg::xet_session::{ItemProgressReport, XetDownloadStream, XetUnorderedDownloadStream};

use crate::convert_xet_error;
use crate::utils::progress_display;

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
/// for chunk in group.download_stream(file_info, start=0, end=1024):
///     process(chunk)
/// ```
#[pyclass(name = "XetDownloadStream")]
pub struct PyXetDownloadStream {
    pub(crate) inner: XetDownloadStream,
}

#[pymethods]
impl PyXetDownloadStream {
    // Example output:
    //   XetDownloadStream(task_id=4, bytes_completed=512/2048)
    //   XetDownloadStream(task_id=5, bytes_completed=?/?)   ← before first progress report
    fn __repr__(&self) -> String {
        let prog = progress_display(self.inner.progress());
        format!("XetDownloadStream(task_id={}, bytes_completed={})", self.inner.task_id(), prog)
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

    /// Current download progress for this stream, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
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
    pub(crate) inner: XetUnorderedDownloadStream,
}

#[pymethods]
impl PyXetUnorderedDownloadStream {
    // Example output:
    //   XetUnorderedDownloadStream(task_id=6, bytes_completed=4096/16384)
    //   XetUnorderedDownloadStream(task_id=7, bytes_completed=?/?)   ← before first progress report
    fn __repr__(&self) -> String {
        let prog = progress_display(self.inner.progress());
        format!("XetUnorderedDownloadStream(task_id={}, bytes_completed={})", self.inner.task_id(), prog)
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

    /// Current download progress for this stream, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }
}
