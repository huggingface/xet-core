use pyo3::prelude::*;
use xet_pkg::xet_session::{ItemProgressReport, UniqueId, XetFileMetadata, XetStreamUpload};

use crate::utils::{progress_display, task_state_display, task_state_to_pystate};
use crate::{PyXetTaskState, convert_xet_error};

// ── PyXetStreamUpload ─────────────────────────────────────────────────────────

/// Handle for a streaming upload within an :class:`XetUploadCommit`.
///
/// Returned by :meth:`XetUploadCommit.start_upload_stream`.  Feed data incrementally
/// with :meth:`write`, then finalise with either:
///
/// - :meth:`finish` (explicit call), or
/// - the context manager, which calls :meth:`finish` automatically on normal exit and :meth:`abort` on exception.
///
/// **:meth:`finish` (or the context manager) must complete before**
/// :meth:`XetUploadCommit.wait_to_finish` is called.
///
/// ```python
/// with commit.start_upload_stream(name="big.bin") as stream:
///     for chunk in produce_chunks():
///         stream.write(chunk)
/// # finish() was called automatically
/// ```
#[pyclass(name = "XetStreamUpload")]
#[derive(Clone)]
pub struct PyXetStreamUpload {
    pub(crate) inner: XetStreamUpload,
}

#[pymethods]
impl PyXetStreamUpload {
    // Example output:
    //   XetStreamUpload(task_id=1, status="Running", bytes_completed=512/4096)
    //   XetStreamUpload(task_id=2, status="Completed", bytes_completed=4096/4096)
    //   XetStreamUpload(task_id=3, status="Running", bytes_completed=?/?)   ← before first progress report
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        let prog = progress_display(self.inner.progress());
        format!("XetStreamUpload(task_id={}, status=\"{}\", bytes_completed={})", self.inner.task_id(), status, prog)
    }

    // ── Context manager ──────────────────────────────────────────────────────

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Calls :meth:`finish` on normal exit (idempotent if already finished) or :meth:`abort` on
    /// exception; never suppresses the exception.
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Bound<'_, pyo3::PyAny>,
        _exc_val: Bound<'_, pyo3::PyAny>,
        _exc_tb: Bound<'_, pyo3::PyAny>,
    ) -> PyResult<bool> {
        if exc_type.is_none() {
            // Normal exit: finalise the stream only if not already done (idempotent — avoids a
            // double-finish error when the caller explicitly called finish() inside the with-block).
            if self.inner.try_finish().is_none() {
                self.finish(py)?;
            }
        } else {
            // Exception path: cancel the upload (infallible).
            self.inner.abort();
        }
        Ok(false) // do not suppress the exception
    }

    // ── Upload methods ───────────────────────────────────────────────────────

    /// Feed a chunk of data into the upload pipeline.
    ///
    /// May be called any number of times before :meth:`finish`.
    /// Releases the GIL while writing.
    pub fn write(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        // Copy bytes into an owned Vec before releasing the GIL so the
        // Python bytes object can be freed independently.
        let owned: Vec<u8> = data.to_vec();
        let inner = self.inner.clone();
        py.detach(|| inner.write_blocking(owned).map_err(convert_xet_error))
    }

    /// Finalise the stream and return per-file upload metadata.
    ///
    /// Must be called before :meth:`XetUploadCommit.wait_to_finish`.
    /// Releases the GIL while waiting.
    pub fn finish(&self, py: Python<'_>) -> PyResult<XetFileMetadata> {
        let inner = self.inner.clone();
        py.detach(|| inner.finish_blocking().map_err(convert_xet_error))
    }

    /// Return upload metadata without blocking, or ``None`` if not yet finished.
    pub fn try_finish(&self) -> Option<XetFileMetadata> {
        self.inner.try_finish()
    }

    /// Per-file progress snapshot, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
    }

    /// The unique task ID for this stream.
    pub fn task_id(&self) -> UniqueId {
        self.inner.task_id()
    }

    /// Cancel the streaming upload.
    pub fn abort(&self) {
        self.inner.abort();
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::{Sha256Policy, XetSessionBuilder};

    use super::*;

    #[test]
    fn test_stream_upload_write_and_finish() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let stream_handle = commit
            .upload_stream_blocking(Some("stream.bin".into()), Sha256Policy::Compute)
            .unwrap();
        let py_stream = PyXetStreamUpload { inner: stream_handle };
        assert!(py_stream.task_id().0 > 0);
        Python::attach(|py| {
            py_stream.write(py, b"hello world").unwrap();
            let result = py_stream.finish(py).unwrap();
            assert_eq!(result.xet_info.file_size, Some(11));
            assert!(!result.xet_info.hash.is_empty());
        });
    }

    #[test]
    fn test_stream_upload_try_finish_before_finish_is_none() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let stream_handle = commit.upload_stream_blocking(None, Sha256Policy::Compute).unwrap();
        let py_stream = PyXetStreamUpload { inner: stream_handle };
        // Before finish(), try_finish() should return None
        assert!(py_stream.try_finish().is_none());
    }
}
