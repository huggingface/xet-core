use pyo3::prelude::*;
use xet_pkg::xet_session::{ItemProgressReport, XetStreamUpload};

use crate::convert_xet_error;
use crate::py_file_upload_handle::PyXetFileUploadResult;
use crate::py_xet_session::task_state_to_str;

// ── PyXetStreamUpload ─────────────────────────────────────────────────────────

/// Handle for a streaming upload within an :class:`XetUploadCommit`.
///
/// Returned by :meth:`XetUploadCommit.upload_stream`.  Feed data incrementally
/// with :meth:`write`, then call :meth:`finish` to finalise ingestion.
/// **:meth:`finish` must be called before** :meth:`XetUploadCommit.commit`.
#[pyclass(name = "XetStreamUpload")]
#[derive(Clone)]
pub struct PyXetStreamUpload {
    pub(crate) inner: XetStreamUpload,
}

#[pymethods]
impl PyXetStreamUpload {
    fn __repr__(&self) -> String {
        format!("XetStreamUpload(task_id={:?})", self.inner.task_id().to_string())
    }

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
    /// Must be called before :meth:`XetUploadCommit.commit`.
    /// Releases the GIL while waiting.
    pub fn finish(&self, py: Python<'_>) -> PyResult<PyXetFileUploadResult> {
        let inner = self.inner.clone();
        let meta = py.detach(|| inner.finish_blocking().map_err(convert_xet_error))?;
        Ok(PyXetFileUploadResult::from(meta))
    }

    /// Return upload metadata without blocking, or ``None`` if not yet finished.
    pub fn try_finish(&self) -> Option<PyXetFileUploadResult> {
        self.inner.try_finish().map(PyXetFileUploadResult::from)
    }

    /// Per-file progress snapshot, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// The unique task ID for this stream, as a string.
    pub fn task_id(&self) -> String {
        self.inner.task_id().to_string()
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
        assert!(!py_stream.task_id().is_empty());
        Python::attach(|py| {
            py_stream.write(py, b"hello world").unwrap();
            let result = py_stream.finish(py).unwrap();
            assert_eq!(result.file_size, 11);
            assert!(!result.hash.is_empty());
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
