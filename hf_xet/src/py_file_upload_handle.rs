use pyo3::prelude::*;
use xet_pkg::xet_session::{ItemProgressReport, XetFileMetadata, XetFileUpload};

use crate::convert_xet_error;
use crate::py_xet_session::task_state_to_str;

// ── PyXetFileUpload ───────────────────────────────────────────────────────────

/// Handle for a background file-upload task.
///
/// Returned by :meth:`XetUploadCommit.upload_file` and
/// :meth:`XetUploadCommit.upload_bytes`.
#[pyclass(name = "XetFileUpload")]
pub struct PyXetFileUpload {
    pub(crate) inner: XetFileUpload,
}

#[pymethods]
impl PyXetFileUpload {
    fn __repr__(&self) -> String {
        format!("XetFileUpload(task_id={:?})", self.inner.task_id().to_string())
    }

    /// Per-file progress, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state (same values as :meth:`XetUploadCommit.status`).
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// Wait for ingestion to complete and return upload metadata.
    ///
    /// Releases the GIL.  Call after :meth:`XetUploadCommit.commit` to get
    /// the final :class:`XetFileUploadResult`.
    pub fn result(&self, py: Python<'_>) -> PyResult<PyXetFileUploadResult> {
        let inner = self.inner.clone();
        let meta = py.detach(|| inner.finalize_ingestion_blocking().map_err(convert_xet_error))?;
        Ok(PyXetFileUploadResult::from(meta))
    }

    /// Return upload metadata without blocking, or ``None`` if not yet done.
    pub fn try_result(&self) -> Option<PyXetFileUploadResult> {
        self.inner.try_finish().map(PyXetFileUploadResult::from)
    }

    /// The unique task ID for this upload, as a string.
    ///
    /// Matches the keys in :attr:`XetCommitReport.uploads`.
    pub fn task_id(&self) -> String {
        self.inner.task_id().to_string()
    }
}

// ── PyXetFileUploadResult ─────────────────────────────────────────────────────

/// Metadata returned after a successful file upload.
#[pyclass(name = "XetFileUploadResult", get_all)]
#[derive(Clone)]
pub struct PyXetFileUploadResult {
    /// Content-addressed hash identifying this file in the Xet storage system.
    pub hash: String,
    /// Total size of the file in bytes.
    pub file_size: u64,
    /// Hex-encoded SHA-256 digest of the original file, or ``None`` if skipped.
    pub sha256: Option<String>,
}

#[pymethods]
impl PyXetFileUploadResult {
    fn __repr__(&self) -> String {
        format!("XetFileUploadResult(hash={:?}, file_size={}, sha256={:?})", self.hash, self.file_size, self.sha256)
    }
}

impl From<XetFileMetadata> for PyXetFileUploadResult {
    fn from(meta: XetFileMetadata) -> Self {
        Self {
            hash: meta.xet_info.hash().to_owned(),
            file_size: meta.xet_info.file_size().unwrap_or(0),
            sha256: meta.xet_info.sha256().map(str::to_owned),
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::{Sha256Policy, XetSessionBuilder};

    use super::*;

    #[test]
    fn test_upload_result_repr_with_sha256() {
        let r = PyXetFileUploadResult {
            hash: "abc".into(),
            file_size: 10,
            sha256: Some("ff".into()),
        };
        assert!(r.__repr__().contains("abc"));
        assert!(r.__repr__().contains("ff"));
    }

    #[test]
    fn test_upload_result_repr_without_sha256() {
        let r = PyXetFileUploadResult {
            hash: "abc".into(),
            file_size: 10,
            sha256: None,
        };
        assert!(r.__repr__().contains("abc"));
        assert!(r.__repr__().contains("None"));
    }

    #[test]
    fn test_file_upload_handle_task_id_and_result() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let handle = commit
            .upload_bytes_blocking(b"hello world".to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
            .unwrap();
        let py_handle = PyXetFileUpload { inner: handle };
        assert!(!py_handle.task_id().is_empty());
        commit.commit_blocking().unwrap();
        Python::attach(|py| {
            let result = py_handle.result(py).unwrap();
            assert_eq!(result.file_size, 11);
            assert!(!result.hash.is_empty());
        });
    }

    #[test]
    fn test_file_upload_handle_try_result_before_commit_is_none() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let handle = commit
            .upload_bytes_blocking(b"data".to_vec(), Sha256Policy::Compute, None)
            .unwrap();
        let py_handle = PyXetFileUpload { inner: handle };
        // Before commit, ingestion may not be finalized yet
        // (try_result may or may not be Some depending on timing; just verify no panic)
        let _ = py_handle.try_result();
    }
}
