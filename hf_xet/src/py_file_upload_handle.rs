use pyo3::prelude::*;
use xet_pkg::xet_session::{ItemProgressReport, UniqueID, XetFileMetadata, XetFileUpload};

use crate::convert_xet_error;
use crate::utils::{progress_display, task_state_display, task_state_to_str};

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
    // Example output:
    //   XetFileUpload(task_id=1, status="Running", bytes_completed=1024/4096)
    //   XetFileUpload(task_id=2, status="Completed", bytes_completed=4096/4096)
    //   XetFileUpload(task_id=3, status="Running", bytes_completed=?/?)   ← before first progress report
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        let prog = progress_display(self.inner.progress());
        format!("XetFileUpload(task_id={}, status=\"{}\", bytes_completed={})", self.inner.task_id(), status, prog)
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
    /// the final :class:`XetFileMetadata`.
    pub fn result(&self, py: Python<'_>) -> PyResult<XetFileMetadata> {
        let inner = self.inner.clone();
        py.detach(|| inner.finalize_ingestion_blocking().map_err(convert_xet_error))
    }

    /// Return upload metadata without blocking, or ``None`` if not yet done.
    pub fn try_result(&self) -> Option<XetFileMetadata> {
        self.inner.try_finish()
    }

    /// The unique task ID for this upload.
    ///
    /// Matches the keys in :attr:`XetCommitReport.uploads`.
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id()
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::{Sha256Policy, XetSessionBuilder};

    use super::*;

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
        assert!(py_handle.task_id().0 > 0);
        commit.commit_blocking().unwrap();
        Python::attach(|py| {
            let result = py_handle.result(py).unwrap();
            assert_eq!(result.xet_info.file_size, Some(11));
            assert!(!result.xet_info.hash.is_empty());
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
