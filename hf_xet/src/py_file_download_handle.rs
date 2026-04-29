use pyo3::prelude::*;
use xet_pkg::xet_session::{ItemProgressReport, UniqueID, XetDownloadReport, XetFileDownload};

use crate::utils::{progress_display, task_state_display, task_state_to_pystate};
use crate::{PyXetTaskState, convert_xet_error};

// ── PyXetFileDownload ─────────────────────────────────────────────────────────

/// Handle for a background file-download task.
///
/// Returned by :meth:`XetFileDownloadGroup.download_file`.
#[pyclass(name = "XetFileDownload")]
pub struct PyXetFileDownload {
    pub(crate) inner: XetFileDownload,
}

#[pymethods]
impl PyXetFileDownload {
    // Example output:
    //   XetFileDownload(task_id=3, status="Running", bytes_completed=1024/4096)
    //   XetFileDownload(task_id=4, status="Completed", bytes_completed=2048/2048)
    //   XetFileDownload(task_id=5, status="Running", bytes_completed=?/?)   ← before first progress report
    fn __repr__(&self) -> String {
        let status = task_state_display(self.inner.status());
        let prog = progress_display(self.inner.progress());
        format!("XetFileDownload(task_id={}, status=\"{}\", bytes_completed={})", self.inner.task_id(), status, prog)
    }

    /// Per-file progress, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state as a :class:`XetTaskState` enum value.  Raises on error.
    pub fn status(&self) -> PyResult<PyXetTaskState> {
        task_state_to_pystate(self.inner.status())
    }

    /// Wait for this download to complete and return its report.
    ///
    /// Releases the GIL.
    pub fn result(&self, py: Python<'_>) -> PyResult<XetDownloadReport> {
        let inner = self.inner.clone();
        py.detach(|| inner.finish_blocking().map_err(convert_xet_error))
    }

    /// Return the download report without blocking.
    ///
    /// Returns ``None`` if the download has not yet completed.
    /// Raises if the download completed with an error.
    pub fn try_result(&self) -> PyResult<Option<XetDownloadReport>> {
        match self.inner.result() {
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(convert_xet_error(e)),
            None => Ok(None),
        }
    }

    /// The unique task ID for this download.
    ///
    /// Matches the keys in :attr:`XetDownloadGroupReport.downloads`.
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id()
    }

    /// Cancel this individual download.
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use tempfile::tempdir;
    use xet_pkg::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};

    use super::*;

    fn upload_bytes_and_get_info(
        data: &[u8],
        endpoint: &str,
        session: &xet_pkg::xet_session::XetSession,
    ) -> XetFileInfo {
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap();
        let handle = commit
            .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, None)
            .unwrap();
        commit.commit_blocking().unwrap();
        let meta = handle.try_finish().unwrap();
        XetFileInfo::new(meta.xet_info.hash().to_owned(), meta.xet_info.file_size().unwrap_or(0))
    }

    #[test]
    fn test_file_download_handle_task_id_and_result() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let file_info = upload_bytes_and_get_info(b"hello world", &endpoint, &session);
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let dest = temp.path().join("out.bin");
        let handle = group.download_file_to_path_blocking(file_info, dest.clone()).unwrap();
        let py_handle = PyXetFileDownload { inner: handle };
        assert!(py_handle.task_id().0 > 0);
        Python::attach(|py| {
            let result = py_handle.result(py).unwrap();
            assert_eq!(result.file_info.file_size, Some(11));
            assert!(dest.exists());
        });
    }

    #[test]
    fn test_file_download_handle_cancel() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();
        let file_info = upload_bytes_and_get_info(b"cancel me", &endpoint, &session);
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap();
        let dest = temp.path().join("cancelled.bin");
        let handle = group.download_file_to_path_blocking(file_info, dest).unwrap();
        let py_handle = PyXetFileDownload { inner: handle };
        // cancel should not panic
        py_handle.cancel();
    }
}
