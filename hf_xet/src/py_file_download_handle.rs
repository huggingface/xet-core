use std::collections::HashMap;

use pyo3::prelude::*;
use xet_pkg::xet_session::{
    GroupProgressReport, ItemProgressReport, XetDownloadGroupReport, XetDownloadReport, XetFileDownload,
};

use crate::convert_xet_error;
use crate::py_xet_session::task_state_to_str;

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
    fn __repr__(&self) -> String {
        format!("XetFileDownload(task_id={:?})", self.inner.task_id().to_string())
    }

    /// Per-file progress, or ``None`` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.progress()
    }

    /// Current task state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// Wait for this download to complete and return its report.
    ///
    /// Releases the GIL.
    pub fn result(&self, py: Python<'_>) -> PyResult<PyXetDownloadReport> {
        let inner = self.inner.clone();
        let report = py.detach(|| inner.finish_blocking().map_err(convert_xet_error))?;
        Ok(PyXetDownloadReport::from(report))
    }

    /// Return the download report without blocking.
    ///
    /// Returns ``None`` if the download has not yet completed.
    /// Raises if the download completed with an error.
    pub fn try_result(&self) -> PyResult<Option<PyXetDownloadReport>> {
        match self.inner.result() {
            Some(Ok(r)) => Ok(Some(PyXetDownloadReport::from(r))),
            Some(Err(e)) => Err(convert_xet_error(e)),
            None => Ok(None),
        }
    }

    /// The unique task ID for this download, as a string.
    ///
    /// Matches the keys in :attr:`XetDownloadGroupReport.downloads`.
    pub fn task_id(&self) -> String {
        self.inner.task_id().to_string()
    }

    /// Cancel this individual download.
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

// ── PyXetDownloadReport ───────────────────────────────────────────────────────

/// Per-file result from a completed download.
#[pyclass(name = "XetDownloadReport", get_all)]
#[derive(Clone)]
pub struct PyXetDownloadReport {
    /// Local path where the file was written.
    pub path: Option<String>,
    /// Content-addressed hash of the downloaded file.
    pub hash: String,
    /// File size in bytes.
    pub file_size: Option<u64>,
}

#[pymethods]
impl PyXetDownloadReport {
    fn __repr__(&self) -> String {
        format!("XetDownloadReport(hash={:?}, path={:?}, file_size={:?})", self.hash, self.path, self.file_size)
    }
}

impl From<XetDownloadReport> for PyXetDownloadReport {
    fn from(r: XetDownloadReport) -> Self {
        Self {
            path: r.path.and_then(|p| p.to_str().map(str::to_owned)),
            hash: r.file_info.hash().to_owned(),
            file_size: r.file_info.file_size(),
        }
    }
}

// ── PyXetDownloadGroupReport ──────────────────────────────────────────────────

/// Summary returned by :meth:`XetFileDownloadGroup.finish`.
#[pyclass(name = "XetDownloadGroupReport", get_all)]
pub struct PyXetDownloadGroupReport {
    /// Final aggregate progress at the time the group completed.
    pub progress: GroupProgressReport,
    /// Per-file download reports, keyed by an internal task ID string.
    pub downloads: HashMap<String, PyXetDownloadReport>,
}

#[pymethods]
impl PyXetDownloadGroupReport {
    fn __repr__(&self) -> String {
        format!("XetDownloadGroupReport({} downloads)", self.downloads.len())
    }
}

impl From<XetDownloadGroupReport> for PyXetDownloadGroupReport {
    fn from(r: XetDownloadGroupReport) -> Self {
        let downloads = r
            .downloads
            .into_iter()
            .map(|(id, dr)| (id.to_string(), PyXetDownloadReport::from(dr)))
            .collect();
        Self {
            progress: r.progress,
            downloads,
        }
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
    fn test_download_report_repr() {
        let r = PyXetDownloadReport {
            path: Some("/tmp/f.bin".into()),
            hash: "abc".into(),
            file_size: Some(10),
        };
        assert!(r.__repr__().contains("abc"));
        assert!(r.__repr__().contains("/tmp/f.bin"));
    }

    #[test]
    fn test_download_group_report_repr_empty() {
        let report = PyXetDownloadGroupReport {
            progress: GroupProgressReport::default(),
            downloads: HashMap::new(),
        };
        assert!(report.__repr__().contains("0 downloads"));
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
        assert!(!py_handle.task_id().is_empty());
        Python::attach(|py| {
            let result = py_handle.result(py).unwrap();
            assert_eq!(result.file_size, Some(11));
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
