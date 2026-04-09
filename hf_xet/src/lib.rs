mod headers;
mod logging;
mod py_download_group;
mod py_download_stream_group;
mod py_upload_commit;
mod py_xet_session;

use pyo3::prelude::*;
use xet_pkg::XetError;

use crate::logging::init_logging;

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

pub(crate) fn convert_xet_error(e: impl Into<XetError>) -> PyErr {
    PyErr::from(e.into())
}

// ── Legacy data types (kept for backward compatibility) ───────────────────────

// TODO: we won't need to subclass this in the next major version update.
#[pyclass(subclass)]
#[derive(Clone, Debug)]
pub struct PyXetDownloadInfo {
    #[pyo3(get, set)]
    pub(crate) destination_path: String,
    #[pyo3(get)]
    pub(crate) hash: String,
    #[pyo3(get)]
    pub(crate) file_size: Option<u64>,
}

#[pymethods]
impl PyXetDownloadInfo {
    #[new]
    #[pyo3(signature = (destination_path, hash, file_size=None))]
    pub fn new(destination_path: String, hash: String, file_size: Option<u64>) -> Self {
        Self {
            destination_path,
            hash,
            file_size,
        }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        let size_str = self.file_size.map_or("None".to_string(), |s| s.to_string());
        format!("PyXetDownloadInfo({}, {}, {})", self.destination_path, self.hash, size_str)
    }
}

// TODO: on the next major version update, delete this class and the trait implementation.
// This is used to support backward compatibility for PyPointerFile with old versions of huggingface_hub
#[pyclass(extends=PyXetDownloadInfo)]
#[derive(Clone, Debug)]
pub struct PyPointerFile {}

#[pymethods]
impl PyPointerFile {
    #[new]
    pub fn new(path: String, hash: String, filesize: u64) -> (Self, PyXetDownloadInfo) {
        (PyPointerFile {}, PyXetDownloadInfo::new(path, hash, Some(filesize)))
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(self_: PyRef<'_, Self>) -> String {
        let super_ = self_.as_super();
        let size_str = super_.file_size.map_or("None".to_string(), |s| s.to_string());
        format!("PyPointerFile({}, {}, {})", super_.destination_path, super_.hash, size_str)
    }

    #[getter]
    fn get_path(self_: PyRef<'_, Self>) -> String {
        self_.as_super().destination_path.clone()
    }

    #[setter]
    fn set_path(mut self_: PyRefMut<'_, Self>, path: String) {
        self_.as_super().destination_path = path;
    }

    #[getter]
    fn filesize(self_: PyRef<'_, Self>) -> Option<u64> {
        self_.as_super().file_size
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyXetUploadInfo {
    #[pyo3(get)]
    pub hash: String,
    #[pyo3(get)]
    pub file_size: u64,
    #[pyo3(get)]
    pub sha256: Option<String>,
}

#[pymethods]
impl PyXetUploadInfo {
    #[new]
    pub fn new(hash: String, file_size: u64) -> Self {
        Self {
            hash,
            file_size,
            sha256: None,
        }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyXetUploadInfo({}, {}, {:?})", self.hash, self.file_size, self.sha256)
    }

    /// TODO: Remove these getters in the next major version update.
    #[getter]
    fn filesize(self_: PyRef<'_, Self>) -> u64 {
        self_.file_size
    }
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule(gil_used = false)]
#[allow(unused_variables)]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ── New XetSession API ───────────────────────────────────────────────────
    m.add_class::<py_xet_session::PyXetSession>()?;
    m.add_class::<py_upload_commit::PyXetUploadCommitBuilder>()?;
    m.add_class::<py_upload_commit::PyXetUploadCommit>()?;
    m.add_class::<py_upload_commit::PyXetFileUpload>()?;
    m.add_class::<py_upload_commit::PyXetFileUploadResult>()?;
    m.add_class::<py_upload_commit::PyXetCommitReport>()?;
    m.add_class::<py_download_group::PyXetFileDownloadGroupBuilder>()?;
    m.add_class::<py_download_group::PyXetFileDownloadGroup>()?;
    m.add_class::<py_download_group::PyXetFileDownload>()?;
    m.add_class::<py_download_group::PyXetDownloadReport>()?;
    m.add_class::<py_download_group::PyXetDownloadGroupReport>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStreamGroupBuilder>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStreamGroup>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStream>()?;
    m.add_class::<py_download_stream_group::PyXetUnorderedDownloadStream>()?;

    // ── Progress types (pyclass-annotated in xet_data with "python" feature) ─
    m.add_class::<xet_pkg::xet_session::GroupProgressReport>()?;
    m.add_class::<xet_pkg::xet_session::ItemProgressReport>()?;

    // ── Legacy types (kept for backward compatibility) ───────────────────────
    m.add_class::<PyXetUploadInfo>()?;
    m.add_class::<PyXetDownloadInfo>()?;
    m.add_class::<PyPointerFile>()?;

    // ── Exceptions ───────────────────────────────────────────────────────────
    xet_pkg::register_exceptions(m)?;

    // ── Logging ──────────────────────────────────────────────────────────────
    init_logging(py);

    #[cfg(feature = "profiling")]
    {
        profiling::start_profiler();

        #[pyfunction]
        fn profiler_cleanup() {
            profiling::save_profiler_report();
        }

        m.add_function(wrap_pyfunction!(profiler_cleanup, m)?)?;

        let atexit = PyModule::import(py, "atexit")?;
        atexit.call_method1("register", (m.getattr("profiler_cleanup")?,))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::*;

    fn setup() {
        let _ = Python::attach(|_py| {});
    }

    #[test]
    fn test_pyxetdownloadinfo_new() {
        setup();
        let info = PyXetDownloadInfo::new("out.bin".into(), "abc123".into(), Some(1024));
        assert_eq!(info.hash, "abc123");
        assert_eq!(info.file_size, Some(1024));
        assert_eq!(info.destination_path, "out.bin");
    }
}
