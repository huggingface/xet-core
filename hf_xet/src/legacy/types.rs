//! Legacy download/upload info types kept for backward compatibility with `huggingface_hub`.

use pyo3::prelude::*;
use xet_pkg::legacy::XetFileInfo;

// ── PyXetDownloadInfo ─────────────────────────────────────────────────────────

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

// ── PyXetUploadInfo ───────────────────────────────────────────────────────────

/// Result returned by the legacy ``upload_bytes`` / ``upload_files`` / ``hash_files`` functions.
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

    /// Alias kept for backward compatibility.
    #[getter]
    fn filesize(self_: PyRef<'_, Self>) -> u64 {
        self_.file_size
    }
}

impl From<XetFileInfo> for PyXetUploadInfo {
    fn from(xf: XetFileInfo) -> Self {
        Self {
            hash: xf.hash().to_owned(),
            file_size: xf.file_size().expect("upload metadata must always include a known file size"),
            sha256: xf.sha256().map(str::to_owned),
        }
    }
}

// ── PyPointerFile ─────────────────────────────────────────────────────────────

/// Legacy subclass of :class:`PyXetDownloadInfo`.
///
/// Kept for backward compatibility with old versions of ``huggingface_hub``.
// TODO: remove during the next major version update.
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

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::*;

    #[test]
    fn test_pyxetdownloadinfo_new() {
        let _ = Python::attach(|_py| {});
        let info = PyXetDownloadInfo::new("out.bin".into(), "abc123".into(), Some(1024));
        assert_eq!(info.hash, "abc123");
        assert_eq!(info.file_size, Some(1024));
        assert_eq!(info.destination_path, "out.bin");
    }
}
