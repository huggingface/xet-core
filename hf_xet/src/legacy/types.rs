//! `PyXetDownloadInfo` — kept for backward compatibility with `huggingface_hub`.

use pyo3::prelude::*;

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
