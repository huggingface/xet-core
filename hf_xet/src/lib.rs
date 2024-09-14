mod data_client;
mod config;

use pyo3::{pyfunction, PyResult};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use data::PointerFile;

#[pyfunction]
#[pyo3(signature = (file_paths), text_signature = "(file_paths: List[str]) -> List[PyPointerFile]")]
pub fn upload_files(file_paths: Vec<String>) -> PyResult<Vec<PyPointerFile>> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            data_client::upload_async(file_paths).await
        }).map_err(|e| PyException::new_err(format!("{e:?}")))?
        .into_iter()
        .map(PyPointerFile::from)
        .collect())
}

#[pyfunction]
#[pyo3(signature = (files), text_signature = "(files: List[PyPointerFile]) -> List[str]")]
pub fn download_files(files: Vec<PyPointerFile>) -> PyResult<Vec<String>> {
    let pfs = files.into_iter().map(PointerFile::from)
        .collect();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            data_client::download_async(pfs).await
        }).map_err(|e| PyException::new_err(format!("{e:?}")))
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyPointerFile {
    #[pyo3(get, set)]
    path: String,
    #[pyo3(get)]
    hash: String,
    #[pyo3(get)]
    filesize: u64,
}

impl From<PointerFile> for PyPointerFile {
    fn from(pf: PointerFile) -> Self {
        Self {
            path: pf.path().to_string(),
            hash: pf.hash_string().to_string(),
            filesize: pf.filesize(),
        }
    }
}

impl From<PyPointerFile> for PointerFile {
    fn from(pf: PyPointerFile) -> Self {
        PointerFile::init_from_info(&pf.path, &pf.hash, pf.filesize)
    }
}

#[pymethods]
impl PyPointerFile {
    #[new]
    pub fn new(path: String, hash: String, filesize: u64) -> Self {
        Self {
            path,
            hash,
            filesize,
        }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyPointerFile({}, {}, {})", self.path, self.hash, self.filesize)
    }
}

#[pymodule]
pub fn hf_xet(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;
    Ok(())
}
