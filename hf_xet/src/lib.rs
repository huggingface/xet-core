mod log;
mod log_buffer;
mod progress_update;
mod runtime;
mod token_refresh;

use std::fmt::Debug;
use std::iter::IntoIterator;
use std::sync::Arc;

use data::{data_client, PointerFile};
use pyo3::prelude::*;
use pyo3::pyfunction;
use runtime::async_run;
use token_refresh::WrappedTokenRefresher;
use utils::auth::TokenRefresher;
use utils::progress::ProgressUpdater;
use utils::ThreadPool;

use crate::progress_update::WrappedProgressUpdater;

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]]) -> List[PyPointerFile]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
) -> PyResult<Vec<PyPointerFile>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);

    async_run(py, move |threadpool| async move {
        let out: Vec<_> = data_client::upload_async(threadpool, file_paths, endpoint, token_info, refresher.into())
            .await?
            .into_iter()
            .map(PyPointerFile::from)
            .collect();

        Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater), text_signature = "(files: List[PyPointerFile], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[List[Callable[[int], None]]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<String>> {
    let pfs = files.into_iter().map(PointerFile::from).collect();

    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater.map(try_parse_progress_updaters).transpose()?;
    async_run(py, move |threadpool| async move {
        data_client::download_async(threadpool, pfs, endpoint, token_info, refresher.into(), updaters).await
    })
}

fn try_parse_progress_updaters(funcs: Vec<Py<PyAny>>) -> PyResult<Vec<Arc<dyn ProgressUpdater>>> {
    let mut updaters = Vec::with_capacity(funcs.len());
    for updater_func in funcs {
        let wrapped = Arc::new(WrappedProgressUpdater::from_func(updater_func)?);
        updaters.push(wrapped);
    }
    Ok(updaters)
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
        Self { path, hash, filesize }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyPointerFile({}, {}, {})", self.path, self.hash, self.filesize)
    }
}

#[pymodule]
pub fn hf_xet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;
    Ok(())
}
