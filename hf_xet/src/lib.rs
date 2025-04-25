mod log;
mod log_buffer;
mod progress_update;
mod runtime;
mod token_refresh;

use std::fmt::Debug;
use std::iter::IntoIterator;
use std::sync::Arc;

use data::errors::DataProcessingError;
use data::{data_client, XetFileInfo};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use runtime::async_run;
use token_refresh::WrappedTokenRefresher;
use utils::progress::ProgressUpdater;

use crate::progress_update::WrappedProgressUpdater;

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

fn convert_data_processing_error(e: DataProcessingError) -> PyErr {
    if cfg!(debug_assertions) {
        PyRuntimeError::new_err(format!("Data processing error: {e:?}"))
    } else {
        PyRuntimeError::new_err(format!("Data processing error: {e}"))
    }
}

#[pyfunction]
#[pyo3(signature = (file_contents, endpoint, token_info, token_refresher, progress_updater, _repo_type), text_signature = "(file_contents: List[bytes], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str]) -> List[PyUploadResult]")]
pub fn upload_bytes(
    py: Python, 
    file_contents: Vec<Vec<u8>>, 
    endpoint: Option<String>, 
    token_info: Option<(String, u64)>, 
    token_refresher: Option<Py<PyAny>>, 
    progress_updater: Option<Py<PyAny>>, 
    _repo_type: Option<String>
) -> PyResult<Vec<PyUploadResult>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater
        .map(WrappedProgressUpdater::from_func)
        .transpose()?
        .map(Arc::new);

    async_run(py, move |thread_pool| async move {
        let out: Vec<PyUploadResult> = data_client::upload_bytes_async(
            thread_pool,
            file_contents,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyUploadResult::from)
        .collect();
        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher, progress_updater, _repo_type), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str]) -> List[PyUploadResult]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
) -> PyResult<Vec<PyUploadResult>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater
        .map(WrappedProgressUpdater::from_func)
        .transpose()?
        .map(Arc::new);

    async_run(py, move |threadpool| async move {
        let out: Vec<PyUploadResult> = data_client::upload_async(
            threadpool,
            file_paths,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyUploadResult::from)
        .collect();
        PyResult::Ok(out)
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
    let file_infos = files.into_iter().map(<(XetFileInfo, SourceFilePath)>::from).collect();

    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater.map(try_parse_progress_updaters).transpose()?;

    async_run(py, move |threadpool| async move {
        let out: Vec<String> = data_client::download_async(
            threadpool,
            file_infos,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updaters,
        )
        .await
        .map_err(convert_data_processing_error)?;

        PyResult::Ok(out)
    })
}

fn try_parse_progress_updaters(funcs: Vec<Py<PyAny>>) -> PyResult<Vec<Arc<dyn ProgressUpdater>>> {
    let mut updaters = Vec::with_capacity(funcs.len());
    for updater_func in funcs {
        let wrapped = Arc::new(WrappedProgressUpdater::from_func(updater_func)?);
        updaters.push(wrapped as Arc<dyn ProgressUpdater>);
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
    #[pyo3(get, name="filesize")]
    file_size: u64,
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyUploadResult {
    #[pyo3(get)]
    pub hash: String,
    #[pyo3(get)]
    pub file_size: u64,
}

type SourceFilePath = String;

impl From<XetFileInfo> for PyUploadResult {
    fn from(xf: XetFileInfo) -> Self {
        Self {
            hash: xf.hash_string(),
            file_size: xf.file_size() as u64,
        }
    }
}

impl From<PyPointerFile> for (XetFileInfo, SourceFilePath) {
    fn from(pf: PyPointerFile) -> Self {
        (XetFileInfo::new(pf.hash, pf.file_size as usize), pf.path)
    }
}

#[pymethods]
impl PyPointerFile {
    #[new]
    pub fn new(path: String, hash: String, file_size: u64) -> Self {
        Self { path, hash, file_size }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyPointerFile({}, {}, {})", self.path, self.hash, self.file_size)
    }
}

#[pymethods]
impl PyUploadResult {
    #[new]
    pub fn new(hash: String, file_size: u64) -> Self {
        Self { hash, file_size }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyUploadResult({}, {})", self.hash, self.file_size)
    }
}

#[pymodule]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(upload_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;

    // Init the threadpool
    runtime::init_threadpool(py)?;

    #[cfg(feature = "profiling")]
    {
        profiling::start_profiler();

        // Setup to save the results at the end.
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
