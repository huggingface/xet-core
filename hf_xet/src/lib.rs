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
use progress_tracking::TrackingProgressUpdater;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use runtime::async_run;
use token_refresh::WrappedTokenRefresher;

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
#[pyo3(signature = (file_contents, endpoint, token_info, token_refresher, progress_updater, _repo_type), text_signature = "(file_contents: List[bytes], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str]) -> List[PyXetUploadInfo]")]
pub fn upload_bytes(
    py: Python,
    file_contents: Vec<Vec<u8>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
) -> PyResult<Vec<PyXetUploadInfo>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater.map(WrappedProgressUpdater::new).transpose()?.map(Arc::new);

    async_run(py, async move {
        let out: Vec<PyXetUploadInfo> = data_client::upload_bytes_async(
            file_contents,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();
        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher, progress_updater, _repo_type), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str]) -> List[PyXetUploadInfo]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
) -> PyResult<Vec<PyXetUploadInfo>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater.map(WrappedProgressUpdater::new).transpose()?.map(Arc::new);

    async_run(py, async move {
        let out: Vec<PyXetUploadInfo> = data_client::upload_async(
            file_paths,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();
        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater), text_signature = "(files: List[PyXetDownloadInfo], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[List[Callable[[int], None]]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyXetDownloadInfo>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<String>> {
    let file_infos = files.into_iter().map(<(XetFileInfo, DestinationPath)>::from).collect();
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater.map(try_parse_progress_updaters).transpose()?;

    async_run(py, async move {
        let out: Vec<String> =
            data_client::download_async(file_infos, endpoint, token_info, refresher.map(|v| v as Arc<_>), updaters)
                .await
                .map_err(convert_data_processing_error)?;

        PyResult::Ok(out)
    })
}

fn try_parse_progress_updaters(funcs: Vec<Py<PyAny>>) -> PyResult<Vec<Arc<dyn TrackingProgressUpdater>>> {
    let mut updaters = Vec::with_capacity(funcs.len());
    for updater_func in funcs {
        let wrapped = Arc::new(WrappedProgressUpdater::new(updater_func)?);
        updaters.push(wrapped as Arc<dyn TrackingProgressUpdater>);
    }
    Ok(updaters)
}

// TODO: we won't need to subclass this in the next major version update.
#[pyclass(subclass)]
#[derive(Clone, Debug)]
pub struct PyXetDownloadInfo {
    #[pyo3(get, set)]
    destination_path: String,
    #[pyo3(get)]
    hash: String,
    #[pyo3(get)]
    file_size: u64,
}

#[pymethods]
impl PyXetDownloadInfo {
    #[new]
    pub fn new(destination_path: String, hash: String, file_size: u64) -> Self {
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
        format!("PyXetDownloadInfo({}, {}, {})", self.destination_path, self.hash, self.file_size)
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
        (PyPointerFile {}, PyXetDownloadInfo::new(path, hash, filesize))
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(self_: PyRef<'_, Self>) -> String {
        let super_ = self_.as_super();
        format!("PyPointerFile({}, {}, {})", super_.destination_path, super_.hash, super_.file_size)
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
    fn filesize(self_: PyRef<'_, Self>) -> u64 {
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
}

#[pymethods]
impl PyXetUploadInfo {
    #[new]
    pub fn new(hash: String, file_size: u64) -> Self {
        Self { hash, file_size }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyXetUploadInfo({}, {})", self.hash, self.file_size)
    }

    /// TODO: Remove these getters in the next major version update.
    #[getter]
    fn filesize(self_: PyRef<'_, Self>) -> u64 {
        self_.file_size
    }
}

type DestinationPath = String;

impl From<XetFileInfo> for PyXetUploadInfo {
    fn from(xf: XetFileInfo) -> Self {
        Self {
            hash: xf.hash().to_owned(),
            file_size: xf.file_size(),
        }
    }
}

impl From<PyXetDownloadInfo> for (XetFileInfo, DestinationPath) {
    fn from(pf: PyXetDownloadInfo) -> Self {
        (XetFileInfo::new(pf.hash, pf.file_size), pf.destination_path)
    }
}

#[pymodule]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(upload_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyXetUploadInfo>()?;
    m.add_class::<PyXetDownloadInfo>()?;
    m.add_class::<PyXetUploadInfo>()?;
    m.add_class::<progress_update::PyItemProgressUpdate>()?;
    m.add_class::<progress_update::PyTotalProgressUpdate>()?;
    // TODO: remove this during the next major version update.
    // This supports backward compatibility for PyPointerFile with old versions
    // huggingface_hub.
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
