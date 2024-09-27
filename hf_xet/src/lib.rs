mod config;
mod data_client;
mod log;
mod token_refresh;

use cas::auth::TokenRefresher;
use data::{errors, PointerFile};
use parutils::{tokio_par_for_each, ParallelError};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::pyfunction;
use std::fmt::Debug;
use std::sync::Arc;
use token_refresh::WrappedTokenRefresher;
use tracing::info;

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token, token_refresher), text_signature = "(file_paths: List[str], endpoint: Optional[str], token: Optional[str], token_refresher: Optional[Callable[[], (str, int)]]) -> List[PyPointerFile]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token: Option<String>,
    token_refresher: Option<Py<PyAny>>,
) -> PyResult<Vec<PyPointerFile>> {
    let refresher = token_refresher
        .map(WrappedTokenRefresher::from_func)
        .transpose()?
        .map(to_arc_dyn);
    // Release GIL to allow python concurrency
    py.allow_threads(move || {
        Ok(tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async {
                data_client::upload_async(file_paths, endpoint, token, refresher).await
            })
            .map_err(|e| PyException::new_err(format!("{e:?}")))?
            .into_iter()
            .map(PyPointerFile::from)
            .collect())
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token, token_refresher), text_signature = "(files: List[PyPointerFile], endpoint: Optional[str], token: Optional[str], token_refresher: Optional[Callable[[], (str, int)]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token: Option<String>,
    token_refresher: Option<Py<PyAny>>,
) -> PyResult<Vec<String>> {
    let pfs = files.into_iter().map(PointerFile::from).collect();
    let refresher = token_refresher
        .map(WrappedTokenRefresher::from_func)
        .transpose()?
        .map(to_arc_dyn);
    // Release GIL to allow python concurrency
    py.allow_threads(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(
                async move { data_client::download_async(pfs, endpoint, token, refresher).await },
            )
            .map_err(|e| PyException::new_err(format!("{e:?}")))
    })
}

// helper to convert the implemented WrappedTokenRefresher into an Arc<dyn TokenRefresher>
#[inline]
fn to_arc_dyn(r: WrappedTokenRefresher) -> Arc<dyn TokenRefresher> {
    Arc::new(r)
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
        format!(
            "PyPointerFile({}, {}, {})",
            self.path, self.hash, self.filesize
        )
    }
}

#[pyfunction]
pub fn try_refresh_token(py: Python, func: Py<PyAny>) -> PyResult<(String, u64)> {
    let refresher = WrappedTokenRefresher::from_func(func)?;
    py.allow_threads(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move { try_refresh(refresher).await })
            .map_err(|e| PyException::new_err(format!("{e:?}")))
    })
}

async fn try_refresh(refresher: WrappedTokenRefresher) -> errors::Result<(String, u64)> {
    let refresher = Arc::new(refresher);
    let tokens = tokio_par_for_each(
        (0..20).collect(),
        data_client::MAX_CONCURRENT_UPLOADS,
        |i, _| {
            let refresher = refresher.clone();
            async move {
                let result = refresher.refresh()?;
                info!("got result {i}:{result:?}");
                Ok(result)
            }
        },
    )
    .await
    .map_err(|e: ParallelError<errors::DataProcessingError>| {
        errors::DataProcessingError::InternalError(format!("err: {e:?}"))
    })?;
    Ok(tokens[0].clone())
}

#[pymodule]
pub fn hf_xet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    log::initialize_logging();
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;
    m.add_function(wrap_pyfunction!(try_refresh_token, m)?)?;
    Ok(())
}
