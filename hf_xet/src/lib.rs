mod config;
mod data_client;
mod log;
mod token_refresh;

use utils::auth::TokenRefresher;
use data::PointerFile;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::pyfunction;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use token_refresh::WrappedTokenRefresher;

// This will be the information that will finally be sent to the telemetry endpoint
//
// #[pyclass]
// #[derive(Clone, Debug)]
// pub struct UploadTelemetry {
//     #[pyo3(get)]
//     total_time_ms: u64,
//     #[pyo3(get)]
//     uploaded_bytes: u64,
// }
//
// #[pyclass]
// #[derive(Clone, Debug)]
// pub struct DownloadTelemetry {
//     #[pyo3(get)]
//     total_time_ms: u64,
//     #[pyo3(get)]
//     downloaded_bytes: u64,
//     #[pyo3(get)]
//     cached_bytes: u32,
// }


#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]]) -> List[PyPointerFile]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
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
                data_client::upload_async(file_paths, endpoint, token_info, refresher).await
            })
            .map_err(|e| PyException::new_err(format!("{e:?}")))?
            .into_iter()
            .map(PyPointerFile::from)
            .collect())
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher), text_signature = "(files: List[PyPointerFile], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
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
            .block_on(async move {
                data_client::download_async(pfs, endpoint, token_info, refresher).await
            })
            .map(|pfs| pfs.into_iter().map(|pointer_file| pointer_file.path().to_string()).collect())
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
    #[pyo3(get)]
    compressed_size: u64,
    #[pyo3(get)]
    latency: f64,
}


impl From<PointerFile> for PyPointerFile {
    fn from(pf: PointerFile) -> Self {
        Self {
            path: pf.path().to_string(),
            hash: pf.hash_string().to_string(),
            filesize: pf.filesize(),
            compressed_size: pf.compressed_size(),
            latency: pf.latency(),
        }
    }
}

impl From<PyPointerFile> for PointerFile {
    fn from(pf: PyPointerFile) -> Self {
        PointerFile::init_from_info(&pf.path, &pf.hash, pf.filesize, pf.compressed_size, Duration::from_secs_f64(pf.latency))
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
            compressed_size: 0,
            latency: 0.0,
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

#[pymodule]
pub fn hf_xet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    log::initialize_logging();
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;
    Ok(())
}
