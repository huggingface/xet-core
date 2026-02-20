mod logging;
mod progress_update;
mod runtime;
mod token_refresh;

use std::collections::HashMap;
use std::fmt::Debug;
use std::iter::IntoIterator;
use std::sync::Arc;

use data::errors::DataProcessingError;
use data::{XetFileInfo, data_client};
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use itertools::Itertools;
use progress_tracking::TrackingProgressUpdater;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::pyfunction;
use rand::Rng;
use runtime::async_run;
use token_refresh::WrappedTokenRefresher;
use tracing::debug;
use xet_runtime::file_handle_limits;

use crate::logging::init_logging;
use crate::progress_update::WrappedProgressUpdater;

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

/// Converts a HashMap of headers to a HeaderMap and merges in the USER_AGENT.
///
/// If the input contains a User-Agent header, the USER_AGENT is appended to it.
/// Otherwise, USER_AGENT is set as the only User-Agent header.
fn build_headers_with_user_agent(
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Option<Arc<HeaderMap>>> {
    let mut map = request_headers
        .map(|headers| {
            let mut map = HeaderMap::new();
            for (key, value) in headers {
                let name = HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| PyRuntimeError::new_err(format!("Invalid header name '{}': {}", key, e)))?;
                let value = HeaderValue::from_str(&value)
                    .map_err(|e| PyRuntimeError::new_err(format!("Invalid header value for '{}': {}", key, e)))?;
                map.insert(name, value);
            }
            Ok::<_, PyErr>(map)
        })
        .transpose()?
        .unwrap_or_default();

    // Append our USER_AGENT to any existing User-Agent header, or add it if not present
    let combined_user_agent = if let Some(existing_ua) = map.get(header::USER_AGENT) {
        // Append our user agent to the existing one
        let existing_str = existing_ua.to_str().unwrap_or("");
        format!("{}; {}", existing_str, USER_AGENT)
    } else {
        // No existing user agent, use ours
        USER_AGENT.to_string()
    };

    // Try to create the combined header value, fall back gracefully if invalid
    let user_agent_value = HeaderValue::from_str(&combined_user_agent)
        .or_else(|_: http::header::InvalidHeaderValue| Ok::<HeaderValue, http::header::InvalidHeaderValue>(HeaderValue::from_static(USER_AGENT)))
        .unwrap_or_else(|_: http::header::InvalidHeaderValue| HeaderValue::from_static("unknown"));
    map.insert(header::USER_AGENT, user_agent_value);

    Ok(Some(Arc::new(map)))
}

fn convert_data_processing_error(e: DataProcessingError) -> PyErr {
    if cfg!(debug_assertions) {
        PyRuntimeError::new_err(format!("Data processing error: {e:?}"))
    } else {
        PyRuntimeError::new_err(format!("Data processing error: {e}"))
    }
}

#[pyfunction]
#[pyo3(signature = (file_contents, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None), text_signature = "(file_contents: List[bytes], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str], request_headers: Optional[Dict[str, str]]) -> List[PyXetUploadInfo]")]
pub fn upload_bytes(
    py: Python,
    file_contents: Vec<Vec<u8>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Vec<PyXetUploadInfo>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater.map(WrappedProgressUpdater::new).transpose()?.map(Arc::new);
    let x: u64 = rand::rng().random();

    // Convert Python dict -> Rust HashMap -> HeaderMap and merge with USER_AGENT
    let header_map = build_headers_with_user_agent(request_headers)?;

    async_run(py, async move {
        debug!(
            "Upload bytes call {x:x}: (PID = {}) Uploading {} files as bytes.",
            std::process::id(),
            file_contents.len(),
        );

        let out: Vec<PyXetUploadInfo> = data_client::upload_bytes_async(
            file_contents,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
            header_map,
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();

        debug!("Upload bytes call {x:x} finished.");

        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int], None]], _repo_type: Optional[str], request_headers: Optional[Dict[str, str]]) -> List[PyXetUploadInfo]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Vec<PyXetUploadInfo>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater.map(WrappedProgressUpdater::new).transpose()?.map(Arc::new);

    let file_names = file_paths.iter().take(3).join(", ");

    let x: u64 = rand::rng().random();

    // Convert Python dict -> Rust HashMap -> HeaderMap and merge with USER_AGENT
    let header_map = build_headers_with_user_agent(request_headers)?;

    async_run(py, async move {
        debug!(
            "Upload call {x:x}: (PID = {}) Uploading {} files {file_names}{}",
            std::process::id(),
            file_paths.len(),
            if file_paths.len() > 3 { "..." } else { "." }
        );

        let out: Vec<PyXetUploadInfo> = data_client::upload_async(
            file_paths,
            None,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
            header_map,
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();
        debug!("Upload call {x:x} finished.");
        PyResult::Ok(out)
    })
}

/// Compute xet hashes for files without uploading.
///
/// This function computes cryptographic hashes for the specified files using the same
/// chunking and hashing algorithm as upload operations, but without requiring
/// authentication or server connection. The resulting hashes can be used to verify
/// file integrity after downloads or to determine which files need to be uploaded.
///
/// Args:
///     file_paths: List of file paths to hash.
///
/// Returns:
///     List[PyXetUploadInfo]: List of hash results in the same order as input paths.
///         Each result contains the hash (as hex string) and file size in bytes.
///
/// Raises:
///     RuntimeError: If any file cannot be read or hashed.
///
/// Example:
///     >>> import hf_xet
///     >>> results = hf_xet.hash_files(["/path/to/file1.txt", "/path/to/file2.txt"])
///     >>> for path, info in zip(file_paths, results):
///     ...     print(f"Hash: {info.hash}, Size: {info.file_size}")
///
/// Note:
///     This function is primarily used for validation and verification of transferred
///     files. Clients can verify that downloaded files are correctly reassembled by
///     comparing the computed hash with the expected hash from the server.
#[pyfunction]
#[pyo3(signature = (file_paths), text_signature = "(file_paths: List[str]) -> List[PyXetUploadInfo]")]
pub fn hash_files(py: Python, file_paths: Vec<String>) -> PyResult<Vec<PyXetUploadInfo>> {
    async_run(py, async move {
        let out: Vec<PyXetUploadInfo> = data_client::hash_files_async(file_paths)
            .await
            .map_err(convert_data_processing_error)?
            .into_iter()
            .map(PyXetUploadInfo::from)
            .collect();

        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater, request_headers=None), text_signature = "(files: List[PyXetDownloadInfo], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[List[Callable[[int], None]]], request_headers: Optional[Dict[str, str]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyXetDownloadInfo>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Vec<String>> {
    let file_infos: Vec<_> = files.into_iter().map(<(XetFileInfo, DestinationPath)>::from).collect();
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater.map(try_parse_progress_updaters).transpose()?;

    // Convert Python dict -> Rust HashMap -> HeaderMap and merge with USER_AGENT
    let header_map = build_headers_with_user_agent(request_headers)?;

    let x: u64 = rand::rng().random();

    let file_names = file_infos.iter().take(3).map(|(_, p)| p).join(", ");

    async_run(py, async move {
        debug!(
            "Download call {x:x}: (PID = {}) Downloading {} files {file_names}{}",
            std::process::id(),
            file_infos.len(),
            if file_infos.len() > 3 { "..." } else { "." }
        );

        let out: Vec<String> = data_client::download_async(
            file_infos,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updaters,
            header_map,
        )
        .await
        .map_err(convert_data_processing_error)?;

        debug!("Download call {x:x}: Completed.");

        PyResult::Ok(out)
    })
}

#[pyfunction]
pub fn force_sigint_shutdown() -> PyResult<()> {
    // Force a signint shutdown in the case where it gets intercepted by another process.
    crate::runtime::perform_sigint_shutdown();
    Err(PyKeyboardInterrupt::new_err(()))
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

#[pymodule(gil_used = false)]
#[allow(unused_variables)]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(upload_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(hash_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_function(wrap_pyfunction!(force_sigint_shutdown, m)?)?;
    m.add_class::<PyXetUploadInfo>()?;
    m.add_class::<PyXetDownloadInfo>()?;
    m.add_class::<PyXetUploadInfo>()?;
    m.add_class::<progress_update::PyItemProgressUpdate>()?;
    m.add_class::<progress_update::PyTotalProgressUpdate>()?;

    // TODO: remove this during the next major version update.
    // This supports backward compatibility for PyPointerFile with old versions
    // huggingface_hub.
    m.add_class::<PyPointerFile>()?;

    // Make sure the logger is set up.
    init_logging(py);

    // Raise the soft file handle limits if possible
    file_handle_limits::raise_nofile_soft_to_hard();

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Initialize Python once for all tests
    fn setup() {
        // When auto-initialize is enabled, Python will be initialized on first use
        // This ensures Python is available for the tests
        let _ = pyo3::Python::attach(|_py| {});
    }

    #[test]
    fn test_build_headers_with_none_empty_hashmap() {
        setup();
        let empty_map: HashMap<String, String> = HashMap::new();
        let result = build_headers_with_user_agent(Some(empty_map)).unwrap();
        let headers = result.unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key(header::USER_AGENT));

        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);

        let result = build_headers_with_user_agent(None).unwrap();
        let headers = result.unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key(header::USER_AGENT));

        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_with_valid_headers() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("Content-Type".to_string(), "application/json".to_string());
        headers_map.insert("Authorization".to_string(), "Bearer token123".to_string());

        let result = build_headers_with_user_agent(Some(headers_map)).unwrap();
        let headers = result.unwrap();

        // Should have 3 headers: Content-Type, Authorization, and USER_AGENT
        assert_eq!(headers.len(), 3);

        // Verify each header was converted correctly
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
            "application/json"
        );
        assert_eq!(
            headers.get(header::AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer token123"
        );

        // Verify USER_AGENT was added
        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_appends_to_existing_user_agent() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("User-Agent".to_string(), "CustomClient/1.0".to_string());

        let result = build_headers_with_user_agent(Some(headers_map)).unwrap();
        let headers = result.unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);

        // Verify USER_AGENT was appended to existing one
        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, format!("CustomClient/1.0; {}", USER_AGENT));
    }

    #[test]
    fn test_build_headers_with_invalid_header_name_or_value() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("Invalid Header!".to_string(), "value".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        // Should return an error for invalid header name
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header name"));
        

        let mut headers_map = HashMap::new();
        // Header values cannot contain newlines 
        headers_map.insert("X-Custom".to_string(), "value\nwith\nnewlines".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        // Should return an error for invalid header value
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header value"));
    }
}
