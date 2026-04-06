mod logging;
mod progress_update;
mod runtime;

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::time::Duration;

use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use itertools::Itertools;
use pyo3::exceptions::{PyKeyboardInterrupt, PyValueError};
use pyo3::prelude::*;
use pyo3::pyfunction;
use rand::Rng;
use runtime::async_run;
use tracing::debug;
use xet_pkg::XetError;
use xet_pkg::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
use xet_runtime::core::file_handle_limits;

use crate::logging::init_logging;
use crate::progress_update::{GroupProgressDiffState, ProgressCallback, send_simple_progress};

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

/// Build a HeaderMap from a Python dict and merge in the USER_AGENT.
fn build_headers_with_user_agent(request_headers: Option<HashMap<String, String>>) -> PyResult<HeaderMap> {
    let mut map = request_headers.map(build_header_map).transpose()?.unwrap_or_default();

    let combined_user_agent = if let Some(existing_ua) = map.get(header::USER_AGENT) {
        let existing_str = existing_ua.to_str().unwrap_or("");
        format!("{}; {}", existing_str, USER_AGENT)
    } else {
        USER_AGENT.to_string()
    };

    let user_agent_value = HeaderValue::from_str(&combined_user_agent)
        .or_else(|_: http::header::InvalidHeaderValue| {
            Ok::<HeaderValue, http::header::InvalidHeaderValue>(HeaderValue::from_static(USER_AGENT))
        })
        .unwrap_or_else(|_: http::header::InvalidHeaderValue| HeaderValue::from_static("unknown"));
    map.insert(header::USER_AGENT, user_agent_value);

    Ok(map)
}

/// Build a HeaderMap from a Python dict without adding USER_AGENT.
fn build_header_map(headers: HashMap<String, String>) -> PyResult<HeaderMap> {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| PyValueError::new_err(format!("Invalid header name '{}': {}", key, e)))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|e| PyValueError::new_err(format!("Invalid header value for '{}': {}", key, e)))?;
        map.insert(name, value);
    }
    Ok(map)
}

fn convert_xet_error(e: impl Into<XetError>) -> PyErr {
    PyErr::from(e.into())
}

/// Configure an auth group builder with optional endpoint, token, headers, and refresh URL.
macro_rules! configure_auth_builder {
    ($builder:expr, $endpoint:expr, $token_info:expr, $custom_headers:expr,
     $token_refresh_url:expr, $token_refresh_headers:expr) => {{
        let mut builder = $builder;
        if let Some(ep) = $endpoint {
            builder = builder.with_endpoint(ep);
        }
        if let Some((token, expiry)) = $token_info {
            builder = builder.with_token_info(token, expiry);
        }
        if let Some(headers) = $custom_headers {
            builder = builder.with_custom_headers(headers);
        }
        if let Some(url) = $token_refresh_url {
            let refresh_headers = $token_refresh_headers.unwrap_or_default();
            builder = builder.with_token_refresh_url(url, refresh_headers);
        }
        builder
    }};
}

#[pyfunction]
#[pyo3(signature = (file_contents, endpoint, token_info, token_refresh_url, token_refresh_headers, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=false), text_signature = "(file_contents: List[bytes], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresh_url: Optional[str], token_refresh_headers: Optional[Dict[str, str]], progress_updater: Optional[Callable], _repo_type: Optional[str], request_headers: Optional[Dict[str, str]], sha256s: Optional[List[str]], skip_sha256: bool = False) -> List[PyXetUploadInfo]")]
#[allow(clippy::too_many_arguments)]
pub fn upload_bytes(
    py: Python,
    file_contents: Vec<Vec<u8>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
    sha256s: Option<Vec<String>>,
    skip_sha256: bool,
) -> PyResult<Vec<PyXetUploadInfo>> {
    if skip_sha256 && sha256s.is_some() {
        return Err(PyValueError::new_err("skip_sha256=True and sha256s are mutually exclusive"));
    }

    if let Some(ref s) = sha256s
        && s.len() != file_contents.len()
    {
        return Err(PyValueError::new_err(format!(
            "sha256s length ({}) must match file_contents length ({})",
            s.len(),
            file_contents.len()
        )));
    }

    let sha256_policies: Vec<Sha256Policy> = match sha256s {
        _ if skip_sha256 => vec![Sha256Policy::Skip; file_contents.len()],
        Some(v) => v.iter().map(|s| Sha256Policy::from_hex(s)).collect(),
        None => vec![Sha256Policy::Compute; file_contents.len()],
    };

    let callback = progress_updater.map(ProgressCallback::new).transpose()?;
    let custom_headers = build_headers_with_user_agent(request_headers)?;
    let refresh_headers = token_refresh_headers.map(build_header_map).transpose()?;
    let x: u64 = rand::rng().random();

    async_run(py, async move {
        debug!(
            "Upload bytes call {x:x}: (PID = {}) Uploading {} files as bytes.",
            std::process::id(),
            file_contents.len(),
        );

        let session = XetSessionBuilder::new().build().map_err(convert_xet_error)?;
        let builder = session.new_upload_commit().map_err(convert_xet_error)?;
        let builder = configure_auth_builder!(
            builder,
            endpoint,
            token_info,
            Some(custom_headers),
            token_refresh_url,
            refresh_headers
        );
        let commit = builder.build().await.map_err(convert_xet_error)?;

        // Upload all byte blobs
        let mut handles = Vec::with_capacity(file_contents.len());
        for (blob, sha256) in file_contents.into_iter().zip(sha256_policies) {
            let handle = commit.upload_bytes(blob, sha256, None).await.map_err(convert_xet_error)?;
            handles.push(handle);
        }

        // Commit with concurrent progress polling
        let out = commit_with_progress(&commit, &handles, callback.as_ref()).await?;

        debug!("Upload bytes call {x:x} finished.");
        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresh_url, token_refresh_headers, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=false), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresh_url: Optional[str], token_refresh_headers: Optional[Dict[str, str]], progress_updater: Optional[Callable], _repo_type: Optional[str], request_headers: Optional[Dict[str, str]], sha256s: Optional[List[str]], skip_sha256: bool = False) -> List[PyXetUploadInfo]")]
#[allow(clippy::too_many_arguments)]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
    sha256s: Option<Vec<String>>,
    skip_sha256: bool,
) -> PyResult<Vec<PyXetUploadInfo>> {
    if skip_sha256 && sha256s.is_some() {
        return Err(PyValueError::new_err("skip_sha256=True and sha256s are mutually exclusive"));
    }

    if let Some(ref s) = sha256s
        && s.len() != file_paths.len()
    {
        return Err(PyValueError::new_err(format!(
            "sha256s length ({}) must match file_paths length ({})",
            s.len(),
            file_paths.len()
        )));
    }

    let sha256_policies: Vec<Sha256Policy> = match sha256s {
        _ if skip_sha256 => vec![Sha256Policy::Skip; file_paths.len()],
        Some(v) => v.iter().map(|s| Sha256Policy::from_hex(s)).collect(),
        None => vec![Sha256Policy::Compute; file_paths.len()],
    };

    let callback = progress_updater.map(ProgressCallback::new).transpose()?;
    let custom_headers = build_headers_with_user_agent(request_headers)?;
    let refresh_headers = token_refresh_headers.map(build_header_map).transpose()?;

    let file_names = file_paths.iter().take(3).join(", ");
    let x: u64 = rand::rng().random();

    async_run(py, async move {
        debug!(
            "Upload call {x:x}: (PID = {}) Uploading {} files {file_names}{}",
            std::process::id(),
            file_paths.len(),
            if file_paths.len() > 3 { "..." } else { "." }
        );

        let session = XetSessionBuilder::new().build().map_err(convert_xet_error)?;
        let builder = session.new_upload_commit().map_err(convert_xet_error)?;
        let builder = configure_auth_builder!(
            builder,
            endpoint,
            token_info,
            Some(custom_headers),
            token_refresh_url,
            refresh_headers
        );
        let commit = builder.build().await.map_err(convert_xet_error)?;

        // Upload all files
        let mut handles = Vec::with_capacity(file_paths.len());
        for (path, sha256) in file_paths.into_iter().zip(sha256_policies) {
            let handle = commit
                .upload_from_path(PathBuf::from(path), sha256)
                .await
                .map_err(convert_xet_error)?;
            handles.push(handle);
        }

        // Commit with concurrent progress polling
        let out = commit_with_progress(&commit, &handles, callback.as_ref()).await?;

        debug!("Upload call {x:x} finished.");
        PyResult::Ok(out)
    })
}

/// Commit an upload, polling progress concurrently if a callback is provided.
/// Returns results in the same order as the handles.
async fn commit_with_progress(
    commit: &xet_pkg::xet_session::XetUploadCommit,
    handles: &[xet_pkg::xet_session::XetFileUpload],
    callback: Option<&ProgressCallback>,
) -> PyResult<Vec<PyXetUploadInfo>> {
    // Collect task_ids in order so we can map results back
    let task_ids: Vec<_> = handles.iter().map(|h| h.task_id()).collect();

    let use_progress = callback.is_some_and(|c| c.is_enabled());

    if use_progress {
        let callback = callback.unwrap();
        let commit_clone = commit.clone();
        let mut commit_task = tokio::spawn(async move { commit_clone.commit().await });

        let mut diff_state = GroupProgressDiffState::new();
        let mut interval = tokio::time::interval(Duration::from_millis(250));

        let report = loop {
            tokio::select! {
                result = &mut commit_task => {
                    let report = result
                        .map_err(|e| convert_xet_error(XetError::from(e)))?
                        .map_err(convert_xet_error)?;

                    // Final progress update
                    let group = commit.progress();
                    let items = handles
                        .iter()
                        .filter_map(|h| h.progress().map(|p| (h.task_id(), p)))
                        .collect();
                    let diff = diff_state.compute_diff(group, items);
                    let _ = callback.send_update(diff).await;

                    break report;
                }
                _ = interval.tick() => {
                    let group = commit.progress();
                    let items = handles
                        .iter()
                        .filter_map(|h| h.progress().map(|p| (h.task_id(), p)))
                        .collect();
                    let diff = diff_state.compute_diff(group, items);
                    let _ = callback.send_update(diff).await;
                }
            }
        };

        // Map results in order
        let mut out = Vec::with_capacity(task_ids.len());
        for id in &task_ids {
            let meta = report
                .uploads
                .get(id)
                .ok_or_else(|| convert_xet_error(XetError::Internal(format!("missing upload result for task {id}"))))?;
            out.push(PyXetUploadInfo::from(meta.xet_info.clone()));
        }
        Ok(out)
    } else {
        let report = commit.commit().await.map_err(convert_xet_error)?;

        let mut out = Vec::with_capacity(task_ids.len());
        for id in &task_ids {
            let meta = report
                .uploads
                .get(id)
                .ok_or_else(|| convert_xet_error(XetError::Internal(format!("missing upload result for task {id}"))))?;
            out.push(PyXetUploadInfo::from(meta.xet_info.clone()));
        }
        Ok(out)
    }
}

/// Compute xet hashes for files without uploading.
#[pyfunction]
#[pyo3(signature = (file_paths), text_signature = "(file_paths: List[str]) -> List[PyXetUploadInfo]")]
pub fn hash_files(py: Python, file_paths: Vec<String>) -> PyResult<Vec<PyXetUploadInfo>> {
    async_run(py, async move {
        let out: Vec<PyXetUploadInfo> = xet_data::processing::data_client::hash_files_async(file_paths)
            .await
            .map_err(convert_xet_error)?
            .into_iter()
            .map(PyXetUploadInfo::from)
            .collect();

        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresh_url, token_refresh_headers, progress_updater, request_headers=None), text_signature = "(files: List[PyXetDownloadInfo], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresh_url: Optional[str], token_refresh_headers: Optional[Dict[str, str]], progress_updater: Optional[List[Callable[[int], None]]], request_headers: Optional[Dict[str, str]]) -> List[str]")]
#[allow(clippy::too_many_arguments)]
pub fn download_files(
    py: Python,
    files: Vec<PyXetDownloadInfo>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresh_url: Option<String>,
    token_refresh_headers: Option<HashMap<String, String>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Vec<String>> {
    let file_infos: Vec<_> = files.into_iter().map(<(XetFileInfo, DestinationPath)>::from).collect();
    let custom_headers = build_headers_with_user_agent(request_headers)?;
    let refresh_headers = token_refresh_headers.map(build_header_map).transpose()?;

    let x: u64 = rand::rng().random();
    let file_names = file_infos.iter().take(3).map(|(_, p)| p).join(", ");

    async_run(py, async move {
        debug!(
            "Download call {x:x}: (PID = {}) Downloading {} files {file_names}{}",
            std::process::id(),
            file_infos.len(),
            if file_infos.len() > 3 { "..." } else { "." }
        );

        let session = XetSessionBuilder::new().build().map_err(convert_xet_error)?;
        let builder = session.new_file_download_group().map_err(convert_xet_error)?;
        let builder = configure_auth_builder!(
            builder,
            endpoint,
            token_info,
            Some(custom_headers),
            token_refresh_url,
            refresh_headers
        );
        let group = builder.build().await.map_err(convert_xet_error)?;

        // Queue all downloads
        let mut dl_handles = Vec::with_capacity(file_infos.len());
        let mut paths = Vec::with_capacity(file_infos.len());
        for (file_info, dest_path) in file_infos {
            let handle = group
                .download_file_to_path(file_info, PathBuf::from(&dest_path))
                .await
                .map_err(convert_xet_error)?;
            dl_handles.push(handle);
            paths.push(dest_path);
        }

        // Finish with concurrent progress polling
        let group_for_finish = group.clone();
        let mut finish_task = tokio::spawn(async move { group_for_finish.finish().await });

        if let Some(updaters) = progress_updater {
            let mut prev_completed: Vec<u64> = vec![0; dl_handles.len()];
            let mut interval = tokio::time::interval(Duration::from_millis(250));

            loop {
                tokio::select! {
                    result = &mut finish_task => {
                        result
                            .map_err(|e| convert_xet_error(XetError::from(e)))?
                            .map_err(convert_xet_error)?;

                        // Final per-file progress update
                        for (i, handle) in dl_handles.iter().enumerate() {
                            if i < updaters.len()
                                && let Some(report) = handle.progress()
                            {
                                let increment = report.bytes_completed.saturating_sub(prev_completed[i]);
                                send_simple_progress(&updaters[i], increment).await;
                            }
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        for (i, handle) in dl_handles.iter().enumerate() {
                            if i < updaters.len()
                                && let Some(report) = handle.progress()
                            {
                                let increment = report.bytes_completed.saturating_sub(prev_completed[i]);
                                if increment > 0 {
                                    prev_completed[i] = report.bytes_completed;
                                    send_simple_progress(&updaters[i], increment).await;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            finish_task
                .await
                .map_err(|e| convert_xet_error(XetError::from(e)))?
                .map_err(convert_xet_error)?;
        }

        debug!("Download call {x:x}: Completed.");
        PyResult::Ok(paths)
    })
}

#[pyfunction]
pub fn force_sigint_shutdown() -> PyResult<()> {
    crate::runtime::perform_sigint_shutdown();
    Err(PyKeyboardInterrupt::new_err(()))
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
    file_size: Option<u64>,
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

type DestinationPath = String;

impl From<XetFileInfo> for PyXetUploadInfo {
    fn from(xf: XetFileInfo) -> Self {
        Self {
            hash: xf.hash().to_owned(),
            file_size: xf.file_size().expect("upload metadata must always include a known file size"),
            sha256: xf.sha256().map(str::to_owned),
        }
    }
}

impl From<PyXetDownloadInfo> for (XetFileInfo, DestinationPath) {
    fn from(pf: PyXetDownloadInfo) -> Self {
        let file_info = match pf.file_size {
            Some(size) => XetFileInfo::new(pf.hash, size),
            None => XetFileInfo::new_hash_only(pf.hash),
        };
        (file_info, pf.destination_path)
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
    m.add_class::<progress_update::PyItemProgressUpdate>()?;
    m.add_class::<progress_update::PyTotalProgressUpdate>()?;

    // TODO: remove this during the next major version update.
    // This supports backward compatibility for PyPointerFile with old versions
    // huggingface_hub.
    m.add_class::<PyPointerFile>()?;

    xet_pkg::register_exceptions(m)?;

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
    use std::collections::HashMap;

    use super::*;

    // Initialize Python once for all tests
    fn setup() {
        pyo3::Python::attach(|_py| {});
    }

    #[test]
    fn test_build_headers_with_none_empty_hashmap() {
        setup();
        let empty_map: HashMap<String, String> = HashMap::new();
        let result = build_headers_with_user_agent(Some(empty_map)).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(header::USER_AGENT));

        let user_agent = result.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);

        let result = build_headers_with_user_agent(None).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(header::USER_AGENT));

        let user_agent = result.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_with_valid_headers() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("Content-Type".to_string(), "application/json".to_string());
        headers_map.insert("Authorization".to_string(), "Bearer token123".to_string());

        let result = build_headers_with_user_agent(Some(headers_map)).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(header::CONTENT_TYPE).unwrap().to_str().unwrap(), "application/json");
        assert_eq!(result.get(header::AUTHORIZATION).unwrap().to_str().unwrap(), "Bearer token123");

        let user_agent = result.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_appends_to_existing_user_agent() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("User-Agent".to_string(), "CustomClient/1.0".to_string());

        let result = build_headers_with_user_agent(Some(headers_map)).unwrap();

        assert_eq!(result.len(), 1);

        let user_agent = result.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, format!("CustomClient/1.0; {}", USER_AGENT));
    }

    #[test]
    fn test_build_headers_with_invalid_header_name_or_value() {
        setup();
        let mut headers_map = HashMap::new();
        headers_map.insert("Invalid Header!".to_string(), "value".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header name"));

        let mut headers_map = HashMap::new();
        headers_map.insert("X-Custom".to_string(), "value\nwith\nnewlines".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header value"));
    }

    #[test]
    fn test_upload_files_sha256s_length_mismatch() {
        setup();
        pyo3::Python::attach(|py| {
            let file_paths = vec!["a.txt".to_string(), "b.txt".to_string()];
            let sha256s = Some(vec!["abc123".to_string()]);

            let result = upload_files(py, file_paths, None, None, None, None, None, None, None, sha256s, false);

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("sha256s length (1) must match file_paths length (2)"), "got: {err_msg}");
        });
    }

    #[test]
    fn test_upload_files_skip_sha256_conflicts_with_sha256s() {
        setup();
        pyo3::Python::attach(|py| {
            let file_paths = vec!["a.txt".to_string()];
            let sha256s = Some(vec!["abc123".to_string()]);

            let result = upload_files(py, file_paths, None, None, None, None, None, None, None, sha256s, true);

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("mutually exclusive"), "got: {err_msg}");
        });
    }
}
