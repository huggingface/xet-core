/// Deprecated top-level upload/download/hash functions.
///
/// These are the original `hf_xet` module-level functions from the pre-1.x API.
/// They are kept here for backward compatibility with older versions of
/// `huggingface_hub`.  New code should use the ``XetSession`` object-oriented API.
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use rand::RngExt;
use tracing::debug;
use xet_pkg::legacy::progress_tracking::TrackingProgressUpdater;
use xet_pkg::legacy::{Sha256Policy, XetFileInfo, data_client};

use super::progress_update::WrappedProgressUpdater;
use super::runtime::async_run;
use super::token_refresh::WrappedTokenRefresher;
use super::types::{PyXetDownloadInfo, PyXetUploadInfo};
use crate::convert_xet_error;
use crate::headers::build_headers_with_user_agent;

fn legacy_headers(request_headers: Option<HashMap<String, String>>) -> PyResult<Option<Arc<http::HeaderMap>>> {
    Ok(Some(Arc::new(build_headers_with_user_agent(request_headers)?)))
}

fn emit_deprecation(py: Python, msg: &str) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let category = py.get_type::<pyo3::exceptions::PyDeprecationWarning>();
    // stacklevel=2 so the warning points at the caller's frame, not this wrapper.
    warnings.call_method1("warn", (msg, category, 2i32))?;
    Ok(())
}

type DestinationPath = String;

impl From<PyXetDownloadInfo> for (XetFileInfo, DestinationPath) {
    fn from(pf: PyXetDownloadInfo) -> Self {
        let file_info = match pf.file_size {
            Some(size) => XetFileInfo::new(pf.hash, size),
            None => XetFileInfo::new_hash_only(pf.hash),
        };
        (file_info, pf.destination_path)
    }
}

/// Upload raw bytes to Xet storage.
///
/// .. deprecated::
///     Use :class:`XetSession` and :meth:`XetUploadCommit.start_upload_bytes` instead.
#[pyfunction]
#[pyo3(signature = (file_contents, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=false),
       text_signature = "(file_contents, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=False)")]
#[allow(clippy::too_many_arguments)]
pub fn upload_bytes(
    py: Python,
    file_contents: Vec<Vec<u8>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
    sha256s: Option<Vec<String>>,
    skip_sha256: bool,
) -> PyResult<Vec<PyXetUploadInfo>> {
    emit_deprecation(
        py,
        "hf_xet.upload_bytes() is deprecated. Use XetSession().new_upload_commit().start_upload_bytes() instead.",
    )?;

    if skip_sha256 && sha256s.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err("skip_sha256=True and sha256s are mutually exclusive"));
    }

    if let Some(ref s) = sha256s
        && s.len() != file_contents.len()
    {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
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

    let ctx = super::runtime::get_or_init_runtime().map_err(super::runtime::convert_multithreading_error)?;
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater
        .map(|f| WrappedProgressUpdater::new(f, ctx.clone()))
        .transpose()?
        .map(Arc::new);
    let header_map = legacy_headers(request_headers)?;
    let x: u64 = rand::rng().random();

    async_run(py, async move {
        debug!(
            "upload_bytes (legacy) call {x:x}: (PID = {}) Uploading {} files as bytes.",
            std::process::id(),
            file_contents.len(),
        );

        let out: Vec<PyXetUploadInfo> = data_client::upload_bytes_async(
            &ctx,
            file_contents,
            sha256_policies,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
            header_map,
        )
        .await
        .map_err(convert_xet_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();

        debug!("upload_bytes (legacy) call {x:x} finished.");
        PyResult::Ok(out)
    })
}

/// Upload files from disk to Xet storage.
///
/// .. deprecated::
///     Use :class:`XetSession` and :meth:`XetUploadCommit.start_upload_file` instead.
#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=false),
       text_signature = "(file_paths, endpoint, token_info, token_refresher, progress_updater, _repo_type, request_headers=None, sha256s=None, skip_sha256=False)")]
#[allow(clippy::too_many_arguments)]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
    _repo_type: Option<String>,
    request_headers: Option<HashMap<String, String>>,
    sha256s: Option<Vec<String>>,
    skip_sha256: bool,
) -> PyResult<Vec<PyXetUploadInfo>> {
    emit_deprecation(
        py,
        "hf_xet.upload_files() is deprecated. Use XetSession().new_upload_commit().start_upload_file() instead.",
    )?;

    if skip_sha256 && sha256s.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err("skip_sha256=True and sha256s are mutually exclusive"));
    }

    if let Some(ref s) = sha256s
        && s.len() != file_paths.len()
    {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
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

    let ctx = super::runtime::get_or_init_runtime().map_err(super::runtime::convert_multithreading_error)?;
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater
        .map(|f| WrappedProgressUpdater::new(f, ctx.clone()))
        .transpose()?
        .map(Arc::new);
    let header_map = legacy_headers(request_headers)?;
    let x: u64 = rand::rng().random();

    async_run(py, async move {
        debug!(
            "upload_files (legacy) call {x:x}: (PID = {}) Uploading {} files.",
            std::process::id(),
            file_paths.len(),
        );

        let out: Vec<PyXetUploadInfo> = data_client::upload_async(
            &ctx,
            file_paths,
            sha256_policies,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
            header_map,
        )
        .await
        .map_err(convert_xet_error)?
        .into_iter()
        .map(PyXetUploadInfo::from)
        .collect();

        debug!("upload_files (legacy) call {x:x} finished.");
        PyResult::Ok(out)
    })
}

/// Compute Xet hashes for files without uploading.
#[pyfunction]
#[pyo3(signature = (file_paths), text_signature = "(file_paths)")]
pub fn hash_files(py: Python, file_paths: Vec<String>) -> PyResult<Vec<PyXetUploadInfo>> {
    let ctx = super::runtime::get_or_init_runtime().map_err(super::runtime::convert_multithreading_error)?;

    async_run(py, async move {
        let out: Vec<PyXetUploadInfo> = data_client::hash_files_async(&ctx, file_paths)
            .await
            .map_err(convert_xet_error)?
            .into_iter()
            .map(PyXetUploadInfo::from)
            .collect();

        PyResult::Ok(out)
    })
}

/// Download files from Xet storage to local paths.
///
/// .. deprecated::
///     Use :class:`XetSession` and :meth:`XetFileDownloadGroup.start_download_file` instead.
#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater, request_headers=None),
       text_signature = "(files, endpoint, token_info, token_refresher, progress_updater, request_headers=None)")]
pub fn download_files(
    py: Python,
    files: Vec<PyXetDownloadInfo>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
    request_headers: Option<HashMap<String, String>>,
) -> PyResult<Vec<String>> {
    emit_deprecation(
        py,
        "hf_xet.download_files() is deprecated. Use XetSession().new_file_download_group().start_download_file() instead.",
    )?;

    let ctx = super::runtime::get_or_init_runtime().map_err(super::runtime::convert_multithreading_error)?;
    let file_infos: Vec<_> = files.into_iter().map(<(XetFileInfo, DestinationPath)>::from).collect();
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater
        .map(|fs| try_parse_progress_updaters(fs, ctx.clone()))
        .transpose()?;
    let header_map = legacy_headers(request_headers)?;
    let x: u64 = rand::rng().random();

    async_run(py, async move {
        debug!(
            "download_files (legacy) call {x:x}: (PID = {}) Downloading {} files.",
            std::process::id(),
            file_infos.len(),
        );

        let out = data_client::download_async(
            &ctx,
            file_infos,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updaters,
            header_map,
        )
        .await
        .map_err(convert_xet_error)?;

        debug!("download_files (legacy) call {x:x}: Completed.");
        PyResult::Ok(out)
    })
}

/// Force a SIGINT shutdown when it has been intercepted by another process.
///
/// .. deprecated::
///     Use :meth:`XetSession.abort` or :meth:`XetSession.sigint_abort` instead.
#[pyfunction]
pub fn force_sigint_shutdown(py: Python) -> PyResult<()> {
    emit_deprecation(py, "hf_xet.force_sigint_shutdown() is deprecated. Use XetSession.sigint_abort() instead.")?;
    super::runtime::perform_sigint_shutdown();
    Err(PyKeyboardInterrupt::new_err(()))
}

fn try_parse_progress_updaters(
    funcs: Vec<Py<PyAny>>,
    ctx: xet_runtime::core::XetContext,
) -> PyResult<Vec<Arc<dyn TrackingProgressUpdater>>> {
    let mut updaters = Vec::with_capacity(funcs.len());
    for func in funcs {
        updaters.push(Arc::new(WrappedProgressUpdater::new(func, ctx.clone())?) as Arc<dyn TrackingProgressUpdater>);
    }
    Ok(updaters)
}
