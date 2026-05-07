//! napi smoke-test binding for `hf-xet` (the `xet` crate at `xet_pkg/`).
//!
//! Exposes:
//!   - `initLogging(version)`  — install xet's tracing subscriber
//!   - `smokeTest()`           — build a `XetSession` and runtime helpers
//!                               without doing any I/O
//!   - `downloadFile(opts)`    — actually download a Xet-stored file from
//!                               the HuggingFace Hub to a local path
//!
//! `downloadFile` is intentionally synchronous: it blocks the caller until the
//! download completes, internally using `xet`'s `*_blocking` APIs which run on
//! xet-runtime's own tokio runtime. Calling it from JS will block the libuv
//! main thread for the duration — fine for a smoke test, but a real binding
//! should wrap this in `napi::Task` / `tokio::task::spawn_blocking` so the JS
//! event loop stays responsive.

use napi::Error as NapiError;
use napi::Status;
use napi_derive::napi;
use xet::xet_session::{HeaderMap, HeaderValue, XetFileInfo, XetSessionBuilder, header};

fn to_napi_err<E: std::fmt::Display>(e: E) -> NapiError {
    NapiError::new(Status::GenericFailure, e.to_string())
}

#[napi(js_name = "initLogging")]
pub fn init_logging(version: String) {
    xet::init_logging(version);
}

#[napi(js_name = "smokeTest")]
pub fn smoke_test() -> Result<String, NapiError> {
    let session = XetSessionBuilder::new().build().map_err(to_napi_err)?;
    let _upload = session.new_upload_commit().map_err(to_napi_err)?;
    let _download = session.new_file_download_group().map_err(to_napi_err)?;
    Ok("xet session built; runtime initialized".to_string())
}

/// Options for [`downloadFile`].
///
/// `xetHash` and `fileSize` come from the HuggingFace Hub's
/// `X-Xet-Hash` and `X-Linked-Size` response headers (issue a HEAD against
/// the `/{repo}/resolve/{ref}/{filename}` URL with a `User-Agent` to see them
/// — Cloudfront strips them on cache hits without a UA hint).
#[napi(object, js_name = "DownloadFileOptions")]
pub struct DownloadFileOptions {
    /// The HuggingFace Hub's xet-read-token endpoint, e.g.
    /// `https://huggingface.co/api/models/{repo}/xet-read-token/{ref}`.
    pub token_refresh_url: String,
    /// Optional bearer token for the refresh endpoint. Required for private
    /// repos; for public repos this can be `null`.
    pub auth_token: Option<String>,
    /// The xet content hash (hex string) of the file to download.
    pub xet_hash: String,
    /// The file's size in bytes. JS `number` is precise up to 2^53; HF files
    /// are well under that.
    pub file_size: i64,
    /// Optional SHA-256 (hex) used by xet to verify the download.
    pub sha256: Option<String>,
    /// Local filesystem destination for the downloaded file.
    pub dest_path: String,
}

/// Result of a successful [`downloadFile`] call.
#[napi(object, js_name = "DownloadFileResult")]
pub struct DownloadFileResult {
    pub dest_path: String,
    pub bytes_downloaded: i64,
}

#[napi(js_name = "downloadFile")]
pub fn download_file(opts: DownloadFileOptions) -> Result<DownloadFileResult, NapiError> {
    let mut headers = HeaderMap::new();
    if let Some(token) = opts.auth_token.as_deref() {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| NapiError::new(Status::InvalidArg, format!("invalid auth token: {e}")))?;
        headers.insert(header::AUTHORIZATION, value);
    }

    let file_size: u64 = opts
        .file_size
        .try_into()
        .map_err(|_| NapiError::new(Status::InvalidArg, "fileSize must be non-negative"))?;
    let file_info = match opts.sha256 {
        Some(sha) => XetFileInfo::new_with_sha256(opts.xet_hash, file_size, sha),
        None => XetFileInfo::new(opts.xet_hash, file_size),
    };

    let session = XetSessionBuilder::new().build().map_err(to_napi_err)?;
    let group = session
        .new_file_download_group()
        .map_err(to_napi_err)?
        .with_token_refresh_url(opts.token_refresh_url, headers)
        .build_blocking()
        .map_err(to_napi_err)?;

    let dest_path = std::path::PathBuf::from(&opts.dest_path);
    group
        .download_file_to_path_blocking(file_info, dest_path.clone())
        .map_err(to_napi_err)?;
    let report = group.finish_blocking().map_err(to_napi_err)?;

    let bytes_downloaded: i64 = report
        .progress
        .total_bytes_completed
        .try_into()
        .unwrap_or(i64::MAX);

    Ok(DownloadFileResult {
        dest_path: opts.dest_path,
        bytes_downloaded,
    })
}
