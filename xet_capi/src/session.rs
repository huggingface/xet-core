use std::ffi::{CStr, c_char};

use xet::xet_session::{HeaderMap, HeaderValue, XetSession as InnerSession, XetSessionBuilder, header};

use crate::download_file::XetFileDownloadGroup;
use crate::download_stream::XetDownloadStreamGroup;
use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::handle::{free_handle, into_handle};
use crate::upload::XetUploadCommit;

/// A Xet session: the root handle from which upload commits and download
/// groups are created via the `xet_session_new_*` functions. Holds no auth
/// itself; each commit/group gets its own from a `XetAuthConfig`. Free with
/// [`xet_session_free`].
pub struct XetSession {
    pub(crate) inner: InnerSession,
}

/// One HTTP header for token-refresh requests. Both pointers are borrowed
/// NUL-terminated C strings, valid only for the duration of the call that
/// takes an array of `XetHeader`s (e.g. via `XetAuthConfig::refresh_headers`).
#[repr(C)]
pub struct XetHeader {
    /// Header name, e.g. `"Authorization"`.
    pub key: *const c_char,
    pub value: *const c_char,
}

/// Per-commit / per-group auth configuration. All pointers are borrowed for
/// the duration of the call. Any nullable field may be NULL.
#[repr(C)]
pub struct XetAuthConfig {
    /// Hub endpoint URL, e.g. `"https://huggingface.co"`. Must not be NULL.
    pub endpoint: *const c_char,
    /// Bearer token used for CAS/Hub requests. Must not be NULL.
    pub token: *const c_char,
    /// Unix timestamp (seconds) at which `token` expires, or `0`/negative if
    /// it never needs refreshing.
    pub token_expiry: i64,
    /// URL to POST to for a token refresh. Refreshing is enabled only when
    /// this is non-NULL and `refresh_headers` has at least one entry.
    pub token_refresh_url: *const c_char,
    /// Borrowed array of `refresh_header_count` headers sent with each
    /// refresh request. May be NULL iff `refresh_header_count == 0`.
    pub refresh_headers: *const XetHeader,
    pub refresh_header_count: usize,
}

/// Borrow a `*const c_char` as `&str`, mapping NUL/invalid-UTF-8 to None.
///
/// # Safety
/// `p` must be null or a valid NUL-terminated C string valid for the call.
pub(crate) unsafe fn opt_str<'a>(p: *const c_char) -> Result<Option<&'a str>, ()> {
    if p.is_null() {
        return Ok(None);
    }
    unsafe { CStr::from_ptr(p) }.to_str().map(Some).map_err(|_| ())
}

/// Like [`opt_str`] but requires a non-null valid string.
///
/// # Safety
/// See [`opt_str`].
pub(crate) unsafe fn req_str<'a>(p: *const c_char) -> Result<&'a str, ()> {
    unsafe { opt_str(p) }?.ok_or(())
}

/// Build a token-refresh HeaderMap from the config, or None if no refresh URL.
pub(crate) fn build_refresh_headers(cfg: &XetAuthConfig) -> Result<Option<HeaderMap>, XetError> {
    if cfg.token_refresh_url.is_null() || cfg.refresh_headers.is_null() || cfg.refresh_header_count == 0 {
        return Ok(None);
    }
    let slice = unsafe { std::slice::from_raw_parts(cfg.refresh_headers, cfg.refresh_header_count) };
    let mut map = HeaderMap::new();
    for h in slice {
        let key =
            unsafe { req_str(h.key) }.map_err(|_| XetError::new(XetStatus::XetErrInvalidArg, "invalid header key"))?;
        let value = unsafe { req_str(h.value) }
            .map_err(|_| XetError::new(XetStatus::XetErrInvalidArg, "invalid header value"))?;
        let name = header::HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| XetError::new(XetStatus::XetErrInvalidArg, "invalid header name"))?;
        let val = HeaderValue::from_str(value)
            .map_err(|_| XetError::new(XetStatus::XetErrInvalidArg, "invalid header value"))?;
        map.insert(name, val);
    }
    Ok(Some(map))
}

/// Create a new session. Writes the handle to `*out`.
///
/// # Safety
/// `out` must be a valid pointer to write the handle to; `err` may be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_session_new(out: *mut *mut XetSession, err: *mut *mut XetError) -> XetStatus {
    ffi_guard(err, || {
        if out.is_null() {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "out is null"));
        }
        match XetSessionBuilder::new().build() {
            Ok(inner) => {
                unsafe { *out = into_handle(XetSession { inner }) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Free a `XetSession`. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_session_free(session: *mut XetSession) {
    free_handle(session);
}

/// Build an upload commit with per-commit auth from `cfg`.
///
/// # Safety
/// `session`/`cfg` valid handles/pointers; `out` non-null; `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_session_new_upload_commit(
    session: *const XetSession,
    cfg: *const XetAuthConfig,
    out: *mut *mut XetUploadCommit,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(session), Some(cfg)) = (unsafe { session.as_ref() }, unsafe { cfg.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null session or cfg"));
        };
        let mut builder = match session.inner.new_upload_commit() {
            Ok(b) => b,
            Err(e) => return set_xet_err(err, &e),
        };
        if let Some(ep) = unsafe { opt_str(cfg.endpoint) }.ok().flatten() {
            builder = builder.with_endpoint(ep);
        }
        if let Some(tok) = unsafe { opt_str(cfg.token) }.ok().flatten() {
            builder = builder.with_token_info(tok, u64::try_from(cfg.token_expiry).unwrap_or(0));
        }
        match build_refresh_headers(cfg) {
            Ok(Some(headers)) => {
                let url = unsafe { opt_str(cfg.token_refresh_url) }
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_string();
                builder = builder.with_token_refresh_url(url, headers);
            },
            Ok(None) => {},
            Err(e) => return set_err(err, e),
        }
        match builder.build_blocking() {
            Ok(commit) => {
                unsafe { *out = into_handle(XetUploadCommit::new(commit)) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Build a file-download group with per-group auth from `cfg`.
///
/// # Safety
/// `session`/`cfg` valid; `out` non-null; `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_session_new_file_download_group(
    session: *const XetSession,
    cfg: *const XetAuthConfig,
    out: *mut *mut XetFileDownloadGroup,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(session), Some(cfg)) = (unsafe { session.as_ref() }, unsafe { cfg.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null session or cfg"));
        };
        let mut builder = match session.inner.new_file_download_group() {
            Ok(b) => b,
            Err(e) => return set_xet_err(err, &e),
        };
        if let Some(ep) = unsafe { opt_str(cfg.endpoint) }.ok().flatten() {
            builder = builder.with_endpoint(ep);
        }
        if let Some(tok) = unsafe { opt_str(cfg.token) }.ok().flatten() {
            builder = builder.with_token_info(tok, u64::try_from(cfg.token_expiry).unwrap_or(0));
        }
        match build_refresh_headers(cfg) {
            Ok(Some(headers)) => {
                let url = unsafe { opt_str(cfg.token_refresh_url) }
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_string();
                builder = builder.with_token_refresh_url(url, headers);
            },
            Ok(None) => {},
            Err(e) => return set_err(err, e),
        }
        match builder.build_blocking() {
            Ok(group) => {
                unsafe { *out = into_handle(XetFileDownloadGroup::new(group)) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Build a download-stream group with per-group auth from `cfg`.
///
/// # Safety
/// `session`/`cfg` valid; `out` non-null; `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_session_new_download_stream_group(
    session: *const XetSession,
    cfg: *const XetAuthConfig,
    out: *mut *mut XetDownloadStreamGroup,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(session), Some(cfg)) = (unsafe { session.as_ref() }, unsafe { cfg.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null session or cfg"));
        };
        let mut builder = match session.inner.new_download_stream_group() {
            Ok(b) => b,
            Err(e) => return set_xet_err(err, &e),
        };
        if let Some(ep) = unsafe { opt_str(cfg.endpoint) }.ok().flatten() {
            builder = builder.with_endpoint(ep);
        }
        if let Some(tok) = unsafe { opt_str(cfg.token) }.ok().flatten() {
            builder = builder.with_token_info(tok, u64::try_from(cfg.token_expiry).unwrap_or(0));
        }
        match build_refresh_headers(cfg) {
            Ok(Some(headers)) => {
                let url = unsafe { opt_str(cfg.token_refresh_url) }
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_string();
                builder = builder.with_token_refresh_url(url, headers);
            },
            Ok(None) => {},
            Err(e) => return set_err(err, e),
        }
        match builder.build_blocking() {
            Ok(group) => {
                unsafe { *out = into_handle(XetDownloadStreamGroup::new(group)) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}
