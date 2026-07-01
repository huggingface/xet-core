use std::ffi::{CStr, c_char};

use xet::xet_session::{HeaderMap, HeaderValue, XetSession as InnerSession, XetSessionBuilder, header};

use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::handle::{free_handle, into_handle};

/// A Xet session. Owns no auth; produces per-commit / per-group auth via the
/// `xet_session_new_*` builders (added in a later task). Free with
/// [`xet_session_free`].
pub struct XetSession {
    pub(crate) inner: InnerSession,
}

/// One HTTP header for token-refresh requests.
#[repr(C)]
pub struct XetHeader {
    pub key: *const c_char,
    pub value: *const c_char,
}

/// Per-commit / per-group auth configuration. All pointers are borrowed for
/// the duration of the call. Any nullable field may be NULL.
#[repr(C)]
pub struct XetAuthConfig {
    pub endpoint: *const c_char,
    pub token: *const c_char,
    pub token_expiry: i64,
    pub token_refresh_url: *const c_char,
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

/// Install xet's tracing subscriber. `version` may be NULL.
///
/// # Safety
/// `version` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_init_logging(version: *const c_char) {
    let v = unsafe { opt_str(version) }.ok().flatten().unwrap_or("xet_capi").to_string();
    xet::init_logging(v);
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

#[unsafe(no_mangle)]
pub extern "C" fn xet_session_free(session: *mut XetSession) {
    free_handle(session);
}
