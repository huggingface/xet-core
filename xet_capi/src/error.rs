use std::ffi::{CString, c_char};
use std::os::raw::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Status returned by every fallible `xet_*` function.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum XetStatus {
    /// The call succeeded; any `out` pointers were filled in.
    XetOk = 0,
    /// The call failed for a reason not covered by a more specific variant.
    /// Check the accompanying `XetError` (if any) for details.
    XetErr = 1,
    /// A required argument was null, malformed, or otherwise invalid (e.g. a
    /// bad hash string, a handle that failed a null check).
    XetErrInvalidArg = 2,
    /// The call failed due to missing, expired, or rejected credentials.
    XetErrAuth = 3,
    /// The requested resource (file, hash, etc.) does not exist.
    XetErrNotFound = 4,
    /// The operation was cancelled before completing.
    XetErrCancelled = 5,
    /// A Rust panic was caught at the FFI boundary and converted into this
    /// status; no partial state should be assumed to be valid.
    XetErrPanic = 6,
}

/// Opaque error object. Read via [`xet_error_message`] / [`xet_error_code`];
/// free with [`xet_error_free`].
pub struct XetError {
    code: XetStatus,
    message: CString,
}

impl XetError {
    pub(crate) fn new(code: XetStatus, message: impl Into<Vec<u8>>) -> Self {
        let message = CString::new(message).unwrap_or_else(|_| CString::new("invalid error text").unwrap());
        Self { code, message }
    }

    pub(crate) fn from_xet(e: &xet::XetError) -> Self {
        use xet::XetError as E;
        let code = match e {
            E::Authentication(_) => XetStatus::XetErrAuth,
            E::NotFound(_) => XetStatus::XetErrNotFound,
            E::UserCancelled(_) | E::Cancelled(_) | E::KeyboardInterrupt => XetStatus::XetErrCancelled,
            E::Configuration(_) => XetStatus::XetErrInvalidArg,
            _ => XetStatus::XetErr,
        };
        Self::new(code, e.to_string())
    }
}

/// Write `e` to `*err` if `err` is non-null, then return `e.code`.
pub(crate) fn set_err(err: *mut *mut XetError, e: XetError) -> XetStatus {
    let code = e.code;
    if !err.is_null() {
        unsafe { *err = Box::into_raw(Box::new(e)) };
    }
    code
}

/// Convenience: map a `xet::XetError` into `*err` and return its status.
pub(crate) fn set_xet_err(err: *mut *mut XetError, e: &xet::XetError) -> XetStatus {
    set_err(err, XetError::from_xet(e))
}

/// Run `f`, converting any panic into `XET_ERR_PANIC` written to `*err`.
pub(crate) fn ffi_guard<F: FnOnce() -> XetStatus>(err: *mut *mut XetError, f: F) -> XetStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(status) => status,
        Err(_) => set_err(err, XetError::new(XetStatus::XetErrPanic, "panic in xet_capi")),
    }
}

/// # Safety
/// `err` must be null or a valid pointer to a live `XetError` produced by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_error_code(err: *const XetError) -> XetStatus {
    if err.is_null() {
        return XetStatus::XetErrInvalidArg;
    }
    unsafe { (*err).code }
}

/// # Safety
/// `err` must be null or a valid pointer to a live `XetError` produced by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_error_message(err: *const XetError) -> *const c_char {
    if err.is_null() {
        return std::ptr::null();
    }
    unsafe { (*err).message.as_ptr() }
}

/// # Safety
/// `err` must be null or a pointer previously returned by this crate that has not
/// already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_error_free(err: *mut XetError) {
    if !err.is_null() {
        drop(unsafe { Box::from_raw(err) });
    }
}

/// Test-only constructor used by ffi_tests.
///
/// # Safety
/// `out` must be a valid, non-null pointer to a writable `*mut XetError`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_test_make_auth_error(out: *mut *mut XetError) -> c_int {
    let e = XetError::from_xet(&xet::XetError::Authentication("nope".into()));
    unsafe { *out = Box::into_raw(Box::new(e)) };
    0
}
