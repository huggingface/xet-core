use std::ffi::c_char;

use xet::xet_session::{Sha256Policy, XetFileInfo as InnerFileInfo};

use crate::error::{XetError, XetStatus, ffi_guard, set_err};
use crate::handle::{free_handle, into_handle};
use crate::session::req_str;

/// File descriptor for downloads. Free with [`xet_file_info_free`].
pub struct XetFileInfo {
    pub(crate) inner: InnerFileInfo,
}

/// SHA-256 handling for uploads.
#[repr(C)]
#[derive(Clone, Copy)]
pub enum XetSha256Policy {
    /// Compute the SHA-256 from the upload data as it is read.
    XetSha256Compute = 0,
    /// Skip SHA-256 entirely; the resulting metadata will have no SHA-256.
    XetSha256Skip = 1,
    /// Use a caller-supplied SHA-256 (hex) instead of computing one. The hex
    /// string is passed via the `provided_sha256` parameter of the calling
    /// function and is not validated against the actual upload contents.
    XetSha256Provided = 2,
}

/// Map the C policy + optional provided hex into `Sha256Policy`.
///
/// # Safety
/// `provided` must be null or a valid NUL-terminated C string (only read when
/// `policy == XetSha256Provided`).
pub(crate) unsafe fn sha256_policy(policy: XetSha256Policy, provided: *const c_char) -> Result<Sha256Policy, XetError> {
    match policy {
        XetSha256Policy::XetSha256Compute => Ok(Sha256Policy::Compute),
        XetSha256Policy::XetSha256Skip => Ok(Sha256Policy::Skip),
        XetSha256Policy::XetSha256Provided => {
            let hex = unsafe { req_str(provided) }
                .map_err(|_| XetError::new(XetStatus::XetErrInvalidArg, "provided sha256 is null/invalid"))?;
            match Sha256Policy::from_hex(hex) {
                p @ Sha256Policy::Provided(_) => Ok(p),
                _ => Err(XetError::new(XetStatus::XetErrInvalidArg, "provided sha256 is not valid hex")),
            }
        },
    }
}

/// Create a file descriptor from a xet content hash (hex) and size.
///
/// # Safety
/// `hash` must be a valid NUL-terminated C string; `out` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_info_new(
    hash: *const c_char,
    file_size: u64,
    out: *mut *mut XetFileInfo,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Ok(hash) = (unsafe { req_str(hash) }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "invalid hash"));
        };
        unsafe {
            *out = into_handle(XetFileInfo {
                inner: InnerFileInfo::new(hash.to_string(), file_size),
            })
        };
        XetStatus::XetOk
    })
}

/// Like [`xet_file_info_new`] but with a known SHA-256 (hex).
///
/// # Safety
/// `hash` and `sha256` must be valid NUL-terminated C strings; `out` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_info_new_with_sha256(
    hash: *const c_char,
    file_size: u64,
    sha256: *const c_char,
    out: *mut *mut XetFileInfo,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Ok(hash), Ok(sha)) = (unsafe { req_str(hash) }, unsafe { req_str(sha256) }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "invalid hash or sha256"));
        };
        let inner = InnerFileInfo::new_with_sha256(hash.to_string(), file_size, sha.to_string());
        unsafe { *out = into_handle(XetFileInfo { inner }) };
        XetStatus::XetOk
    })
}

/// Free a `XetFileInfo`. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_file_info_free(fi: *mut XetFileInfo) {
    free_handle(fi);
}
