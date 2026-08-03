use xet::xet_session::XetStreamUpload as InnerStreamUpload;

use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::handle::{free_handle, into_handle};
use crate::upload::XetFileMetadataHandle;

/// A streaming upload handle. Feed data with [`xet_stream_upload_write`], then
/// [`xet_stream_upload_finish`]. Free with [`xet_stream_upload_free`].
pub struct XetStreamUpload {
    inner: InnerStreamUpload,
}
impl XetStreamUpload {
    pub(crate) fn new(inner: InnerStreamUpload) -> Self {
        Self { inner }
    }
}

/// Write `len` bytes from `data` into the stream, blocking until the write is
/// ingested.
///
/// # Safety
/// `su` valid; `data`/`len` a valid buffer (data may be null iff len==0);
/// `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_stream_upload_write(
    su: *const XetStreamUpload,
    data: *const u8,
    len: usize,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(su) = (unsafe { su.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null stream upload"));
        };
        if data.is_null() && len != 0 {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null data"));
        }
        let chunk = if len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        match su.inner.write_blocking(chunk) {
            Ok(_) => XetStatus::XetOk,
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Finalize the stream, blocking until complete. On success fills `*out` with
/// an owned `XetFileMetadataHandle` (free with `xet_file_metadata_free`).
///
/// Progress can be observed via `xet_upload_commit_progress` on the owning
/// commit.
///
/// # Safety
/// `su` valid; `out` non-null; `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_stream_upload_finish(
    su: *const XetStreamUpload,
    out: *mut *mut XetFileMetadataHandle,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(su) = (unsafe { su.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null stream upload"));
        };
        match su.inner.finish_blocking() {
            Ok(meta) => {
                if !out.is_null() {
                    unsafe { *out = into_handle(XetFileMetadataHandle::owned(meta)) };
                }
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Free a `XetStreamUpload`. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_stream_upload_free(su: *mut XetStreamUpload) {
    free_handle(su);
}
