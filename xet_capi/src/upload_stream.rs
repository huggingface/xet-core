use xet::xet_session::XetStreamUpload as InnerStreamUpload;

use crate::error::{XetError, XetStatus, ffi_guard, set_err};
use crate::handle::free_handle;
use crate::op::{OpOutput, XetOp, spawn_op};

/// A streaming upload handle. Feed data with [`xet_stream_upload_write_start`],
/// then [`xet_stream_upload_finish_start`]. Free with [`xet_stream_upload_free`].
pub struct XetStreamUpload {
    inner: InnerStreamUpload,
}
impl XetStreamUpload {
    pub(crate) fn new(inner: InnerStreamUpload) -> Self {
        Self { inner }
    }
}

/// Start an async write of `len` bytes from `data`. Poll the returned op; it
/// yields void on success.
///
/// # Safety
/// `su` valid; `data`/`len` a valid buffer (data may be null iff len==0);
/// `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_stream_upload_write_start(
    su: *const XetStreamUpload,
    data: *const u8,
    len: usize,
    out: *mut *mut XetOp,
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
        let handle = su.inner.clone();
        unsafe { *out = spawn_op(move || handle.write_blocking(chunk).map(|_| OpOutput::Void)) };
        XetStatus::XetOk
    })
}

/// Start finalizing the stream. Poll the returned op; it yields file metadata.
///
/// # Safety
/// `su` valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_stream_upload_finish_start(
    su: *const XetStreamUpload,
    out: *mut *mut XetOp,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(su) = (unsafe { su.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null stream upload"));
        };
        let handle = su.inner.clone();
        unsafe { *out = spawn_op(move || handle.finish_blocking().map(OpOutput::FileMetadata)) };
        XetStatus::XetOk
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_stream_upload_free(su: *mut XetStreamUpload) {
    free_handle(su);
}
