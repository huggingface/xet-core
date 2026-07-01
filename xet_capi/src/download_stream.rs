use std::sync::{Arc, Mutex};

use xet::xet_session::{
    XetDownloadStream as InnerStream, XetDownloadStreamGroup as InnerGroup,
    XetUnorderedDownloadStream as InnerUnordered,
};

use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::file_info::XetFileInfo;
use crate::handle::{free_handle, into_handle};
use crate::op::{OpOutput, XetOp, spawn_op};
use crate::reports::XetProgress;

/// A stream-download group. Free with [`xet_download_stream_group_free`].
pub struct XetDownloadStreamGroup {
    inner: InnerGroup,
}
impl XetDownloadStreamGroup {
    pub(crate) fn new(inner: InnerGroup) -> Self {
        Self { inner }
    }
}

/// An active download stream (ordered or unordered). `next` requires `&mut`, so
/// the inner stream is guarded by a mutex to keep the handle usable across the
/// op worker thread. Free with [`xet_download_stream_free`].
pub struct XetDownloadStream {
    ordered: Option<Arc<Mutex<InnerStream>>>,
    unordered: Option<Arc<Mutex<InnerUnordered>>>,
}

/// # Safety
/// `group`/`file_info` valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_group_download_stream(
    group: *const XetDownloadStreamGroup,
    file_info: *const XetFileInfo,
    has_range: bool,
    range_start: u64,
    range_end: u64,
    out: *mut *mut XetDownloadStream,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(group), Some(fi)) = (unsafe { group.as_ref() }, unsafe { file_info.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null group or file_info"));
        };
        let range = if has_range { Some(range_start..range_end) } else { None };
        match group.inner.download_stream_blocking(fi.inner.clone(), range) {
            Ok(s) => {
                unsafe {
                    *out = into_handle(XetDownloadStream {
                        ordered: Some(Arc::new(Mutex::new(s))),
                        unordered: None,
                    })
                };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// # Safety
/// `group`/`file_info` valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_group_download_unordered_stream(
    group: *const XetDownloadStreamGroup,
    file_info: *const XetFileInfo,
    has_range: bool,
    range_start: u64,
    range_end: u64,
    out: *mut *mut XetDownloadStream,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(group), Some(fi)) = (unsafe { group.as_ref() }, unsafe { file_info.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null group or file_info"));
        };
        let range = if has_range { Some(range_start..range_end) } else { None };
        match group.inner.download_unordered_stream_blocking(fi.inner.clone(), range) {
            Ok(s) => {
                unsafe {
                    *out = into_handle(XetDownloadStream {
                        ordered: None,
                        unordered: Some(Arc::new(Mutex::new(s))),
                    })
                };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Start fetching the next chunk. Poll the returned op:
/// - ordered streams: consume with `xet_op_take_bytes` (NULL = EOF).
/// - unordered streams: consume with `xet_op_take_chunk` (NULL = EOF).
///
/// # Safety
/// `stream`/`out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_next_start(
    stream: *const XetDownloadStream,
    out: *mut *mut XetOp,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(stream) = (unsafe { stream.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null stream"));
        };
        if let Some(ordered) = &stream.ordered {
            let s = Arc::clone(ordered);
            unsafe {
                *out = spawn_op(move || {
                    let mut guard = s.lock().unwrap();
                    guard.blocking_next().map(OpOutput::Bytes)
                })
            };
        } else if let Some(unordered) = &stream.unordered {
            let s = Arc::clone(unordered);
            unsafe {
                *out = spawn_op(move || {
                    let mut guard = s.lock().unwrap();
                    guard.blocking_next().map(OpOutput::Chunk)
                })
            };
        } else {
            return set_err(err, XetError::new(XetStatus::XetErr, "empty stream handle"));
        }
        XetStatus::XetOk
    })
}

/// # Safety
/// `stream` valid; `out` a valid pointer to a `XetProgress`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_progress(
    stream: *const XetDownloadStream,
    out: *mut XetProgress,
) -> XetStatus {
    let (Some(stream), false) = (unsafe { stream.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    let item = if let Some(o) = &stream.ordered {
        o.lock().unwrap().progress()
    } else if let Some(u) = &stream.unordered {
        u.lock().unwrap().progress()
    } else {
        None
    };
    match item {
        Some(p) => {
            unsafe { *out = XetProgress::from_item(&p) };
            XetStatus::XetOk
        },
        None => XetStatus::XetErr,
    }
}

/// # Safety
/// `stream` must be null or a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_cancel(stream: *const XetDownloadStream) {
    if let Some(stream) = unsafe { stream.as_ref() } {
        if let Some(o) = &stream.ordered {
            o.lock().unwrap().cancel();
        } else if let Some(u) = &stream.unordered {
            u.lock().unwrap().cancel();
        }
    }
}

/// Returns the stream's task id, or 0 if `stream` is null.
///
/// # Safety
/// `stream` must be null or a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_task_id(stream: *const XetDownloadStream) -> u64 {
    match unsafe { stream.as_ref() } {
        Some(s) => {
            if let Some(o) = &s.ordered {
                o.lock().unwrap().task_id().0
            } else if let Some(u) = &s.unordered {
                u.lock().unwrap().task_id().0
            } else {
                0
            }
        },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_download_stream_group_free(group: *mut XetDownloadStreamGroup) {
    free_handle(group);
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_download_stream_free(stream: *mut XetDownloadStream) {
    free_handle(stream);
}
