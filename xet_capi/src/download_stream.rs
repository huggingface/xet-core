use std::sync::{Arc, Mutex};

use xet::xet_session::{
    XetDownloadStream as InnerStream, XetDownloadStreamGroup as InnerGroup,
    XetUnorderedDownloadStream as InnerUnordered,
};

use crate::bytes::XetBytes;
use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::file_info::XetFileInfo;
use crate::handle::{free_handle, into_handle};
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

/// An active download stream (ordered or unordered). `next` requires `&mut`,
/// so the inner stream is guarded by a mutex; calls on the handle from other
/// threads (progress/cancel) wait for the in-flight `next` to return. Free
/// with [`xet_download_stream_free`].
pub struct XetDownloadStream {
    ordered: Option<Arc<Mutex<InnerStream>>>,
    unordered: Option<Arc<Mutex<InnerUnordered>>>,
}

/// Open an ordered download stream for `file_info`; chunks are returned in
/// file order via `xet_download_stream_next`. If `has_range`, only bytes
/// `[range_start, range_end)` are streamed. Free with
/// `xet_download_stream_free`.
///
/// # Safety
/// `group`/`file_info` valid; `out` non-null; `err` null or valid.
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

/// Like [`xet_download_stream_group_download_stream`] but chunks may be
/// delivered out of order (each `next` also reports the chunk's offset),
/// allowing higher throughput.
///
/// # Safety
/// `group`/`file_info` valid; `out` non-null; `err` null or valid.
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

/// Fetch the next chunk, blocking until it is available. On success `*out` is
/// a `XetBytes*` (free with `xet_bytes_free`), or NULL at end of stream.
///
/// - Ordered streams: chunks arrive in file order; `*offset` is not written (`offset` may be NULL).
/// - Unordered streams: `*offset` receives the chunk's byte offset in the file.
///
/// Blocks the calling thread until the chunk arrives. The stream handle is
/// internally serialized: [`xet_download_stream_progress`] and
/// [`xet_download_stream_cancel`] called from other threads will wait until
/// the in-flight `xet_download_stream_next` returns.
///
/// # Safety
/// `stream` valid; `offset` null or a valid writable pointer; `out` non-null;
/// `err` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_stream_next(
    stream: *const XetDownloadStream,
    offset: *mut u64,
    out: *mut *mut XetBytes,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(stream) = (unsafe { stream.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null stream"));
        };
        if let Some(ordered) = &stream.ordered {
            match ordered.lock().unwrap().blocking_next() {
                Ok(opt) => {
                    if !out.is_null() {
                        unsafe { *out = opt.map_or(std::ptr::null_mut(), |b| into_handle(XetBytes::new(b))) };
                    }
                    XetStatus::XetOk
                },
                Err(e) => set_xet_err(err, &e),
            }
        } else if let Some(unordered) = &stream.unordered {
            match unordered.lock().unwrap().blocking_next() {
                Ok(Some((off, b))) => {
                    if !offset.is_null() {
                        unsafe { *offset = off };
                    }
                    if !out.is_null() {
                        unsafe { *out = into_handle(XetBytes::new(b)) };
                    }
                    XetStatus::XetOk
                },
                Ok(None) => {
                    if !out.is_null() {
                        unsafe { *out = std::ptr::null_mut() };
                    }
                    XetStatus::XetOk
                },
                Err(e) => set_xet_err(err, &e),
            }
        } else {
            set_err(err, XetError::new(XetStatus::XetErr, "empty stream handle"))
        }
    })
}

/// Snapshot the stream's progress into `*out`. Returns `XetErr` if no
/// progress is available yet. Waits for any in-flight
/// `xet_download_stream_next` on the same stream to return first.
///
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

/// Cancel the stream; subsequent `xet_download_stream_next` calls return a
/// cancelled error. Waits for any in-flight `next` on the same stream to
/// return first.
///
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

/// Free a `XetDownloadStreamGroup`. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_download_stream_group_free(group: *mut XetDownloadStreamGroup) {
    free_handle(group);
}

/// Free a `XetDownloadStream`. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_download_stream_free(stream: *mut XetDownloadStream) {
    free_handle(stream);
}
