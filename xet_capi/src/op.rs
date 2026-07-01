use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use bytes::Bytes;

use crate::bytes::XetBytes;
use crate::error::{XetError, XetStatus, set_err, set_xet_err};
use crate::handle::{free_handle, into_handle};
// TODO(Task 11): re-enable once reports.rs / upload.rs exist
// use crate::reports::{XetCommitReportHandle, XetDownloadGroupReportHandle};
// use crate::upload::XetFileMetadataHandle;

/// Result payload of a completed operation. Internal; extracted via the typed
/// `xet_op_take_*` functions which validate the variant.
pub(crate) enum OpOutput {
    Void,
    FileMetadata(xet::xet_session::XetFileMetadata),
    CommitReport(xet::xet_session::XetCommitReport),
    DownloadReport(xet::xet_session::XetDownloadGroupReport),
    /// `None` means the ordered stream reached EOF.
    Bytes(Option<Bytes>),
    /// `None` means the unordered stream reached EOF.
    Chunk(Option<(u64, Bytes)>),
}

struct OpState {
    done: AtomicBool,
    slot: Mutex<Option<Result<OpOutput, xet::XetError>>>,
}

/// A spawned, poll-able operation. Poll with [`xet_op_poll`], then consume with
/// the matching `xet_op_take_*`. Free an un-taken op with [`xet_op_free`].
pub struct XetOp {
    state: Arc<OpState>,
    join: Option<JoinHandle<()>>,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum XetPollState {
    XetPollPending = 0,
    XetPollReady = 1,
    XetPollError = 2,
}

/// Spawn `f` on an OS thread; its result becomes the op's payload. `f` runs the
/// public `_blocking` variant, which internally drives xet-runtime.
pub(crate) fn spawn_op<F>(f: F) -> *mut XetOp
where
    F: FnOnce() -> Result<OpOutput, xet::XetError> + Send + 'static,
{
    let state = Arc::new(OpState {
        done: AtomicBool::new(false),
        slot: Mutex::new(None),
    });
    let worker = Arc::clone(&state);
    let join = std::thread::spawn(move || {
        let result = f();
        *worker.slot.lock().unwrap() = Some(result);
        worker.done.store(true, Ordering::Release);
    });
    into_handle(XetOp {
        state,
        join: Some(join),
    })
}

fn take_output(op: *mut XetOp) -> Option<Result<OpOutput, xet::XetError>> {
    let op = unsafe { op.as_mut() }?;
    if let Some(join) = op.join.take() {
        let _ = join.join();
    }
    op.state.slot.lock().unwrap().take()
}

/// # Safety
/// `op` must be null or a valid pointer to a live `XetOp` produced by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_poll(op: *const XetOp) -> XetPollState {
    let Some(op) = (unsafe { op.as_ref() }) else {
        return XetPollState::XetPollError;
    };
    if !op.state.done.load(Ordering::Acquire) {
        return XetPollState::XetPollPending;
    }
    match &*op.state.slot.lock().unwrap() {
        Some(Ok(_)) => XetPollState::XetPollReady,
        _ => XetPollState::XetPollError,
    }
}

/// # Safety
/// `op` must be null or a pointer previously returned by this crate that has not
/// already been freed or consumed by a `xet_op_take_*` call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_free(op: *mut XetOp) {
    if let Some(o) = unsafe { op.as_mut() }
        && let Some(join) = o.join.take()
    {
        let _ = join.join();
    }
    free_handle(op);
}

/// Extract a `XetError` from a failed op. Returns `XET_OK` and fills `*err`;
/// returns `XET_ERR_INVALID_ARG` if the op did not fail.
///
/// # Safety
/// `op` must be a valid pointer to a live `XetOp`; `err` must be null or a valid
/// writable `*mut XetError`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_take_error(op: *mut XetOp, err: *mut *mut XetError) -> XetStatus {
    match take_output(op) {
        Some(Err(e)) => {
            free_handle(op);
            set_err(err, XetError::from_xet(&e));
            XetStatus::XetOk
        },
        _ => XetStatus::XetErrInvalidArg,
    }
}

/// Macro: define a typed take-fn that expects a specific `OpOutput` variant.
macro_rules! take_variant {
    ($name:ident, $variant:path, $out_ty:ty, $wrap:expr) => {
        /// # Safety
        /// `op` must be a valid pointer to a live `XetOp`; `out` and `err` must be
        /// null or valid writable out-pointers.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(op: *mut XetOp, out: *mut *mut $out_ty, err: *mut *mut XetError) -> XetStatus {
            match take_output(op) {
                Some(Ok($variant(v))) => {
                    if !out.is_null() {
                        unsafe { *out = into_handle($wrap(v)) };
                    }
                    free_handle(op);
                    XetStatus::XetOk
                },
                Some(Err(e)) => {
                    free_handle(op);
                    set_xet_err(err, &e)
                },
                _ => XetStatus::XetErrInvalidArg,
            }
        }
    };
}

// TODO(Task 11): re-enable once reports.rs / upload.rs exist
// take_variant!(xet_op_take_file_metadata, OpOutput::FileMetadata, XetFileMetadataHandle,
// XetFileMetadataHandle::owned); take_variant!(xet_op_take_commit_report, OpOutput::CommitReport,
// XetCommitReportHandle, XetCommitReportHandle::new); take_variant!(xet_op_take_download_report,
// OpOutput::DownloadReport, XetDownloadGroupReportHandle, XetDownloadGroupReportHandle::new);

/// Void ops (e.g. stream write) — no payload.
///
/// # Safety
/// `op` must be a valid pointer to a live `XetOp`; `err` must be null or a valid
/// writable `*mut XetError`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_take_void(op: *mut XetOp, err: *mut *mut XetError) -> XetStatus {
    match take_output(op) {
        Some(Ok(OpOutput::Void)) => {
            free_handle(op);
            XetStatus::XetOk
        },
        Some(Err(e)) => {
            free_handle(op);
            set_xet_err(err, &e)
        },
        _ => XetStatus::XetErrInvalidArg,
    }
}

/// Ordered stream chunk. On success `*out` is a `XetBytes*`, or NULL at EOF.
///
/// # Safety
/// `op` must be a valid pointer to a live `XetOp`; `out` and `err` must be null
/// or valid writable out-pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_take_bytes(
    op: *mut XetOp,
    out: *mut *mut XetBytes,
    err: *mut *mut XetError,
) -> XetStatus {
    match take_output(op) {
        Some(Ok(OpOutput::Bytes(opt))) => {
            if !out.is_null() {
                unsafe { *out = opt.map_or(std::ptr::null_mut(), |b| into_handle(XetBytes::new(b))) };
            }
            free_handle(op);
            XetStatus::XetOk
        },
        Some(Err(e)) => {
            free_handle(op);
            set_xet_err(err, &e)
        },
        _ => XetStatus::XetErrInvalidArg,
    }
}

/// Unordered stream chunk. On success fills `*offset` and `*out` (NULL at EOF).
///
/// # Safety
/// `op` must be a valid pointer to a live `XetOp`; `offset`, `out`, and `err`
/// must be null or valid writable out-pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_op_take_chunk(
    op: *mut XetOp,
    offset: *mut u64,
    out: *mut *mut XetBytes,
    err: *mut *mut XetError,
) -> XetStatus {
    match take_output(op) {
        Some(Ok(OpOutput::Chunk(opt))) => {
            match opt {
                Some((off, b)) => {
                    if !offset.is_null() {
                        unsafe { *offset = off };
                    }
                    if !out.is_null() {
                        unsafe { *out = into_handle(XetBytes::new(b)) };
                    }
                },
                None => {
                    if !out.is_null() {
                        unsafe { *out = std::ptr::null_mut() };
                    }
                },
            }
            free_handle(op);
            XetStatus::XetOk
        },
        Some(Err(e)) => {
            free_handle(op);
            set_xet_err(err, &e)
        },
        _ => XetStatus::XetErrInvalidArg,
    }
}

// --- test-only ops ---

#[unsafe(no_mangle)]
pub extern "C" fn xet_test_make_void_op() -> *mut XetOp {
    spawn_op(|| {
        std::thread::sleep(std::time::Duration::from_millis(50));
        Ok(OpOutput::Void)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_test_make_error_op() -> *mut XetOp {
    spawn_op(|| {
        std::thread::sleep(std::time::Duration::from_millis(50));
        Err(xet::XetError::Internal("boom".into()))
    })
}
