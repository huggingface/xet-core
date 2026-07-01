use std::ffi::c_char;

use xet::xet_session::{XetFileDownload as InnerFileDownload, XetFileDownloadGroup as InnerGroup};

use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::file_info::XetFileInfo;
use crate::handle::{free_handle, into_handle};
use crate::op::{OpOutput, XetOp, spawn_op};
use crate::reports::XetProgress;
use crate::session::req_str;

/// A file-download group (Arc-backed; cheap to clone). Free with
/// [`xet_file_download_group_free`].
pub struct XetFileDownloadGroup {
    inner: InnerGroup,
}
impl XetFileDownloadGroup {
    pub(crate) fn new(inner: InnerGroup) -> Self {
        Self { inner }
    }
}

/// A queued file download. Free with [`xet_file_download_free`].
pub struct XetFileDownload {
    inner: InnerFileDownload,
}

/// Queue a file for download to `dest_path`. Returns a handle immediately;
/// call `xet_file_download_group_finish_start` to await all downloads.
///
/// # Safety
/// `group`/`file_info` valid handles; `dest_path` a valid C string; `out`/`err` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_download_group_download_to_path(
    group: *const XetFileDownloadGroup,
    file_info: *const XetFileInfo,
    dest_path: *const c_char,
    out: *mut *mut XetFileDownload,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let (Some(group), Some(fi)) = (unsafe { group.as_ref() }, unsafe { file_info.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null group or file_info"));
        };
        let Ok(dest) = (unsafe { req_str(dest_path) }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "invalid dest_path"));
        };
        match group.inner.download_file_to_path_blocking(fi.inner.clone(), dest.into()) {
            Ok(d) => {
                unsafe { *out = into_handle(XetFileDownload { inner: d }) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Start awaiting all queued downloads. Poll the returned op; it yields a
/// download-group report.
///
/// # Safety
/// `group`/`out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_download_group_finish_start(
    group: *const XetFileDownloadGroup,
    out: *mut *mut XetOp,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(group) = (unsafe { group.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null group"));
        };
        let handle = group.inner.clone();
        unsafe { *out = spawn_op(move || handle.finish_blocking().map(OpOutput::DownloadReport)) };
        XetStatus::XetOk
    })
}

/// # Safety
/// `group` valid; `out` a valid pointer to a `XetProgress`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_download_group_progress(
    group: *const XetFileDownloadGroup,
    out: *mut XetProgress,
) -> XetStatus {
    let (Some(group), false) = (unsafe { group.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    unsafe { *out = XetProgress::from_group(&group.inner.progress()) };
    XetStatus::XetOk
}

/// # Safety
/// `group` valid; `err` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_download_group_abort(
    group: *const XetFileDownloadGroup,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(group) = (unsafe { group.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null group"));
        };
        match group.inner.abort() {
            Ok(()) => XetStatus::XetOk,
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Returns the download's task id, or 0 if `download` is null.
///
/// # Safety
/// `download` must be null or a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_download_task_id(download: *const XetFileDownload) -> u64 {
    match unsafe { download.as_ref() } {
        Some(d) => d.inner.task_id().0,
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_file_download_group_free(group: *mut XetFileDownloadGroup) {
    free_handle(group);
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_file_download_free(download: *mut XetFileDownload) {
    free_handle(download);
}
