use std::ffi::{CString, c_char};

use xet::xet_session::{XetFileMetadata, XetFileUpload as InnerFileUpload, XetUploadCommit as InnerCommit};

use crate::error::{XetError, XetStatus, ffi_guard, set_err, set_xet_err};
use crate::file_info::{XetSha256Policy, sha256_policy};
use crate::handle::{free_handle, into_handle};
use crate::op::{OpOutput, XetOp, spawn_op};
use crate::reports::XetProgress;
use crate::session::opt_str;

/// An upload commit (Arc-backed; cheap to clone). Free with [`xet_upload_commit_free`].
pub struct XetUploadCommit {
    pub(crate) inner: InnerCommit,
}
impl XetUploadCommit {
    pub(crate) fn new(inner: InnerCommit) -> Self {
        Self { inner }
    }
}

/// A queued file upload. Free with [`xet_file_upload_free`].
pub struct XetFileUpload {
    inner: InnerFileUpload,
}

/// Per-file metadata result. C strings are prebuilt at construction so accessor
/// pointers stay valid until the handle is freed. Free OWNED handles (from
/// `xet_op_take_file_metadata`) with [`xet_file_metadata_free`]; borrowed views
/// returned by report accessors must NOT be freed.
pub struct XetFileMetadataHandle {
    inner: XetFileMetadata,
    hash: CString,
    sha256: Option<CString>,
    tracking_name: Option<CString>,
}
impl XetFileMetadataHandle {
    fn build(inner: XetFileMetadata) -> Self {
        let hash = CString::new(inner.xet_info.hash.clone()).unwrap_or_default();
        let sha256 = inner.xet_info.sha256.clone().and_then(|s| CString::new(s).ok());
        let tracking_name = inner.tracking_name.clone().and_then(|s| CString::new(s).ok());
        Self {
            inner,
            hash,
            sha256,
            tracking_name,
        }
    }
    /// Owned handle whose lifetime the caller controls (freed via `xet_file_metadata_free`).
    pub(crate) fn owned(inner: XetFileMetadata) -> Self {
        Self::build(inner)
    }
    /// A view stored inside a report; freed when the report is freed.
    pub(crate) fn borrowed_view(inner: XetFileMetadata) -> Self {
        Self::build(inner)
    }
}

/// # Safety
/// `commit` must be a valid handle; `path` a valid C string; `provided_sha256`
/// null or valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_upload_from_path(
    commit: *const XetUploadCommit,
    path: *const c_char,
    policy: XetSha256Policy,
    provided_sha256: *const c_char,
    out: *mut *mut XetFileUpload,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(commit) = (unsafe { commit.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null commit"));
        };
        let Ok(path) = (unsafe { crate::session::req_str(path) }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "invalid path"));
        };
        let sha = match unsafe { sha256_policy(policy, provided_sha256) } {
            Ok(s) => s,
            Err(e) => return set_err(err, e),
        };
        match commit.inner.upload_from_path_blocking(path.into(), sha) {
            Ok(u) => {
                unsafe { *out = into_handle(XetFileUpload { inner: u }) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// # Safety
/// `commit` valid; `data`/`len` a valid buffer (data may be null iff len==0);
/// `name`/`provided_sha256` null or valid; `out`/`err` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_upload_bytes(
    commit: *const XetUploadCommit,
    data: *const u8,
    len: usize,
    name: *const c_char,
    policy: XetSha256Policy,
    provided_sha256: *const c_char,
    out: *mut *mut XetFileUpload,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(commit) = (unsafe { commit.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null commit"));
        };
        if data.is_null() && len != 0 {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null data"));
        }
        let bytes = if len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        let tracking = unsafe { opt_str(name) }.ok().flatten().map(|s| s.to_string());
        let sha = match unsafe { sha256_policy(policy, provided_sha256) } {
            Ok(s) => s,
            Err(e) => return set_err(err, e),
        };
        match commit.inner.upload_bytes_blocking(bytes, sha, tracking) {
            Ok(u) => {
                unsafe { *out = into_handle(XetFileUpload { inner: u }) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// Begin a streaming upload. `name` (nullable) is a tracking name.
///
/// # Safety
/// `commit` valid; `name`/`provided_sha256` null or valid; `out`/`err` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_upload_stream(
    commit: *const XetUploadCommit,
    name: *const c_char,
    policy: XetSha256Policy,
    provided_sha256: *const c_char,
    out: *mut *mut crate::upload_stream::XetStreamUpload,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(commit) = (unsafe { commit.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null commit"));
        };
        let tracking = unsafe { opt_str(name) }.ok().flatten().map(|s| s.to_string());
        let sha = match unsafe { sha256_policy(policy, provided_sha256) } {
            Ok(s) => s,
            Err(e) => return set_err(err, e),
        };
        match commit.inner.upload_stream_blocking(tracking, sha) {
            Ok(su) => {
                unsafe { *out = into_handle(crate::upload_stream::XetStreamUpload::new(su)) };
                XetStatus::XetOk
            },
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// # Safety
/// `upload` valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_upload_finalize_start(
    upload: *const XetFileUpload,
    out: *mut *mut XetOp,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(upload) = (unsafe { upload.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null upload"));
        };
        let handle = upload.inner.clone();
        unsafe { *out = spawn_op(move || handle.finalize_ingestion_blocking().map(OpOutput::FileMetadata)) };
        XetStatus::XetOk
    })
}

/// # Safety
/// `commit` valid; `out`/`err` valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_commit_start(
    commit: *const XetUploadCommit,
    out: *mut *mut XetOp,
    err: *mut *mut XetError,
) -> XetStatus {
    ffi_guard(err, || {
        let Some(commit) = (unsafe { commit.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null commit"));
        };
        let handle = commit.inner.clone();
        unsafe { *out = spawn_op(move || handle.commit_blocking().map(OpOutput::CommitReport)) };
        XetStatus::XetOk
    })
}

/// # Safety
/// `commit` valid; `out` a valid pointer to a `XetProgress`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_progress(
    commit: *const XetUploadCommit,
    out: *mut XetProgress,
) -> XetStatus {
    let (Some(commit), false) = (unsafe { commit.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    unsafe { *out = XetProgress::from_group(&commit.inner.progress()) };
    XetStatus::XetOk
}

/// # Safety
/// `commit` valid; `err` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_upload_commit_abort(commit: *const XetUploadCommit, err: *mut *mut XetError) -> XetStatus {
    ffi_guard(err, || {
        let Some(commit) = (unsafe { commit.as_ref() }) else {
            return set_err(err, XetError::new(XetStatus::XetErrInvalidArg, "null commit"));
        };
        match commit.inner.abort() {
            Ok(()) => XetStatus::XetOk,
            Err(e) => set_xet_err(err, &e),
        }
    })
}

/// # Safety
/// `m` must be null or a valid metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_metadata_hash(m: *const XetFileMetadataHandle) -> *const c_char {
    match unsafe { m.as_ref() } {
        Some(m) => m.hash.as_ptr(),
        None => std::ptr::null(),
    }
}

/// # Safety
/// `m` must be null or a valid metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_metadata_file_size(m: *const XetFileMetadataHandle) -> u64 {
    match unsafe { m.as_ref() } {
        Some(m) => m.inner.xet_info.file_size.unwrap_or(0),
        None => 0,
    }
}

/// Returns NULL if no sha256 is present.
///
/// # Safety
/// `m` must be null or a valid metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_metadata_sha256(m: *const XetFileMetadataHandle) -> *const c_char {
    match unsafe { m.as_ref() } {
        Some(m) => m.sha256.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
        None => std::ptr::null(),
    }
}

/// Returns NULL if no tracking name is present.
///
/// # Safety
/// `m` must be null or a valid metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_file_metadata_tracking_name(m: *const XetFileMetadataHandle) -> *const c_char {
    match unsafe { m.as_ref() } {
        Some(m) => m.tracking_name.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
        None => std::ptr::null(),
    }
}

/// Free an OWNED metadata handle (from `xet_op_take_file_metadata`). Do NOT call
/// on a borrowed handle returned by a report accessor.
#[unsafe(no_mangle)]
pub extern "C" fn xet_file_metadata_free(m: *mut XetFileMetadataHandle) {
    free_handle(m);
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_upload_commit_free(commit: *mut XetUploadCommit) {
    free_handle(commit);
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_file_upload_free(upload: *mut XetFileUpload) {
    free_handle(upload);
}
