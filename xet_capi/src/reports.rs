use xet::xet_session::{
    DeduplicationMetrics, GroupProgressReport, ItemProgressReport, XetCommitReport, XetDownloadGroupReport,
};

use crate::error::XetStatus;
use crate::handle::free_handle;
use crate::upload::XetFileMetadataHandle;

/// Flat progress snapshot (all scalar; stable layout).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct XetProgress {
    pub total_bytes: u64,
    pub total_bytes_completed: u64,
    pub total_transfer_bytes: u64,
    pub total_transfer_bytes_completed: u64,
}
impl XetProgress {
    pub(crate) fn from_group(p: &GroupProgressReport) -> Self {
        Self {
            total_bytes: p.total_bytes,
            total_bytes_completed: p.total_bytes_completed,
            total_transfer_bytes: p.total_transfer_bytes,
            total_transfer_bytes_completed: p.total_transfer_bytes_completed,
        }
    }
    pub(crate) fn from_item(p: &ItemProgressReport) -> Self {
        Self {
            total_bytes: p.total_bytes,
            total_bytes_completed: p.bytes_completed,
            total_transfer_bytes: p.total_bytes,
            total_transfer_bytes_completed: p.bytes_completed,
        }
    }
}

/// Flat dedup metrics snapshot.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct XetDedupMetrics {
    pub total_bytes: u64,
    pub deduped_bytes: u64,
    pub new_bytes: u64,
    pub deduped_bytes_by_global_dedup: u64,
    pub total_chunks: u64,
    pub deduped_chunks: u64,
    pub new_chunks: u64,
    pub xorb_bytes_uploaded: u64,
    pub shard_bytes_uploaded: u64,
    pub total_bytes_uploaded: u64,
}
impl XetDedupMetrics {
    pub(crate) fn from_metrics(m: &DeduplicationMetrics) -> Self {
        Self {
            total_bytes: m.total_bytes,
            deduped_bytes: m.deduped_bytes,
            new_bytes: m.new_bytes,
            deduped_bytes_by_global_dedup: m.deduped_bytes_by_global_dedup,
            total_chunks: m.total_chunks,
            deduped_chunks: m.deduped_chunks,
            new_chunks: m.new_chunks,
            xorb_bytes_uploaded: m.xorb_bytes_uploaded,
            shard_bytes_uploaded: m.shard_bytes_uploaded,
            total_bytes_uploaded: m.total_bytes_uploaded,
        }
    }
}

/// Owned commit report. Free with [`xet_commit_report_free`].
pub struct XetCommitReportHandle {
    inner: XetCommitReport,
    // Stable insertion-ordered view of uploads for `_at` indexing. Owned views
    // live as long as the report; do NOT free them individually.
    files: Vec<XetFileMetadataHandle>,
}
impl XetCommitReportHandle {
    pub(crate) fn new(inner: XetCommitReport) -> Self {
        let files = inner
            .uploads
            .values()
            .cloned()
            .map(XetFileMetadataHandle::borrowed_view)
            .collect();
        Self { inner, files }
    }
}

/// Owned download-group report. Free with [`xet_download_group_report_free`].
pub struct XetDownloadGroupReportHandle {
    inner: XetDownloadGroupReport,
    task_ids: Vec<u64>,
}
impl XetDownloadGroupReportHandle {
    pub(crate) fn new(inner: XetDownloadGroupReport) -> Self {
        let task_ids = inner.downloads.keys().map(|k| k.0).collect();
        Self { inner, task_ids }
    }
}

/// # Safety
/// `r` must be null or a valid commit-report handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_commit_report_file_count(r: *const XetCommitReportHandle) -> usize {
    match unsafe { r.as_ref() } {
        Some(r) => r.files.len(),
        None => 0,
    }
}

/// Borrow the i-th file metadata. The returned pointer is valid until the
/// report is freed; do NOT pass it to `xet_file_metadata_free`.
///
/// # Safety
/// `r` valid; `out` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_commit_report_file_at(
    r: *const XetCommitReportHandle,
    index: usize,
    out: *mut *const XetFileMetadataHandle,
) -> XetStatus {
    let (Some(r), false) = (unsafe { r.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    match r.files.get(index) {
        Some(m) => {
            unsafe { *out = m as *const XetFileMetadataHandle };
            XetStatus::XetOk
        },
        None => XetStatus::XetErrInvalidArg,
    }
}

/// # Safety
/// `r` valid; `out` a valid pointer to a `XetDedupMetrics`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_commit_report_dedup(
    r: *const XetCommitReportHandle,
    out: *mut XetDedupMetrics,
) -> XetStatus {
    let (Some(r), false) = (unsafe { r.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    unsafe { *out = XetDedupMetrics::from_metrics(&r.inner.dedup_metrics) };
    XetStatus::XetOk
}

/// # Safety
/// `r` valid; `out` a valid pointer to a `XetProgress`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_commit_report_progress(
    r: *const XetCommitReportHandle,
    out: *mut XetProgress,
) -> XetStatus {
    let (Some(r), false) = (unsafe { r.as_ref() }, out.is_null()) else {
        return XetStatus::XetErrInvalidArg;
    };
    unsafe { *out = XetProgress::from_group(&r.inner.progress) };
    XetStatus::XetOk
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_commit_report_free(r: *mut XetCommitReportHandle) {
    free_handle(r);
}

/// # Safety
/// `r` must be null or a valid download-group-report handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_group_report_count(r: *const XetDownloadGroupReportHandle) -> usize {
    match unsafe { r.as_ref() } {
        Some(r) => r.task_ids.len(),
        None => 0,
    }
}

/// Fill `*task_id` and `*bytes_completed` for the i-th download.
///
/// # Safety
/// `r` valid; `task_id`/`bytes_completed` may be null (skipped) or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_download_group_report_at(
    r: *const XetDownloadGroupReportHandle,
    index: usize,
    task_id: *mut u64,
    bytes_completed: *mut u64,
) -> XetStatus {
    let Some(r) = (unsafe { r.as_ref() }) else {
        return XetStatus::XetErrInvalidArg;
    };
    let Some(&tid) = r.task_ids.get(index) else {
        return XetStatus::XetErrInvalidArg;
    };
    if !task_id.is_null() {
        unsafe { *task_id = tid };
    }
    if !bytes_completed.is_null() {
        let bytes = r
            .inner
            .downloads
            .get(&xet::xet_session::UniqueId(tid))
            .and_then(|d| d.progress.as_ref())
            .map_or(0, |p| p.bytes_completed);
        unsafe { *bytes_completed = bytes };
    }
    XetStatus::XetOk
}

#[unsafe(no_mangle)]
pub extern "C" fn xet_download_group_report_free(r: *mut XetDownloadGroupReportHandle) {
    free_handle(r);
}
