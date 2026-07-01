use xet::xet_session::{DeduplicationMetrics, GroupProgressReport, ItemProgressReport};

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
