use serde::{Deserialize, Serialize};

/// Serializes Some(non-finite) as null — serde_json errors on NaN/Infinity,
/// which would fail an entire response right when an operator needs it.
fn none_if_not_finite<S: serde::Serializer>(v: &Option<f64>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(x) if x.is_finite() => s.serialize_some(x),
        _ => s.serialize_none(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub service: String,   // "xet-console"
    pub version: String,   // env!("CARGO_PKG_VERSION")
    pub pid: u32,
    pub argv: Vec<String>,
    pub start_time_ms: u64,
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub as_of: u64,
    pub pid: u32,
    pub argv: Vec<String>,
    pub start_time_ms: u64, // console server start, not process start
    pub version: String,
    pub n_active_sessions: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState { Active, Ended }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String, // uuid
    pub state: SessionState,
    pub created_at: u64,
    pub n_upload_commits: usize,
    pub n_download_groups: usize,
    pub n_monitors: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsResponse {
    pub as_of: u64,
    pub sessions: Vec<SessionSummary>,
    pub ended_sessions: Vec<SessionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub as_of: u64,
    pub id: String,
    pub state: SessionState,
    pub created_at: u64,
    /// Selected config values captured at registration (concurrency settings,
    /// runtime kind, worker threads) as display strings.
    pub config: Vec<(String, String)>,
    pub monitors: Vec<MonitorSnapshot>,
    pub upload_commits: Vec<UploadCommitSummary>,
    pub ended_upload_commits: Vec<UploadCommitDetail>,
    pub download_groups: Vec<DownloadGroupSummary>,
    pub ended_download_groups: Vec<DownloadGroupDetail>,
}

// ---- concurrency monitors ----

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentRecommendation {
    Increase,
    #[default]
    Hold,
    Decrease,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct Thresholds {
    pub increase: f64,
    pub decrease: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct PermitBounds {
    pub min: usize,
    pub max: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SuccessModelSnapshot {
    pub success_ratio: f64,
    pub thresholds: Thresholds,
    pub recommended_adjustment: AdjustmentRecommendation,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyModelSnapshot {
    #[serde(serialize_with = "none_if_not_finite")]
    pub predicted_max_rtt_ms: Option<f64>,
    #[serde(serialize_with = "none_if_not_finite")]
    pub rtt_standard_error_ms: Option<f64>,
    #[serde(serialize_with = "none_if_not_finite")]
    pub predicted_bandwidth_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub tag: String,
    pub total_permits: usize,
    pub active_permits: usize,
    pub available_permits: usize,
    pub bounds: PermitBounds,
    pub adjustment_enabled: bool,
    pub bytes_sent: u64,
    pub success: Option<SuccessModelSnapshot>,
    pub latency: Option<LatencyModelSnapshot>,
    /// (epoch_ms, limit) — one entry per adjustment.
    pub limit_history: Vec<(u64, usize)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyResponse {
    pub as_of: u64,
    pub session_id: String,
    pub monitors: Vec<MonitorSnapshot>,
}

// ---- shared progress/dedup ----

/// Mirrors xet_data's GroupProgressReport (which xet_runtime cannot depend on).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProgressSnapshot {
    pub total_bytes: u64,
    pub bytes_completed: u64,
    #[serde(serialize_with = "none_if_not_finite")]
    pub rate_bps: Option<f64>,
    pub transfer_bytes: u64,
    pub transfer_bytes_completed: u64,
    #[serde(serialize_with = "none_if_not_finite")]
    pub transfer_rate_bps: Option<f64>,
}

/// Mirrors xet_data's DeduplicationMetrics byte fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DedupSnapshot {
    pub total_bytes: u64,
    pub deduped_bytes: u64,
    pub new_bytes: u64,
    pub deduped_bytes_by_global_dedup: u64,
    pub defrag_prevented_dedup_bytes: u64,
    pub xorb_bytes_uploaded: u64,
    pub shard_bytes_uploaded: u64,
}

// ---- upload side ----

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UploadCommitState { Active, Committing, Completed, Aborted }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitSummary {
    pub id: u64,
    pub state: UploadCommitState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub n_files_in_flight: usize,
    pub n_files_completed: u64,
    pub progress: Option<ProgressSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileUploadState {
    Queued, Chunking, Processed, AwaitingXorbs, AwaitingShard, Complete, Failed, Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorbDepSnapshot {
    pub xorb_hash: String,
    pub n_bytes: u64,
    pub uploaded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadSnapshot {
    pub id: u64,
    pub name: String,
    pub size: Option<u64>,
    pub state: FileUploadState,
    pub bytes_chunked: u64,
    pub n_chunks: u64,
    pub file_hash: Option<String>,
    pub sha256: Option<String>,
    pub dedup: Option<DedupSnapshot>,
    pub xorb_deps: Vec<XorbDepSnapshot>,
    pub shard_uploaded: bool,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileCounts {
    pub in_flight: usize,
    pub completed: u64,
    pub failed: u64,
    pub aborted: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum XorbState { Formed, Queued, Uploading, Uploaded, Failed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorbSnapshot {
    pub hash: String,
    pub state: XorbState,
    pub raw_bytes: u64,
    pub serialized_bytes: u64,
    pub bytes_transferred: u64,
    pub n_files: usize,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct XorbCounts {
    pub formed: u64,
    pub uploaded: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct XorbsSnapshot {
    pub in_flight: Vec<XorbSnapshot>,
    pub counts: XorbCounts,
    /// (epoch_ms of completion, snapshot) — recent ring.
    pub recent: Vec<(u64, XorbSnapshot)>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShardState { Staging, Uploading, Uploaded }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub hash: Option<String>, // None for the live staging accumulator entry
    pub state: ShardState,
    pub n_xorbs: usize,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitDetail {
    pub as_of: u64,
    pub id: u64,
    pub state: UploadCommitState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub progress: Option<ProgressSnapshot>,
    pub dedup: DedupSnapshot,
    pub files: Vec<FileUploadSnapshot>,
    /// (epoch_ms of completion, final snapshot) — recent ring; ?files=all
    /// returns everything still retained here plus in-flight.
    pub completed_files: Vec<(u64, FileUploadSnapshot)>,
    pub file_counts: FileCounts,
    pub xorbs: XorbsSnapshot,
    pub shards: Vec<ShardSnapshot>,
}

// ---- download side ----

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadGroupKind { Files, Stream }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadGroupState { Active, Finished, Aborted }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadGroupSummary {
    pub id: u64,
    pub kind: DownloadGroupKind,
    pub state: DownloadGroupState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub n_files_in_flight: usize,
    pub n_files_completed: u64,
    pub progress: Option<ProgressSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileDownloadState { Queued, Reconstructing, Complete, Failed, Aborted }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TermState { Enqueued, Fetching, Fetched, Consumed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermInfo {
    pub xorb_hash: String,
    pub chunk_range: (u32, u32),
    pub byte_range: (u64, u64),
}

/// One prefetch fetch-block (the queue unit in ReconstructionTermManager).
/// Resolves to one or more terms once fetched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermBlockSnapshot {
    pub block_id: u64,
    pub byte_range: (u64, u64),
    pub state: TermState,
    pub terms: Vec<TermInfo>, // populated on Fetched
    pub created_at: u64,
    pub fetched_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PrefetchSnapshot {
    pub queue_depth: usize,
    pub prefetched_byte_position: u64,
    pub active_byte_position: u64,
    #[serde(serialize_with = "none_if_not_finite")]
    pub completion_rate_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDownloadSnapshot {
    pub id: u64,
    pub name: String,
    pub file_hash: Option<String>,
    pub requested_range: Option<(u64, u64)>,
    pub total_bytes: u64,
    pub bytes_completed: u64,
    pub state: FileDownloadState,
    pub prefetch: Option<PrefetchSnapshot>,
    pub term_blocks: Vec<TermBlockSnapshot>,
    pub consumed_blocks: u64,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadGroupDetail {
    pub as_of: u64,
    pub id: u64,
    pub kind: DownloadGroupKind,
    pub state: DownloadGroupState,
    pub created_at: u64,
    pub endpoint: Option<String>,
    pub progress: Option<ProgressSnapshot>,
    pub files: Vec<FileDownloadSnapshot>,
    pub completed_files: Vec<(u64, FileDownloadSnapshot)>,
    pub file_counts: FileCounts,
}

// ---- list responses (envelope carrying as_of) ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitsResponse {
    pub as_of: u64,
    pub commits: Vec<UploadCommitSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadGroupsResponse {
    pub as_of: u64,
    pub groups: Vec<DownloadGroupSummary>,
}

// ---- snapshot (agent one-shot) ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFull {
    pub detail: SessionDetail,
    pub upload_commit_details: Vec<UploadCommitDetail>,
    pub download_group_details: Vec<DownloadGroupDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub as_of: u64,
    pub process: ProcessInfo,
    pub sessions: Vec<SessionFull>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_serialize_snake_case() {
        assert_eq!(serde_json::to_string(&FileUploadState::AwaitingXorbs).unwrap(), "\"awaiting_xorbs\"");
        assert_eq!(serde_json::to_string(&TermState::Fetching).unwrap(), "\"fetching\"");
        assert_eq!(serde_json::to_string(&XorbState::Uploading).unwrap(), "\"uploading\"");
        assert_eq!(serde_json::to_string(&SessionState::Ended).unwrap(), "\"ended\"");
        assert_eq!(serde_json::to_string(&AdjustmentRecommendation::Increase).unwrap(), "\"increase\"");
    }

    #[test]
    fn monitor_snapshot_golden_json() {
        let m = MonitorSnapshot {
            tag: "upload".into(),
            total_permits: 16,
            active_permits: 14,
            available_permits: 2,
            bounds: PermitBounds { min: 1, max: 64 },
            adjustment_enabled: true,
            bytes_sent: 123,
            success: Some(SuccessModelSnapshot {
                success_ratio: 0.9,
                thresholds: Thresholds { increase: 0.8, decrease: 0.5 },
                recommended_adjustment: AdjustmentRecommendation::Increase,
            }),
            latency: Some(LatencyModelSnapshot {
                predicted_max_rtt_ms: Some(412.5),
                rtt_standard_error_ms: Some(f64::NAN), // sanitizer must emit null
                predicted_bandwidth_bps: None,
            }),
            limit_history: vec![(1000, 15), (2000, 16)],
        };
        let json = serde_json::to_string(&m).unwrap(); // must not error despite NaN
        assert_eq!(
            json,
            "{\"tag\":\"upload\",\"total_permits\":16,\"active_permits\":14,\"available_permits\":2,\"bounds\":{\"min\":1,\"max\":64},\"adjustment_enabled\":true,\"bytes_sent\":123,\"success\":{\"success_ratio\":0.9,\"thresholds\":{\"increase\":0.8,\"decrease\":0.5},\"recommended_adjustment\":\"increase\"},\"latency\":{\"predicted_max_rtt_ms\":412.5,\"rtt_standard_error_ms\":null,\"predicted_bandwidth_bps\":null},\"limit_history\":[[1000,15],[2000,16]]}"
        );
    }

    #[test]
    fn upload_commit_detail_round_trips() {
        let detail = UploadCommitDetail {
            as_of: 1,
            id: 7,
            state: UploadCommitState::Active,
            created_at: 0,
            endpoint: Some("local://x".into()),
            progress: Some(ProgressSnapshot::default()),
            dedup: DedupSnapshot::default(),
            files: vec![],
            completed_files: vec![],
            file_counts: FileCounts::default(),
            xorbs: XorbsSnapshot::default(),
            shards: vec![],
        };
        let json = serde_json::to_string(&detail).unwrap();
        let back: UploadCommitDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 7);
        assert!(matches!(back.state, UploadCommitState::Active));
    }
}
