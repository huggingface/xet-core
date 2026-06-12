//! One canonical fixture snapshot shared by every render test. Covers: an
//! active session with one active commit (mixed file states, in-flight +
//! recent xorbs, staging + uploaded shards), one ended commit, one active
//! download group (term blocks + prefetch), one monitor with history, and an
//! ended session in the registry sense (not present here: /snapshot only
//! carries live sessions).

use xet_runtime::console::model::*;

pub fn sample_snapshot() -> SnapshotResponse {
    let progress = ProgressSnapshot {
        total_bytes: 8 << 30,
        bytes_completed: 3 << 30,
        rate_bps: Some(412_000_000.0),
        transfer_bytes: 1 << 30,
        transfer_bytes_completed: 900 << 20,
        transfer_rate_bps: Some(380_000_000.0),
    };
    let dedup = DedupSnapshot {
        total_bytes: 3 << 30,
        deduped_bytes: 2 << 30,
        new_bytes: 1 << 30,
        deduped_bytes_by_global_dedup: 500 << 20,
        defrag_prevented_dedup_bytes: 1 << 20,
        xorb_bytes_uploaded: 800 << 20,
        shard_bytes_uploaded: 256 << 10,
    };
    let files = vec![
        FileUploadSnapshot {
            id: 12,
            name: "model-00001.safetensors".into(),
            size: Some(4 << 30),
            state: FileUploadState::Chunking,
            bytes_chunked: 1 << 30,
            n_chunks: 16384,
            file_hash: None,
            sha256: None,
            dedup: None,
            xorb_deps: vec![],
            shard_uploaded: false,
            created_at: 1,
            finished_at: None,
        },
        FileUploadSnapshot {
            id: 13,
            name: "tokenizer.json".into(),
            size: Some(9 << 20),
            state: FileUploadState::AwaitingShard,
            bytes_chunked: 9 << 20,
            n_chunks: 80,
            file_hash: Some("9c01dd00aa00bb00".into()),
            sha256: Some("ff".repeat(32)),
            dedup: Some(dedup.clone()),
            xorb_deps: vec![XorbDepSnapshot {
                xorb_hash: "f3ab12cd".into(),
                n_bytes: 9 << 20,
                uploaded: true,
            }],
            shard_uploaded: false,
            created_at: 1,
            finished_at: None,
        },
    ];
    let commit = UploadCommitDetail {
        as_of: 1000,
        id: 7,
        state: UploadCommitState::Active,
        created_at: 1,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        dedup: dedup.clone(),
        files,
        completed_files: vec![(
            900,
            FileUploadSnapshot {
                id: 11,
                name: "config.json".into(),
                size: Some(4096),
                state: FileUploadState::Complete,
                bytes_chunked: 4096,
                n_chunks: 1,
                file_hash: Some("4be211aa00".into()),
                sha256: None,
                dedup: None,
                xorb_deps: vec![],
                shard_uploaded: true,
                created_at: 1,
                finished_at: Some(900),
            },
        )],
        file_counts: FileCounts { in_flight: 2, completed: 1, failed: 0, aborted: 0 },
        xorbs: XorbsSnapshot {
            in_flight: vec![XorbSnapshot {
                hash: "f3ab12cd5566".into(),
                state: XorbState::Uploading,
                raw_bytes: 64 << 20,
                serialized_bytes: 60 << 20,
                bytes_transferred: 30 << 20,
                n_files: 1,
                created_at: 2,
                finished_at: None,
            }],
            counts: XorbCounts { formed: 48, uploaded: 46, failed: 1 },
            recent: vec![(
                950,
                XorbSnapshot {
                    hash: "9c01ee".into(),
                    state: XorbState::Uploaded,
                    raw_bytes: 64 << 20,
                    serialized_bytes: 61 << 20,
                    bytes_transferred: 61 << 20,
                    n_files: 2,
                    created_at: 2,
                    finished_at: Some(950),
                },
            )],
        },
        shards: vec![
            ShardSnapshot { hash: None, state: ShardState::Staging, n_xorbs: 48, size: 0 },
            ShardSnapshot {
                hash: Some("aa55cc".into()),
                state: ShardState::Uploaded,
                n_xorbs: 12,
                size: 262_144,
            },
        ],
    };
    let ended_commit = UploadCommitDetail {
        as_of: 500,
        id: 5,
        state: UploadCommitState::Completed,
        created_at: 0,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        dedup: dedup.clone(),
        files: vec![],
        completed_files: vec![],
        file_counts: FileCounts { in_flight: 0, completed: 8, failed: 0, aborted: 0 },
        xorbs: XorbsSnapshot::default(),
        shards: vec![],
    };
    let group = DownloadGroupDetail {
        as_of: 1000,
        id: 3,
        kind: DownloadGroupKind::Files,
        state: DownloadGroupState::Active,
        created_at: 5,
        endpoint: Some("local://cas".into()),
        progress: Some(progress.clone()),
        files: vec![FileDownloadSnapshot {
            id: 21,
            name: "out.bin".into(),
            file_hash: Some("ffee9900".into()),
            requested_range: None,
            total_bytes: 1 << 30,
            bytes_completed: 600 << 20,
            state: FileDownloadState::Reconstructing,
            prefetch: Some(PrefetchSnapshot {
                queue_depth: 2,
                prefetched_byte_position: 800 << 20,
                active_byte_position: 600 << 20,
                completion_rate_bps: Some(900_000_000.0),
            }),
            term_blocks: vec![TermBlockSnapshot {
                block_id: 4,
                byte_range: (600 << 20, 700 << 20),
                state: TermState::Fetching,
                terms: vec![],
                created_at: 6,
                fetched_at: None,
            }],
            consumed_blocks: 5,
            created_at: 5,
            finished_at: None,
        }],
        completed_files: vec![],
        file_counts: FileCounts { in_flight: 1, completed: 0, failed: 0, aborted: 0 },
    };
    let monitor = MonitorSnapshot {
        tag: "upload".into(),
        total_permits: 16,
        active_permits: 14,
        available_permits: 2,
        bounds: PermitBounds { min: 1, max: 64 },
        adjustment_enabled: true,
        bytes_sent: 3 << 30,
        success: Some(SuccessModelSnapshot {
            success_ratio: 0.94,
            thresholds: Thresholds { increase: 0.8, decrease: 0.5 },
            recommended_adjustment: AdjustmentRecommendation::Increase,
        }),
        latency: Some(LatencyModelSnapshot {
            predicted_max_rtt_ms: Some(412.5),
            rtt_standard_error_ms: Some(38.0),
            predicted_bandwidth_bps: Some(510_000_000.0),
        }),
        limit_history: vec![(100, 8), (200, 10), (300, 12), (400, 14), (500, 16)],
    };
    let detail = SessionDetail {
        as_of: 1000,
        id: "9f3c0000-1111-2222-3333-444455556666".into(),
        state: SessionState::Active,
        created_at: 0,
        config: vec![("max_concurrent_file_ingestion".into(), "8".into())],
        monitors: vec![monitor],
        upload_commits: vec![],
        ended_upload_commits: vec![ended_commit.clone()],
        download_groups: vec![],
        ended_download_groups: vec![],
    };
    SnapshotResponse {
        as_of: 1000,
        process: ProcessInfo {
            as_of: 1000,
            pid: 4242,
            argv: vec!["pytest".into()],
            start_time_ms: 0,
            version: "1.5.2".into(),
            n_active_sessions: 1,
        },
        sessions: vec![SessionFull {
            detail,
            upload_commit_details: vec![commit],
            download_group_details: vec![group],
        }],
    }
}
