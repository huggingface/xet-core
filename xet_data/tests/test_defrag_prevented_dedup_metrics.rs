//! Regression test for `total_bytes` being over-counted when defrag prevention rejects a dedup
//! match.
//!
//! `FileCleaner::finish_inner` reports `DeduplicationMetrics::total_bytes` as the file's size, so
//! any over-count there becomes a wrong `XetFileInfo::file_size`. Previously the deduped range was
//! added to `total_bytes` *before* the defrag check, and a rejected range then fell through and was
//! counted a second time as new data — inflating the reported size by exactly
//! `defrag_prevented_dedup_bytes` while the file's actual content was unchanged.

use std::sync::Arc;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileUploadSession, Sha256Policy};
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetContext;
use xet_runtime::test_set_constants;

// Small chunks so a few hundred KB of data produces enough ranges to drive the fragmentation
// estimator.
test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 64 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 64;
}

fn random_data(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = vec![0u8; len];
    rng.fill(&mut data[..]);
    data
}

/// Interleaves unique filler with a short repeated block. Each repeat dedups against the earlier
/// copy as a very small range, which is what defrag prevention rejects.
fn fragmented_dedup_data(repeats: usize) -> Vec<u8> {
    let repeated_block = random_data(0xABCD, 4 * 1024);
    let mut data = Vec::new();
    for i in 0..repeats {
        data.extend_from_slice(&random_data(i as u64, 24 * 1024));
        data.extend_from_slice(&repeated_block);
    }
    data
}

/// Builds a session whose defrag prevention rejects essentially every dedup match: the estimator
/// is ready after a single range, and the target chunks-per-range is set far above anything the
/// data can produce, so `allow_dedup_on_next_range` always takes the reject path.
async fn upload_session_rejecting_dedup(dir: &std::path::Path) -> Arc<FileUploadSession> {
    let config = XetConfig::default()
        .with_config("deduplication.nranges_in_streaming_fragmentation_estimator", "1")
        .expect("nranges config path should exist")
        .with_config("deduplication.min_n_chunks_per_range", "100000")
        .expect("min_n_chunks_per_range config path should exist");
    let ctx = XetContext::with_config(config).expect("failed to build XetContext");
    let translator = Arc::new(TranslatorConfig::local_config(&ctx, dir).expect("failed to build TranslatorConfig"));
    FileUploadSession::new(translator)
        .await
        .expect("failed to create upload session")
}

/// The reported file size must equal the bytes fed in, even when defrag prevention rejects dedup
/// matches. Before the fix this returned `len + defrag_prevented_dedup_bytes`.
#[tokio::test]
async fn defrag_prevented_dedup_does_not_inflate_reported_file_size() {
    let data = fragmented_dedup_data(24);
    let dir = tempfile::tempdir().unwrap();
    let session = upload_session_rejecting_dedup(dir.path()).await;

    let (_, mut cleaner) = session
        .start_clean(None, Some(data.len() as u64), Sha256Policy::Skip)
        .expect("start_clean failed");
    // Feed in frames rather than one buffer so chunks are produced incrementally, as they are for
    // a streamed upload.
    for frame in data.chunks(8 * 1024) {
        cleaner
            .add_data_from_bytes(Bytes::copy_from_slice(frame))
            .await
            .expect("add_data_from_bytes failed");
    }
    let (file_info, metrics) = cleaner.finish().await.expect("finish failed");

    assert!(
        metrics.defrag_prevented_dedup_bytes > 0,
        "test did not exercise the defrag-prevented path; tune the data or the config"
    );
    assert_eq!(
        metrics.total_bytes,
        data.len() as u64,
        "total_bytes over-counted by {} (defrag_prevented_dedup_bytes = {})",
        metrics.total_bytes as i128 - data.len() as i128,
        metrics.defrag_prevented_dedup_bytes
    );
    assert_eq!(file_info.file_size, Some(data.len() as u64), "reported file size must match the bytes written");
}
