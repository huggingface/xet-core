//! Regression test for upload progress that finished below 100% after a successful upload.
//!
//! When a file repeats data still in its in-progress xorb, the repeat is deduplicated against that
//! xorb. If the xorb was then cut while the file was still being processed
//! (`FileDeduper::process_chunks`), the file's progress dependency on it was registered with the
//! xorb's size instead of the bytes of the file that reference it, so the repeated bytes were never
//! reported as completed.

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_data::processing::test_utils::TestEnvironment;
use xet_data::processing::{FileUploadSession, Sha256Policy};
use xet_runtime::test_set_constants;

// Small chunks and xorbs so a few hundred KB of data cuts several xorbs within one file.
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

/// A block repeated back to back, followed by enough unique data to force a xorb cut before the
/// file ends.
fn data_with_repeat_in_cut_xorb() -> Vec<u8> {
    let block = random_data(1, 16 * 1024);
    let mut data = Vec::new();
    data.extend_from_slice(&block);
    data.extend_from_slice(&block);
    data.extend_from_slice(&random_data(2, 4 * *MAX_XORB_BYTES));
    data
}

/// Once every xorb is uploaded, progress must account for all of the file's bytes, including the
/// ones deduplicated against a xorb that was cut mid-file. Before the fix this was short by exactly
/// `deduped_bytes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeat_within_cut_xorb_completes_progress() {
    let data = data_with_repeat_in_cut_xorb();
    let env = TestEnvironment::new().await;
    let session = FileUploadSession::new(env.config.clone()).await.unwrap();

    let (_, mut cleaner) = session
        .start_clean(Some("repeated".into()), Some(data.len() as u64), Sha256Policy::Skip)
        .unwrap();
    for frame in data.chunks(8 * 1024) {
        cleaner.add_data_from_bytes(Bytes::copy_from_slice(frame)).await.unwrap();
    }
    let (file_info, metrics) = cleaner.finish().await.unwrap();
    assert_eq!(file_info.file_size, Some(data.len() as u64));
    assert!(metrics.deduped_bytes > 0, "test did not exercise local dedup; tune the data");

    // Upload the remaining data and wait for every xorb, so all progress has been reported.
    session.checkpoint().await.unwrap();

    let report = session.report();
    assert_eq!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
    assert_eq!(report.total_bytes, data.len() as u64);
    assert_eq!(
        report.total_bytes_completed,
        report.total_bytes,
        "{} of {} bytes never reported complete (deduped_bytes = {})",
        report.total_bytes - report.total_bytes_completed,
        report.total_bytes,
        metrics.deduped_bytes
    );

    session.finalize().await.unwrap();
}
