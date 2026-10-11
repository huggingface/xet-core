//! Integration coverage for verified reuse of existing download data.
//! Uses the simulation server; cancellation coverage does not simulate network loss.

#![cfg(feature = "simulation")]

use rand::{Rng, SeedableRng, rngs::StdRng};
use std::sync::Arc;
use xet_data::processing::test_utils::TestEnvironment;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

// Larger than a typical ~64 MiB segment so a partial prefix can include complete segments.
const SIZE: usize = 128 * 1024 * 1024;

async fn fixture() -> (TestEnvironment, Vec<u8>, XetFileInfo) {
    let env = TestEnvironment::new().await;
    let mut data = vec![0; SIZE];
    StdRng::seed_from_u64(989).fill_bytes(&mut data);
    let upload = FileUploadSession::new(env.config.clone()).await.unwrap();
    let (_, mut cleaner) = upload
        .start_clean(Some("weights.bin".into()), Some(data.len() as u64), Sha256Policy::Compute)
        .unwrap();
    cleaner.add_data(&data).await.unwrap();
    let info = cleaner.finish().await.unwrap().0;
    upload.finalize().await.unwrap();
    (env, data, info)
}

async fn fresh_session(env: &TestEnvironment) -> Arc<FileDownloadSession> {
    // None explicitly disables client chunk-cache injection: savings must come from the file.
    FileDownloadSession::new(env.config.clone(), None).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn complete_existing_file_needs_no_weight_transfer() {
    let (env, data, info) = fixture().await;
    let out = env.base_dir.join("complete.bin");
    std::fs::write(&out, &data).unwrap();
    let session = fresh_session(&env).await;
    let (_, bytes) = session.download_file_reusing_existing(&info, &out).await.unwrap();
    let report = session.report();
    assert_eq!(bytes, SIZE as u64);
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert_eq!(report.total_resume_check_bytes_completed, SIZE as u64);
    assert_eq!(report.total_transfer_bytes_completed, 0);
    session.finalize().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn partial_prefix_in_new_session_transfers_less_than_full_download() {
    let (env, data, info) = fixture().await;
    let out = env.base_dir.join("partial.bin");
    // State left by an interrupted sequential write, including an incomplete segment.
    std::fs::write(&out, &data[..SIZE * 3 / 4 + 17]).unwrap();
    let session = fresh_session(&env).await;
    session.download_file_reusing_existing(&info, &out).await.unwrap();
    let resumed = session.report();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(resumed.total_resume_check_bytes_completed > 0);
    session.finalize().await.unwrap();

    let baseline = fresh_session(&env).await;
    baseline.download_file(&info, &env.base_dir.join("baseline.bin")).await.unwrap();
    let full = baseline.report();
    assert!(resumed.total_transfer_bytes_completed > 0);
    assert!(resumed.total_transfer_bytes_completed < full.total_transfer_bytes_completed);
    baseline.finalize().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn corrupted_segment_is_repaired_and_trailing_bytes_are_removed() {
    let (env, data, info) = fixture().await;
    let out = env.base_dir.join("corrupt.bin");
    let mut existing = data.clone();
    existing[0] ^= 0xff;
    existing.extend_from_slice(b"unexpected trailing bytes");
    std::fs::write(&out, existing).unwrap();
    let session = fresh_session(&env).await;
    session.download_file_reusing_existing(&info, &out).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(session.report().total_transfer_bytes_completed > 0);
    session.finalize().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn cancelled_stream_partial_file_is_reused_by_new_session() {
    use std::io::Write;

    let (env, data, info) = fixture().await;
    let out = env.base_dir.join("cancelled.bin");
    let first_session = fresh_session(&env).await;
    let (_, mut stream) = first_session.download_stream(&info, None).await.unwrap();
    let mut file = std::fs::File::create(&out).unwrap();
    let mut written = 0;
    while written < SIZE * 3 / 4 {
        let chunk = stream.next().await.unwrap().expect("stream ended before cancellation");
        file.write_all(&chunk).unwrap();
        written += chunk.len();
    }
    stream.cancel();
    assert!(stream.next().await.unwrap().is_none());
    file.sync_all().unwrap();
    drop(file);
    drop(stream);
    drop(first_session);
    assert!(written < SIZE, "fixture must leave an incomplete file");

    let resumed_session = fresh_session(&env).await;
    resumed_session.download_file_reusing_existing(&info, &out).await.unwrap();
    let resumed = resumed_session.report();
    assert_eq!(std::fs::read(&out).unwrap(), data);
    assert!(resumed.total_resume_check_bytes_completed > 0);
    resumed_session.finalize().await.unwrap();

    let baseline = fresh_session(&env).await;
    baseline
        .download_file(&info, &env.base_dir.join("cancelled-baseline.bin"))
        .await
        .unwrap();
    assert!(resumed.total_transfer_bytes_completed < baseline.report().total_transfer_bytes_completed);
    baseline.finalize().await.unwrap();
}
