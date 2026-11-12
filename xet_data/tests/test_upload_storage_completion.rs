//! Storage completion must not depend on logical-byte progress accuracy (#1053).
//!
//! Run with xet-data's debug progress assertions disabled:
//! cargo --config 'profile.dev.package.xet-data.debug-assertions=false' test \
//!     -p xet-data --test test_upload_storage_completion -- --ignored

use std::sync::Arc;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy};
use xet_runtime::core::XetContext;
use xet_runtime::test_set_constants;

test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 64 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 64;
}

fn random_data(seed: u64, size: usize) -> Vec<u8> {
    let mut data = vec![0; size];
    StdRng::seed_from_u64(seed).fill(&mut data[..]);
    data
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires xet-data debug assertions disabled; #1053 still affects the debug progress assertion"]
async fn repeated_data_finalizes_and_round_trips_despite_progress_undercount() {
    let block = random_data(1, 16 * 1024);
    let mut data = block.clone();
    data.extend_from_slice(&block);
    data.extend_from_slice(&random_data(2, 4 * *MAX_XORB_BYTES));

    let temp = tempfile::tempdir().unwrap();
    let ctx = XetContext::default().unwrap();
    let config = Arc::new(TranslatorConfig::local_config(&ctx, temp.path()).unwrap());
    let session = FileUploadSession::new(config.clone()).await.unwrap();
    let (_, mut cleaner) = session
        .start_clean(Some("repeated".into()), Some(data.len() as u64), Sha256Policy::Skip)
        .unwrap();
    for frame in data.chunks(8 * 1024) {
        cleaner.add_data_from_bytes(Bytes::copy_from_slice(frame)).await.unwrap();
    }
    let (file, metrics) = cleaner.finish().await.unwrap();
    assert!(metrics.deduped_bytes > 0, "fixture must exercise repeated data");
    session.checkpoint().await.unwrap();
    let progress = session.report();
    assert_eq!(progress.total_transfer_bytes_completed, progress.total_transfer_bytes);

    session.finalize().await.unwrap();
    let download = FileDownloadSession::new(config, None).await.unwrap();
    let dest = temp.path().join("roundtrip");
    download.download_file(&file, &dest).await.unwrap();
    assert_eq!(std::fs::read(dest).unwrap(), data);
}
