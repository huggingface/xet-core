#![cfg(feature = "simulation")]
//! A xorb range response that stays open but only trickles in must not stall the download, and a link
//! that is slow for every request must still complete.

use std::sync::Arc;
use std::time::Duration;

use xet_client::cas_client::simulation::ClientTestingUtils;
use xet_client::cas_client::{Client, LocalTestServer, LocalTestServerBuilder, RemoteClient};
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::{FileReconstructor, Result};
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetContext;
use xet_runtime::test_set_config;

test_set_config! {
    client {
        retry_base_delay = Duration::from_millis(10);
        download_min_speed_window = Duration::from_secs(1);
    }
}

const CHUNK_SIZE: usize = 4096;

/// Downloads `file_hash` from `server`, failing the test if it takes longer than 30 seconds.
async fn download(server: &LocalTestServer, file_hash: MerkleHash) -> Result<Vec<u8>> {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("out");
    let client: Arc<dyn Client> = server.remote_client();
    let reconstructor = FileReconstructor::new(&XetContext::default().unwrap(), &client, file_hash);

    tokio::time::timeout(Duration::from_secs(30), reconstructor.reconstruct_to_file(&out_path, None, true))
        .await
        .expect("download stalled")?;
    Ok(std::fs::read(&out_path).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trickling_range_is_retried() {
    let server = LocalTestServerBuilder::new().start().await;
    let file = server
        .remote_client()
        .upload_random_file(&[(1, (0, 4)), (2, (0, 4)), (3, (0, 4))], CHUNK_SIZE)
        .await
        .unwrap();

    server.trickle_next_fetch_terms(1);
    let data = download(&server, file.file_hash).await.unwrap();

    assert_eq!(server.pending_trickled_fetch_terms(), 0);
    assert_eq!(data, file.data.as_ref());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_slow_link_restarts_each_range_once() {
    // Every range comes in at 100 bytes/s, below the 4000 bytes/s minimum. Each is aborted once, and its
    // unchecked retry completes.
    let server = LocalTestServerBuilder::new().start().await;
    let file = server
        .remote_client()
        .upload_random_file(&[(1, (0, 4)), (2, (0, 4)), (3, (0, 4))], 256)
        .await
        .unwrap();

    server.trickle_next_fetch_terms(usize::MAX);
    let data = download(&server, file.file_hash).await.unwrap();

    assert_eq!(usize::MAX - server.pending_trickled_fetch_terms(), 6);
    assert_eq!(data, file.data.as_ref());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_slow_range_is_not_aborted_without_a_retry_left() {
    let server = LocalTestServerBuilder::new().start().await;
    let file = server.remote_client().upload_random_file(&[(1, (0, 4))], 256).await.unwrap();

    let config = XetConfig::new().with_config("client.retry_max_attempts", 0).unwrap();
    let ctx = XetContext::with_config(config).unwrap();
    let client: Arc<dyn Client> = RemoteClient::new(ctx.clone(), server.http_endpoint(), &None, "test", false, None);

    server.trickle_next_fetch_terms(1);
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("out");
    let reconstructor = FileReconstructor::new(&ctx, &client, file.file_hash);
    tokio::time::timeout(Duration::from_secs(30), reconstructor.reconstruct_to_file(&out_path, None, true))
        .await
        .expect("download stalled")
        .unwrap();

    assert_eq!(std::fs::read(&out_path).unwrap(), file.data.as_ref());
}
