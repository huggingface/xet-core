#![cfg(feature = "simulation")]
//! A xorb range response that stays open but only trickles in must not stall an in-order download, and a link
//! that is slow for every request must not be hedged.

use std::sync::Arc;
use std::time::Duration;

use xet_client::cas_client::simulation::ClientTestingUtils;
use xet_client::cas_client::{Client, LocalTestServer, LocalTestServerBuilder};
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::{FileReconstructor, Result};
use xet_runtime::core::XetContext;

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
async fn test_trickling_range_is_hedged() {
    let server = LocalTestServerBuilder::new().start().await;
    let file = server
        .remote_client()
        .upload_random_file(&[(1, (0, 4)), (2, (0, 4)), (3, (0, 4))], 4096)
        .await
        .unwrap();

    // Trickle the first term, which the write waits on.
    server.trickle_next_fetch_terms(1, Some(file.terms[0].xorb_hash));
    let data = download(&server, file.file_hash).await.unwrap();

    assert_eq!(server.pending_trickled_fetch_terms(), 0);
    assert_eq!(data, file.data.as_ref());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_uniformly_slow_link_is_not_hedged() {
    // Every range comes in at 100 bytes/s, so each request is as slow as the others.
    let server = LocalTestServerBuilder::new().start().await;
    let file = server
        .remote_client()
        .upload_random_file(&[(1, (0, 4)), (2, (0, 4)), (3, (0, 4))], 256)
        .await
        .unwrap();

    server.trickle_next_fetch_terms(usize::MAX, None);
    let data = download(&server, file.file_hash).await.unwrap();

    assert_eq!(usize::MAX - server.pending_trickled_fetch_terms(), 3);
    assert_eq!(data, file.data.as_ref());
}
