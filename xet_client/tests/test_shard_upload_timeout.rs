//! Integration tests for the shard upload no-read-timeout client (XET-885).
//!
//! Verifies that shard uploads succeed even when the server takes a long time to process,
//! since the shard upload client has no read_timeout.

use std::time::Duration;

use xet_client::cas_client::simulation::ClientTestingUtils;
use xet_client::cas_client::{DirectAccessClient, LocalTestServerBuilder};
use xet_runtime::test_set_config;

test_set_config! {
    client {
        retry_max_attempts = 1usize;
        retry_base_delay = Duration::from_millis(10);
    }
}

const CHUNK_SIZE: usize = 123;

#[tokio::test]
async fn test_shard_upload_succeeds_with_no_server_delay() {
    let server = LocalTestServerBuilder::new().start().await;

    let result = server.remote_client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await;

    assert!(result.is_ok(), "Shard upload should succeed with no server delay: {result:?}");
}

#[tokio::test]
#[cfg_attr(feature = "smoke-test", ignore)]
async fn test_shard_upload_succeeds_with_slow_server() {
    let server = LocalTestServerBuilder::new().start().await;

    // Server takes 3s to respond — shard upload client has no read_timeout so this should succeed
    server.set_api_delay_range(Some(Duration::from_secs(3)..Duration::from_secs(3)));

    let result = server.remote_client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await;

    assert!(result.is_ok(), "Shard upload should succeed even with slow server (no read_timeout): {result:?}");
}
