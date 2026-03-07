//! Integration tests for the shard upload per-request timeout feature (XET-885).
//!
//! Uses test_set_config! to set a short shard_read_timeout (1s) and disable retries,
//! then uses LocalTestServer with set_api_delay_range to simulate slow server responses.

use std::time::Duration;

use cas_client::LocalTestServer;
use cas_client::simulation::ClientTestingUtils;
use utils::test_set_config;

test_set_config! {
    client {
        shard_read_timeout = Duration::from_secs(1);
        retry_max_attempts = 1usize;
        retry_base_delay = Duration::from_millis(10);
    }
}

const CHUNK_SIZE: usize = 123;

#[tokio::test]
async fn test_shard_upload_succeeds_within_timeout() {
    let server = LocalTestServer::start(true).await;

    let result = server.remote_client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await;

    assert!(result.is_ok(), "Shard upload should succeed with no server delay: {result:?}");
}

#[tokio::test]
async fn test_shard_upload_times_out_when_server_slow() {
    let server = LocalTestServer::start(true).await;

    server
        .client()
        .set_api_delay_range(Some(Duration::from_secs(3)..Duration::from_secs(3)));

    let result = server.remote_client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await;

    assert!(result.is_err(), "Shard upload should fail with timeout when server is slow");
}
