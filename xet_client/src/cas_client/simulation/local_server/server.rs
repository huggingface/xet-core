//! Local CAS Server Implementation
//!
//! This module provides `LocalServer`, an HTTP server that wraps `LocalClient`
//! and exposes its functionality via REST API endpoints compatible with `RemoteClient`.
//!
//! # Architecture
//!
//! The server uses Axum as its HTTP framework and shares an `Arc<LocalClient>`
//! across all request handlers. Routes are organized to match the API expected
//! by `RemoteClient`, with some legacy route aliases for compatibility.
//!
//! # Example
//!
//! ```no_run
//! use anyhow::Result;
//! use xet_client::cas_client::{LocalServer, LocalServerConfig};
//! use xet_runtime::core::XetContext;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let ctx = XetContext::default().unwrap();
//!     let config = LocalServerConfig {
//!         data_directory: "./data".into(),
//!         host: "127.0.0.1".to_string(),
//!         port: 8080,
//!         in_memory: false,
//!     };
//!     let server: LocalServer = LocalServer::new(ctx, config).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, head, post};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use xet_runtime::core::XetContext;

use super::super::{DeletionControlableClient, DirectAccessClient, LocalClient, MemoryClient};
use super::handlers;
use super::latency_simulation::LatencySimulation;
use crate::error::{ClientError, Result};

/// Configuration for the local CAS server.
#[derive(Debug, Clone)]
pub struct LocalServerConfig {
    /// Directory where CAS data (XORBs, shards, indices) will be stored.
    /// Only used when `in_memory` is false.
    pub data_directory: PathBuf,
    /// Network interface to bind to (e.g., "127.0.0.1" or "0.0.0.0").
    pub host: String,
    /// TCP port number for the HTTP server.
    pub port: u16,
    /// Whether to use in-memory storage instead of disk-backed storage.
    pub in_memory: bool,
}

impl Default for LocalServerConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::from("./local_cas_data"),
            host: "127.0.0.1".to_string(),
            port: 8080,
            in_memory: false,
        }
    }
}

/// A local HTTP server that wraps a `DirectAccessClient` and exposes CAS operations via REST API.
///
/// This server implements the same API that `RemoteClient` expects, making it useful for:
/// - Integration testing without a remote backend
/// - Local development and debugging
/// - Offline CAS workflows
///
/// The server can use either a disk-backed `LocalClient` or an in-memory `MemoryClient`.
pub struct LocalServer {
    config: LocalServerConfig,
    client: Arc<dyn DirectAccessClient>,
    deletion_client: Option<Arc<dyn DeletionControlableClient>>,
    latency_simulation: Arc<LatencySimulation>,
}

impl LocalServer {
    /// Creates a new server with the given configuration.
    ///
    /// If `in_memory` is false, creates a new `LocalClient` pointing to the configured data directory.
    /// If `in_memory` is true, creates a new `MemoryClient` (data directory is ignored).
    pub async fn new(ctx: XetContext, config: LocalServerConfig) -> Result<Self> {
        let (client, deletion_client): (Arc<dyn DirectAccessClient>, Option<Arc<dyn DeletionControlableClient>>) =
            if config.in_memory {
                let client = MemoryClient::new(ctx.clone());
                let deletion_client = client.clone() as Arc<dyn DeletionControlableClient>;
                (client, Some(deletion_client))
            } else {
                let client = LocalClient::new(ctx, &config.data_directory).await?;
                let deletion_client = client.clone() as Arc<dyn DeletionControlableClient>;
                (client, Some(deletion_client))
            };
        let latency_simulation = LatencySimulation::new();
        Ok(Self {
            config,
            client,
            deletion_client,
            latency_simulation,
        })
    }

    /// Creates a server from an existing `DirectAccessClient`.
    ///
    /// Useful when you want to share a client instance between the server
    /// and other code (e.g., for testing where you want to verify server behavior
    /// against direct client access).
    pub fn from_client(
        client: Arc<dyn DirectAccessClient>,
        deletion_client: Option<Arc<dyn DeletionControlableClient>>,
        host: String,
        port: u16,
    ) -> Self {
        let latency_simulation = LatencySimulation::new();
        Self {
            config: LocalServerConfig {
                data_directory: PathBuf::new(),
                host,
                port,
                in_memory: false,
            },
            client,
            deletion_client,
            latency_simulation,
        }
    }

    /// Returns a clone of the underlying client.
    pub fn client(&self) -> Arc<dyn DirectAccessClient> {
        self.client.clone()
    }

    /// Returns the server's bind address as "host:port".
    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    /// Builds the Axum router with all CAS API routes.
    ///
    /// Routes follow the pattern used by RemoteClient:
    /// - `/v1/` prefixed routes for chunks, xorbs, reconstructions, and files
    /// - Root-level `/reconstructions` for batch queries and `/shards` for uploads
    /// - `/simulation/` prefixed routes for testing/simulation configuration and direct access
    fn create_router(&self) -> Router {
        Router::new()
            .route("/health", get(handlers::health_check))
            .nest(
                "/v1",
                Router::new()
                    .route("/reconstructions", get(handlers::batch_get_reconstruction))
                    .route("/reconstructions/{file_id}", get(handlers::get_reconstruction))
                    .route("/chunks/{prefix}/{hash}", get(handlers::get_dedup_info_by_chunk))
                    .route("/xorbs/{prefix}/{hash}", head(handlers::head_xorb).post(handlers::post_xorb))
                    .route("/shards", post(handlers::post_shard))
                    .route("/files/{file_id}", head(handlers::head_file))
                    .route("/get_xorb/{prefix}/{hash}/", get(handlers::get_file_term_data))
                    .route("/fetch_term", get(handlers::fetch_term)),
            )
            .nest("/v2", Router::new().route("/reconstructions/{file_id}", get(handlers::get_reconstruction_v2)))
            .nest(
                "/simulation",
                super::simulation_handlers::simulation_routes()
                    .route("/ping", get(handlers::ping))
                    .route("/set_config", post(handlers::set_config))
                    .route("/dummy_upload", post(handlers::dummy_upload)),
            )
            // Routes used by RemoteClient without /v1/ prefix
            .route("/reconstructions", get(handlers::batch_get_reconstruction))
            .layer(CorsLayer::very_permissive())
            .with_state(handlers::ServerState {
                client: self.client.clone(),
                latency_simulation: self.latency_simulation.clone(),
                deletion_client: self.deletion_client.clone(),
            })
    }

    /// Runs the server, listening for incoming HTTP requests.
    ///
    /// This method blocks until the server is shut down via signal (Ctrl+C on Unix).
    /// For programmatic shutdown, use `run_until_stopped` instead.
    pub async fn run(&self) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| ClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| ClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| ClientError::Other(format!("Server error: {e}")))
    }

    /// Runs the server until a shutdown signal is received on the provided channel.
    ///
    /// This is useful for tests where you want programmatic control over server lifecycle.
    pub async fn run_until_stopped(&self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| ClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| ClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| ClientError::Other(format!("Server error: {e}")))
    }
}

/// Waits for a shutdown signal (currently blocks forever as there's no SIGTERM handling).
async fn shutdown_signal() {
    std::future::pending::<()>().await
}


#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use xet_core_structures::merklehash::MerkleHash;
    use xet_runtime::core::XetContext;

    use crate::cas_client::Client;
    use crate::cas_client::simulation::LocalTestServerBuilder;
    use crate::cas_client::simulation::client_testing_utils::ClientTestingUtils;
    use crate::cas_client::simulation::local_server::SimulationControlClient;
    use crate::cas_client::simulation::{DeletionControlableClient, DirectAccessClient, LocalClient};

    const CHUNK_SIZE: usize = 123;

    async fn all_file_hashes(client: &LocalClient) -> HashSet<MerkleHash> {
        client
            .list_file_shard_entries()
            .await
            .unwrap()
            .into_iter()
            .map(|(fh, _)| fh)
            .collect()
    }

    async fn all_xorb_hashes(client: &LocalClient) -> HashSet<MerkleHash> {
        client.list_xorbs().await.unwrap().into_iter().collect()
    }

    /// Integration test: creates a LocalClient, wraps it in a LocalTestServer,
    /// uploads via remote client, then uses the held LocalClient reference for
    /// deletion controls.
    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_deletion_lifecycle_via_server() {
        let ctx = XetContext::default().expect("XetContext::new");
        let lc = LocalClient::temporary(ctx.clone()).await.unwrap();
        let server = LocalTestServerBuilder::new().with_client(lc.clone()).start().await;

        // Upload files via remote client (goes through HTTP server)
        let file1 = server
            .remote_client()
            .upload_random_file(&[(10, (0, 3))], CHUNK_SIZE)
            .await
            .unwrap();
        let file2 = server
            .remote_client()
            .upload_random_file(&[(20, (0, 2))], CHUNK_SIZE)
            .await
            .unwrap();

        let all_files: HashSet<_> = [file1.file_hash, file2.file_hash].into();
        let all_xorbs: HashSet<_> = [&file1, &file2]
            .iter()
            .flat_map(|f| f.terms.iter().map(|t| t.xorb_hash))
            .collect();

        // Verify everything is consistent
        lc.verify_integrity().await.unwrap();

        assert_eq!(all_file_hashes(&lc).await, all_files);
        assert_eq!(all_xorb_hashes(&lc).await, all_xorbs);

        // Verify files are retrievable
        let data1 = server.client().get_file_data(&file1.file_hash, None).await.unwrap();
        assert_eq!(data1, file1.data);

        // Step 1: Delete file entries -- shards and xorbs remain
        lc.delete_file_entry(&file1.file_hash).await.unwrap();

        assert_eq!(all_file_hashes(&lc).await, HashSet::from([file2.file_hash]));

        lc.delete_file_entry(&file2.file_hash).await.unwrap();
        assert!(lc.list_file_shard_entries().await.unwrap().is_empty());
        assert!(!lc.list_shard_entries().await.unwrap().is_empty());

        assert_eq!(all_xorb_hashes(&lc).await, all_xorbs);

        // Step 2: Delete shards -- xorbs remain
        let shard_hashes = lc.list_shard_entries().await.unwrap();
        for h in &shard_hashes {
            lc.delete_shard_entry(h).await.unwrap();
        }

        assert!(lc.list_shard_entries().await.unwrap().is_empty());
        assert_eq!(all_xorb_hashes(&lc).await, all_xorbs);

        // Step 3: Delete xorbs -- everything gone
        for h in &all_xorbs {
            lc.delete_xorb(h).await;
        }

        assert!(lc.list_xorbs().await.unwrap().is_empty());
    }

    /// Runs the common DirectAccessClient test suite via SimulationControlClient.
    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_simulation_control_client_common_suite() {
        crate::cas_client::simulation::client_unit_testing::test_client_functionality(|| async {
            let ctx = XetContext::default().expect("XetContext::new");
            let lc = LocalClient::temporary(ctx.clone()).await.unwrap();
            let dc: Arc<dyn DeletionControlableClient> = lc.clone();
            let server = LocalTestServerBuilder::new()
                .with_client(lc)
                .with_deletion_client(dc)
                .start()
                .await;
            let endpoint = server.http_endpoint().to_string();
            Arc::new(SimulationControlClient::new(ctx, &endpoint).with_keep_alive(server))
                as Arc<dyn DirectAccessClient>
        })
        .await;
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_simulation_control_client_config_eventual_apply() {
        let server = LocalTestServerBuilder::new().with_ephemeral_disk().start().await;
        let ctx = XetContext::default().expect("XetContext::new");
        let sc = SimulationControlClient::new(ctx, server.http_endpoint());

        let file = sc.upload_random_file(&[(1, (0, 4))], CHUNK_SIZE).await.unwrap();
        let first_chunk = file.terms[0].chunk_hashes[0];

        sc.set_global_dedup_shard_expiration(Some(Duration::from_millis(1)));

        let mut expiration_enabled = false;
        for _ in 0..40 {
            let shard_bytes = Client::query_for_global_dedup_shard(&sc, "default", &first_chunk)
                .await
                .unwrap()
                .unwrap();

            let minimal_shard = xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard::from_reader(
                &mut std::io::Cursor::new(&shard_bytes),
                true,
                true,
            )
            .unwrap();
            let shard_info = xet_core_structures::metadata_shard::MDBShardInfo::load_from_reader(
                &mut std::io::Cursor::new(&shard_bytes),
            )
            .unwrap();

            if minimal_shard.num_files() == 0 && shard_info.metadata.shard_key_expiry > 0 {
                expiration_enabled = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(expiration_enabled);
    }

    /// Runs the common DeletionControlableClient test suite via SimulationControlClient.
    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_simulation_control_client_deletion_suite() {
        crate::cas_client::simulation::deletion_unit_testing::test_deletion_functionality(|| async {
            let ctx = XetContext::default().expect("XetContext::new");
            let lc = LocalClient::temporary(ctx.clone()).await.unwrap();
            let dc: Arc<dyn DeletionControlableClient> = lc.clone();
            let server = LocalTestServerBuilder::new()
                .with_client(lc)
                .with_deletion_client(dc)
                .start()
                .await;
            let endpoint = server.http_endpoint().to_string();
            Arc::new(SimulationControlClient::new(ctx, &endpoint).with_keep_alive(server))
        })
        .await;
    }

    /// Tests that deletion routes are available when the backend is MemoryClient.
    #[tokio::test]
    async fn test_simulation_control_client_deletion_on_memory_backend() {
        let server = LocalTestServerBuilder::new().start().await;
        let ctx = XetContext::default().expect("XetContext::new");
        let sc = SimulationControlClient::new(ctx, server.http_endpoint());

        // DirectAccessClient methods should work.
        let xorbs = DirectAccessClient::list_xorbs(&sc).await.unwrap();
        assert!(xorbs.is_empty());

        // DeletionControlableClient methods should be wired and functional.
        let file = sc.upload_random_file(&[(1, (0, 2))], 1024).await.unwrap();
        let shard_entries = DeletionControlableClient::list_shard_entries(&sc).await.unwrap();
        assert_eq!(shard_entries.len(), 1);

        let file_entries = DeletionControlableClient::list_file_shard_entries(&sc).await.unwrap();
        assert_eq!(file_entries.len(), 1);
        assert_eq!(file_entries[0].0, file.file_hash);

        sc.verify_integrity().await.unwrap();
    }

    /// Tests that LocalTestServerBuilder with ephemeral disk correctly wires the deletion client,
    /// so deletion-control routes work through the HTTP layer (not 501).
    #[tokio::test]
    async fn test_builder_ephemeral_disk_deletion_wired() {
        let server = LocalTestServerBuilder::new().with_ephemeral_disk().start().await;
        let ctx = XetContext::default().expect("XetContext::new");
        let sc = SimulationControlClient::new(ctx, server.http_endpoint());

        let file = sc.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

        let shards = DeletionControlableClient::list_shard_entries(&sc).await.unwrap();
        assert!(!shards.is_empty(), "list_shard_entries should work, not return 501");

        let file_entries = DeletionControlableClient::list_file_shard_entries(&sc).await.unwrap();
        assert_eq!(file_entries.len(), 1);
        assert_eq!(file_entries[0].0, file.file_hash);
        let shard_hash = file_entries[0].1;

        sc.verify_integrity().await.unwrap();

        let first_chunk = file.terms[0].chunk_hashes[0];
        assert!(
            Client::query_for_global_dedup_shard(&sc, "default", &first_chunk)
                .await
                .unwrap()
                .is_some()
        );

        DeletionControlableClient::remove_shard_dedup_entries(&sc, &shard_hash)
            .await
            .unwrap();
        assert!(
            Client::query_for_global_dedup_shard(&sc, "default", &first_chunk)
                .await
                .unwrap()
                .is_none(),
            "dedup entries for the shard should be removable through builder-wired HTTP routes"
        );

        DeletionControlableClient::delete_file_entry(&sc, &file.file_hash)
            .await
            .unwrap();
        let file_entries_after = DeletionControlableClient::list_file_shard_entries(&sc).await.unwrap();
        assert!(file_entries_after.is_empty(), "Deleted file should be hidden");
    }
}
