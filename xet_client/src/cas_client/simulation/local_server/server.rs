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
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = LocalServerConfig {
//!         data_directory: "./data".into(),
//!         host: "127.0.0.1".to_string(),
//!         port: 8080,
//!         in_memory: false,
//!     };
//!     let server: LocalServer = LocalServer::new(config).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

use std::net::SocketAddr;
#[cfg(test)]
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use async_trait::async_trait;
use axum::Router;
use axum::routing::{get, head, post};
#[cfg(test)]
use reqwest::header::{self, HeaderMap, HeaderValue};
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::sync::oneshot;
use tower_http::cors::CorsLayer;

#[cfg(test)]
use super::super::super::RemoteClient;
#[cfg(test)]
use super::super::super::interface::Client;
#[cfg(test)]
#[cfg(unix)]
use super::super::socket_proxy::UnixSocketProxy;
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
    pub async fn new(config: LocalServerConfig) -> Result<Self> {
        let (client, deletion_client): (Arc<dyn DirectAccessClient>, Option<Arc<dyn DeletionControlableClient>>) =
            if config.in_memory {
                (MemoryClient::new(), None)
            } else {
                let client = LocalClient::new(&config.data_directory).await?;
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
            .route("/shards", post(handlers::post_shard))
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
pub struct LocalTestServer {
    endpoint: String,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
    remote_client: Arc<RemoteClient>,
    client: Arc<dyn DirectAccessClient>,
    deletion_client: Option<Arc<dyn DeletionControlableClient>>,

    #[cfg(unix)]
    _socket_proxy: Option<UnixSocketProxy>,
}

#[cfg(test)]
impl LocalTestServer {
    /// Starts a new test server.
    ///
    /// # Arguments
    /// * `in_memory` - If true, uses in-memory storage; if false, uses a temporary directory on disk.
    /// * `socket_path` - Optional Unix socket path. If provided, creates a proxy and connects RemoteClient through it.
    ///
    /// The server listens on a randomly assigned available port on localhost.
    pub async fn start(in_memory: bool) -> Self {
        Self::start_with_socket_proxy(in_memory, None).await
    }

    /// Starts a new test server with an optional socket proxy.
    ///
    /// # Arguments
    /// * `in_memory` - If true, uses in-memory storage; if false, uses a temporary directory on disk.
    /// * `socket_path` - Optional Unix socket path. If provided, creates a proxy and connects RemoteClient through it.
    ///
    /// The server listens on a randomly assigned available port on localhost.
    pub async fn start_with_socket_proxy(in_memory: bool, socket_path: Option<PathBuf>) -> Self {
        if in_memory {
            let client = MemoryClient::new();
            Self::start_with_client_and_socket(client, None, socket_path).await
        } else {
            let client = LocalClient::temporary().await.unwrap();
            let deletion_client: Arc<dyn DeletionControlableClient> = client.clone();
            Self::start_with_client_and_socket(client, Some(deletion_client), socket_path).await
        }
    }

    /// Starts a new test server using an existing `DirectAccessClient`.
    ///
    /// Useful when you need to pre-populate the client with data before starting the server.
    pub async fn start_with_client(client: Arc<dyn DirectAccessClient>) -> Self {
        Self::start_with_client_and_socket(client, None, None).await
    }

    /// Starts a new test server using an existing `DirectAccessClient` and optional
    /// deletion-capable client, with an optional socket proxy.
    pub async fn start_with_client_and_deletion(
        client: Arc<dyn DirectAccessClient>,
        deletion_client: Option<Arc<dyn DeletionControlableClient>>,
    ) -> Self {
        Self::start_with_client_and_socket(client, deletion_client, None).await
    }

    /// Starts a new test server using an existing `DirectAccessClient` with an optional socket proxy.
    async fn start_with_client_and_socket(
        client: Arc<dyn DirectAccessClient>,
        deletion_client: Option<Arc<dyn DeletionControlableClient>>,
        _socket_path: Option<PathBuf>,
    ) -> Self {
        let port = Self::find_available_port();
        let host = "127.0.0.1".to_string();
        let tcp_endpoint = format!("http://{}:{}", host, port);

        let server = LocalServer::from_client(client.clone(), deletion_client.clone(), host, port);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = server.run_until_stopped(shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_static("test-agent"));
        let (remote_client, _socket_proxy) = {
            #[cfg(unix)]
            {
                if let Some(socket_path) = _socket_path {
                    // Extract host:port from http://host:port
                    let tcp_addr = tcp_endpoint.strip_prefix("http://").unwrap_or(&tcp_endpoint).to_string();

                    let proxy = UnixSocketProxy::new(socket_path.clone(), tcp_addr)
                        .await
                        .expect("Failed to create Unix socket proxy");

                    tokio::time::sleep(Duration::from_millis(500)).await;

                    // Create RemoteClient with socket path
                    let socket_path_str = socket_path.to_string_lossy().to_string();
                    let client = RemoteClient::new_with_socket(
                        &tcp_endpoint,
                        &None,
                        "test-session",
                        false,
                        Some(&socket_path_str),
                        Some(Arc::new(headers)),
                    );

                    (client, Some(proxy))
                } else {
                    let client =
                        RemoteClient::new(&tcp_endpoint, &None, "test-session", false, Some(Arc::new(headers)));
                    (client, None)
                }
            }

            #[cfg(not(unix))]
            {
                let client = RemoteClient::new(&tcp_endpoint, &None, "test-session", false, None);
                (client, Option::<()>::None)
            }
        };

        Self {
            endpoint: tcp_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_client,
            client,
            deletion_client,
            #[cfg(unix)]
            _socket_proxy,
        }
    }

    /// Returns the HTTP endpoint URL (e.g., "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the `RemoteClient` configured to connect to this test server.
    pub fn remote_client(&self) -> &Arc<RemoteClient> {
        &self.remote_client
    }

    /// Returns the underlying `DirectAccessClient` for direct state access.
    pub fn client(&self) -> &Arc<dyn DirectAccessClient> {
        &self.client
    }

    /// Returns the deletion-capable client if the server backend supports it.
    #[allow(unused)]
    pub fn deletion_client(&self) -> Option<&Arc<dyn DeletionControlableClient>> {
        self.deletion_client.as_ref()
    }

    fn find_available_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }
}

#[cfg(test)]
#[async_trait]
impl Client for LocalTestServer {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<
        Option<(
            xet_core_structures::metadata_shard::file_structs::MDBFileInfo,
            Option<xet_core_structures::merklehash::MerkleHash>,
        )>,
    > {
        self.remote_client.get_file_reconstruction_info(file_hash).await
    }

    async fn get_reconstruction(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponseV2>> {
        self.remote_client.get_reconstruction(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(
        &self,
        file_ids: &[xet_core_structures::merklehash::MerkleHash],
    ) -> Result<crate::cas_types::BatchQueryReconstructionResponse> {
        self.remote_client.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<crate::cas_client::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn crate::cas_client::interface::URLProvider>,
        download_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
        progress_callback: Option<crate::cas_client::progress_tracked_streams::ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.remote_client
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<Option<bytes::Bytes>> {
        self.remote_client.query_for_global_dedup_shard(prefix, chunk_hash).await
    }

    async fn acquire_upload_permit(&self) -> Result<crate::cas_client::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_upload_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: bytes::Bytes,
        upload_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.remote_client.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: xet_core_structures::xorb_object::SerializedXorbObject,
        progress_callback: Option<crate::cas_client::progress_tracked_streams::ProgressCallback>,
        upload_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.remote_client
            .upload_xorb(prefix, serialized_xorb_object, progress_callback, upload_permit)
            .await
    }
}

#[cfg(test)]
#[async_trait]
impl DirectAccessClient for LocalTestServer {
    fn set_fetch_term_url_expiration(&self, expiration: std::time::Duration) {
        self.client.set_fetch_term_url_expiration(expiration);
    }

    fn set_global_dedup_shard_expiration(&self, expiration: Option<std::time::Duration>) {
        self.client.set_global_dedup_shard_expiration(expiration);
    }

    fn set_max_ranges_per_fetch(&self, max_ranges: usize) {
        self.client.set_max_ranges_per_fetch(max_ranges);
    }

    fn disable_v2_reconstruction(&self, status_code: u16) {
        self.client.disable_v2_reconstruction(status_code);
    }

    fn v2_disabled_status_code(&self) -> u16 {
        self.client.v2_disabled_status_code()
    }

    async fn get_reconstruction_v1(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponse>> {
        self.remote_client.get_reconstruction_v1(file_id, bytes_range).await
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponseV2>> {
        self.remote_client.get_reconstruction_v2(file_id, bytes_range).await
    }

    fn set_api_delay_range(&self, delay_range: Option<std::ops::Range<std::time::Duration>>) {
        self.client.set_api_delay_range(delay_range);
    }

    async fn apply_api_delay(&self) {
        self.client.apply_api_delay().await;
    }

    async fn list_xorbs(&self) -> Result<Vec<xet_core_structures::merklehash::MerkleHash>> {
        self.client.list_xorbs().await
    }

    async fn delete_xorb(&self, hash: &xet_core_structures::merklehash::MerkleHash) {
        self.client.delete_xorb(hash).await;
    }

    async fn get_full_xorb(&self, hash: &xet_core_structures::merklehash::MerkleHash) -> Result<bytes::Bytes> {
        self.client.get_full_xorb(hash).await
    }

    async fn get_xorb_ranges(
        &self,
        hash: &xet_core_structures::merklehash::MerkleHash,
        chunk_ranges: Vec<(u32, u32)>,
    ) -> Result<Vec<bytes::Bytes>> {
        self.client.get_xorb_ranges(hash, chunk_ranges).await
    }

    async fn xorb_length(&self, hash: &xet_core_structures::merklehash::MerkleHash) -> Result<u32> {
        self.client.xorb_length(hash).await
    }

    async fn xorb_exists(&self, hash: &xet_core_structures::merklehash::MerkleHash) -> Result<bool> {
        self.client.xorb_exists(hash).await
    }

    async fn xorb_footer(
        &self,
        hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<xet_core_structures::xorb_object::XorbObject> {
        self.client.xorb_footer(hash).await
    }

    async fn get_file_size(&self, hash: &xet_core_structures::merklehash::MerkleHash) -> Result<u64> {
        self.client.get_file_size(hash).await
    }

    async fn get_file_data(
        &self,
        hash: &xet_core_structures::merklehash::MerkleHash,
        byte_range: Option<crate::cas_types::FileRange>,
    ) -> Result<bytes::Bytes> {
        self.client.get_file_data(hash, byte_range).await
    }

    async fn get_xorb_raw_bytes(
        &self,
        hash: &xet_core_structures::merklehash::MerkleHash,
        byte_range: Option<crate::cas_types::FileRange>,
    ) -> Result<bytes::Bytes> {
        self.client.get_xorb_raw_bytes(hash, byte_range).await
    }

    async fn xorb_raw_length(&self, hash: &xet_core_structures::merklehash::MerkleHash) -> Result<u64> {
        self.client.xorb_raw_length(hash).await
    }

    async fn fetch_term_data(
        &self,
        hash: xet_core_structures::merklehash::MerkleHash,
        fetch_term: crate::cas_types::XorbReconstructionFetchInfo,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.client.fetch_term_data(hash, fetch_term).await
    }
}

#[cfg(test)]
impl Drop for LocalTestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use xet_core_structures::merklehash::MerkleHash;

    use super::*;
    use crate::cas_client::Client;
    use crate::cas_client::simulation::client_testing_utils::ClientTestingUtils;
    use crate::cas_client::simulation::local_server::SimulationControlClient;
    use crate::cas_client::simulation::{DeletionControlableClient, DirectAccessClient};
    use crate::cas_types::{FileRange, QueryReconstructionResponseV2};

    const CHUNK_SIZE: usize = 123;

    /// Verifies basic server operations: upload, reconstruction (full/range/batch/multi-xorb),
    /// file info, dedup queries, and fetch_term endpoint.
    async fn check_basic_correctness(server: &LocalTestServer) {
        // Upload via RemoteClient, verify via LocalClient
        let file = server
            .remote_client()
            .upload_random_file(&[(1, (0, 5))], CHUNK_SIZE)
            .await
            .unwrap();
        let local_data = server.client().get_file_data(&file.file_hash, None).await.unwrap();
        assert_eq!(file.data, local_data);

        // Full file reconstruction - compare remote and local (V1)
        let remote_recon = server
            .remote_client()
            .get_reconstruction_v1(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        let local_recon = server
            .client()
            .get_reconstruction_v1(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote_recon.terms.len(), local_recon.terms.len());
        assert_eq!(remote_recon.offset_into_first_range, local_recon.offset_into_first_range);
        for (remote_term, local_term) in remote_recon.terms.iter().zip(local_recon.terms.iter()) {
            assert_eq!(remote_term.hash, local_term.hash);
            assert_eq!(remote_term.range, local_term.range);
        }

        // Range reconstruction
        let file_size = file.data.len() as u64;
        let range = FileRange::new(file_size / 4, file_size * 3 / 4);
        let range_recon = server
            .remote_client()
            .get_reconstruction_v1(&file.file_hash, Some(range))
            .await
            .unwrap();
        assert!(range_recon.is_some());

        // Upload multi-xorb file
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let multi_file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();
        let multi_recon = server
            .remote_client()
            .get_reconstruction_v1(&multi_file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(multi_recon.terms.len(), 3);

        // Batch reconstruction
        let file_ids = vec![file.file_hash, multi_file.file_hash];
        let batch_result = server.remote_client().batch_get_reconstruction(&file_ids).await.unwrap();
        assert_eq!(batch_result.files.len(), 2);

        // File info (MDBFileInfo)
        let (remote_mdb, _) = server
            .remote_client()
            .get_file_reconstruction_info(&file.file_hash)
            .await
            .unwrap()
            .unwrap();
        let (local_mdb, _) = server
            .client()
            .get_file_reconstruction_info(&file.file_hash)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote_mdb.file_size(), local_mdb.file_size());

        // Dedup query - use chunk hash from RandomFileContents
        let first_chunk_hash = file.terms[0].chunk_hashes[0];
        let shard_result = server
            .remote_client()
            .query_for_global_dedup_shard("default", &first_chunk_hash)
            .await
            .unwrap();
        let local_shard = server
            .client()
            .query_for_global_dedup_shard("default", &first_chunk_hash)
            .await
            .unwrap();
        assert!(shard_result.is_some());
        assert_eq!(shard_result.unwrap(), local_shard.unwrap());

        // Fetch term endpoint - verify URLs are HTTP and data can be fetched
        let http_client = reqwest::Client::new();
        for fetch_infos in remote_recon.fetch_info.values() {
            for fi in fetch_infos {
                assert!(fi.url.starts_with("http://"));
                assert!(fi.url.contains("/fetch_term?term="));
                let response = http_client.get(&fi.url).send().await.unwrap();
                assert!(response.status().is_success());
                assert!(!response.bytes().await.unwrap().is_empty());
            }
        }

        // Fetch term with range request
        let first_fi = &remote_recon.fetch_info.values().next().unwrap()[0];
        let full_data = http_client.get(&first_fi.url).send().await.unwrap().bytes().await.unwrap();
        if full_data.len() > 100 {
            let range_resp = http_client
                .get(&first_fi.url)
                .header(reqwest::header::RANGE, "bytes=0-99")
                .send()
                .await
                .unwrap();
            assert!(range_resp.status().is_success());
            let range_data = range_resp.bytes().await.unwrap();
            assert_eq!(range_data.len(), 100);
            assert_eq!(&range_data[..], &full_data[..100]);
        }
    }

    /// Tests that invalid requests return appropriate error responses.
    async fn check_error_handling(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        // Nonexistent file hash for reconstruction
        let fake_hash = xet_core_structures::merklehash::MerkleHash::from_hex(
            "d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da",
        )
        .unwrap();
        let result = server.remote_client().get_reconstruction(&fake_hash, None).await;
        assert!(result.is_err() || result.unwrap().is_none());

        // Nonexistent file for file info
        let file_info = server.remote_client().get_file_reconstruction_info(&fake_hash).await;
        assert!(file_info.is_err() || file_info.unwrap().is_none());

        // Invalid fetch_term (valid base64 but nonexistent path)
        let invalid_term_url = format!("{}/v1/fetch_term?term=aW52YWxpZF9wYXRo", server.endpoint());
        let response = http_client.get(&invalid_term_url).send().await.unwrap();
        assert!(response.status().is_client_error() || response.status().is_server_error());

        // Malformed base64 in fetch_term
        let malformed_url = format!("{}/v1/fetch_term?term=not-valid-base64!!!", server.endpoint());
        let response = http_client.get(&malformed_url).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    /// Verifies that reconstruction responses contain valid HTTP URLs.
    async fn check_url_transformation(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        // Single XORB file
        let file1 = server.client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await.unwrap();

        // Multi-XORB file
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let multi_file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();

        // Verify single XORB URLs are HTTP
        let recon1 = server
            .remote_client()
            .get_reconstruction_v1(&file1.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        for (hash, fetch_infos) in &recon1.fetch_info {
            for fi in fetch_infos {
                assert!(
                    fi.url.starts_with("http://") || fi.url.starts_with("https://"),
                    "URL for hash {} should be HTTP, got: {}",
                    hash,
                    fi.url
                );
                assert!(fi.url.contains("/fetch_term?term="));
                assert!(!fi.url.contains("\":"));
            }
        }

        // Verify multi-XORB file has HTTP URLs for all XORBs
        let multi_recon = server
            .remote_client()
            .get_reconstruction_v1(&multi_file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert!(multi_recon.fetch_info.len() >= 2);
        for fetch_infos in multi_recon.fetch_info.values() {
            for fi in fetch_infos {
                assert!(fi.url.starts_with("http://"));
            }
        }

        // Verify partial range reconstruction has HTTP URLs
        let file_size = multi_file.data.len() as u64;
        let range = FileRange::new(file_size / 4, file_size * 3 / 4);
        let range_recon = server
            .remote_client()
            .get_reconstruction_v1(&multi_file.file_hash, Some(range))
            .await
            .unwrap()
            .unwrap();
        for fetch_infos in range_recon.fetch_info.values() {
            for fi in fetch_infos {
                assert!(fi.url.starts_with("http://"));
                assert!(fi.url.contains("/fetch_term?term="));
            }
        }

        // Verify all term URLs are fetchable
        for term in &recon1.terms {
            let fetch_infos = recon1.fetch_info.get(&term.hash).unwrap();
            for fi in fetch_infos {
                let response = http_client.get(&fi.url).send().await.unwrap();
                assert!(response.status().is_success());
                assert!(!response.bytes().await.unwrap().is_empty());
            }
        }
    }

    /// Verifies reconstruction term hashes match the uploaded file's expected terms.
    async fn check_reconstruction_term_hashes_match(server: &LocalTestServer) {
        // Upload a multi-term file
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();

        // Get reconstruction via remote client
        let recon = server
            .remote_client()
            .get_reconstruction_v1(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        // Verify term count matches
        assert_eq!(recon.terms.len(), file.terms.len());

        // Verify each term's XORB hash matches
        for (i, recon_term) in recon.terms.iter().enumerate() {
            let expected_term = &file.terms[i];
            assert_eq!(recon_term.hash.0, expected_term.xorb_hash, "Term {} XORB hash mismatch", i);
        }
    }

    /// Verifies that reconstruction data can be fetched and downloaded file matches expected data.
    async fn check_downloaded_terms_match_expected_data(server: &LocalTestServer) {
        // Upload a file with known term structure
        let term_spec = &[(1, (0, 4)), (2, (0, 3))];
        let file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();

        // Get reconstruction
        let recon = server
            .remote_client()
            .get_reconstruction_v1(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        // Verify term count and XORB hashes match
        assert_eq!(recon.terms.len(), file.terms.len());
        for (term_idx, recon_term) in recon.terms.iter().enumerate() {
            let expected_term = &file.terms[term_idx];
            assert_eq!(recon_term.hash.0, expected_term.xorb_hash);

            // Verify fetch_info exists for each XORB
            let fetch_infos = recon.fetch_info.get(&recon_term.hash).unwrap();
            assert!(!fetch_infos.is_empty());
        }

        // Verify the complete file can be retrieved correctly via LocalClient
        let retrieved_data = server.client().get_file_data(&file.file_hash, None).await.unwrap();
        assert_eq!(retrieved_data, file.data);

        // Verify term_matches works correctly for each term
        for (term_idx, term) in file.terms.iter().enumerate() {
            assert!(file.term_matches(term_idx, &term.data));
        }
    }

    /// Verifies that the complete file can be reconstructed by concatenating term data.
    async fn check_complete_file_reconstruction(server: &LocalTestServer) {
        // Upload a multi-term file
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();

        // Reconstruct file from term data
        let mut reconstructed = Vec::new();
        for term in &file.terms {
            reconstructed.extend_from_slice(&term.data);
        }

        // Verify it matches the original file data
        assert_eq!(reconstructed, file.data);
        assert!(file.term_matches(0, &file.terms[0].data));
        assert!(file.term_matches(1, &file.terms[1].data));
        assert!(file.term_matches(2, &file.terms[2].data));

        // Verify term_matches returns false for wrong data
        assert!(!file.term_matches(0, &file.terms[1].data));
    }

    /// Verifies chunk hashes in RandomFileContents match expected values.
    async fn check_chunk_hashes_correctness(server: &LocalTestServer) {
        let file = server.client().upload_random_file(&[(1, (0, 3))], CHUNK_SIZE).await.unwrap();

        // Verify we have the expected number of chunks
        assert_eq!(file.terms.len(), 1);
        assert_eq!(file.terms[0].chunk_hashes.len(), 3);

        // Verify chunk hashes match the RawXorbData cas_info (keyed by xorb hash)
        let xorb_hash = file.terms[0].xorb_hash;
        let raw_xorb = file.xorbs.get(&xorb_hash).unwrap();
        assert_eq!(raw_xorb.xorb_info.chunks.len(), 3);
        for (i, chunk_hash) in file.terms[0].chunk_hashes.iter().enumerate() {
            assert_eq!(*chunk_hash, raw_xorb.xorb_info.chunks[i].chunk_hash);
        }
    }

    /// Tests V2 reconstruction endpoint returns valid responses through the server.
    async fn check_v2_reconstruction(server: &LocalTestServer) {
        let file = server.client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await.unwrap();

        // Query V2 endpoint via remote client
        let v2 = server
            .remote_client()
            .get_reconstruction_v2(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        assert!(!v2.terms.is_empty());
        assert!(!v2.xorbs.is_empty());
        assert_eq!(v2.offset_into_first_range, 0);

        // V2 URLs should be HTTP URLs pointing to /v1/fetch_term
        for fetch_entries in v2.xorbs.values() {
            for fetch in fetch_entries {
                assert!(fetch.url.starts_with("http://"), "V2 URL should be HTTP, got: {}", fetch.url);
                assert!(
                    fetch.url.contains("/v1/fetch_term?term="),
                    "V2 URL should point to fetch_term endpoint, got: {}",
                    fetch.url
                );
            }
        }

        // V2 terms should match V1 terms
        let v1 = server
            .remote_client()
            .get_reconstruction_v1(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(v1.terms.len(), v2.terms.len());
        assert_eq!(v1.offset_into_first_range, v2.offset_into_first_range);
        for (t1, t2) in v1.terms.iter().zip(v2.terms.iter()) {
            assert_eq!(t1.hash, t2.hash);
            assert_eq!(t1.range, t2.range);
        }
    }

    /// Tests V2 fetch URLs are fetchable via the /v1/fetch_term endpoint.
    async fn check_v2_url_transformation(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        let file = server
            .client()
            .upload_random_file(&[(1, (0, 3)), (2, (0, 2))], CHUNK_SIZE)
            .await
            .unwrap();

        let v2 = server
            .remote_client()
            .get_reconstruction_v2(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        for fetch_entries in v2.xorbs.values() {
            for fetch in fetch_entries {
                let response = http_client.get(&fetch.url).send().await.unwrap();
                assert!(
                    response.status().is_success(),
                    "V2 fetch URL should be fetchable: {} (status: {})",
                    fetch.url,
                    response.status()
                );
                let data = response.bytes().await.unwrap();
                assert!(!data.is_empty(), "Fetched data should not be empty");
            }
        }
    }

    /// Tests V2 with range requests through the server.
    async fn check_v2_range_reconstruction(server: &LocalTestServer) {
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();
        let file_size = file.data.len() as u64;

        let range = FileRange::new(file_size / 4, file_size * 3 / 4);
        let v2 = server
            .remote_client()
            .get_reconstruction_v2(&file.file_hash, Some(range))
            .await
            .unwrap()
            .unwrap();

        assert!(!v2.terms.is_empty());
        for fetch_entries in v2.xorbs.values() {
            for fetch in fetch_entries {
                assert!(fetch.url.starts_with("http://"));
            }
        }

        // Validate open-ended and suffix range variants through the V2 HTTP endpoint.
        let v2_url = format!("{}/v2/reconstructions/{}", server.endpoint(), file.file_hash.hex());
        let http_client = reqwest::Client::new();

        let open_rhs: QueryReconstructionResponseV2 = http_client
            .get(&v2_url)
            .header(reqwest::header::RANGE, "bytes=100-")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!open_rhs.terms.is_empty());

        let suffix: QueryReconstructionResponseV2 = http_client
            .get(&v2_url)
            .header(reqwest::header::RANGE, "bytes=-128")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!suffix.terms.is_empty());
    }

    /// Tests V2 max_ranges_per_fetch through the server.
    async fn check_v2_max_ranges(server: &LocalTestServer) {
        let term_spec = &[(1, (0, 2)), (2, (0, 1)), (1, (2, 4)), (2, (1, 2)), (1, (4, 6))];
        let file = server.client().upload_random_file(term_spec, 512).await.unwrap();

        // Set max_ranges_per_fetch to 1
        server.set_max_ranges_per_fetch(1);

        let v2 = server
            .client()
            .get_reconstruction_v2(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();

        let xorb1_hash: crate::cas_types::HexMerkleHash = file.terms[0].xorb_hash.into();
        if let Some(desc) = v2.xorbs.get(&xorb1_hash) {
            for fetch in desc {
                assert!(fetch.ranges.len() <= 1, "Each fetch should have at most 1 range, got {}", fetch.ranges.len());
            }
        }

        // Reset
        server.set_max_ranges_per_fetch(usize::MAX);
    }

    /// Verifies that disabling V2 with various status codes causes the V2 endpoint
    /// to return that code, and that get_reconstruction falls back to V1.
    async fn check_v2_disabled_fallback(server: &LocalTestServer) {
        let file = server
            .remote_client()
            .upload_random_file(&[(1, (0, 3)), (2, (0, 2))], CHUNK_SIZE)
            .await
            .unwrap();

        // V2 should work before disabling.
        let v2_result = server.remote_client().get_reconstruction_v2(&file.file_hash, None).await;
        assert!(v2_result.is_ok());

        // Test 501 (Not Implemented) fallback first, before the RemoteClient
        // caches a V1 preference from a 404 fallback.
        server.disable_v2_reconstruction(501);

        let v2_result = server.remote_client().get_reconstruction_v2(&file.file_hash, None).await;
        assert!(v2_result.is_err(), "V2 should return error when disabled with 501");

        // Forced V2 should surface the endpoint error directly with no fallback.
        let forced_v2 = server
            .remote_client()
            .get_reconstruction_with_version_override(&file.file_hash, None, Some(2))
            .await;
        assert!(forced_v2.is_err());
        assert_eq!(forced_v2.unwrap_err().status(), Some(reqwest::StatusCode::NOT_IMPLEMENTED));

        // Forced V1 should continue to succeed when V2 is disabled.
        let forced_v1 = server
            .remote_client()
            .get_reconstruction_with_version_override(&file.file_hash, None, Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(forced_v1.terms.len(), 2);

        let result = server
            .remote_client()
            .get_reconstruction(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.terms.len(), 2);

        // Re-enable V2, then test 404 fallback.
        server.disable_v2_reconstruction(0);

        // Reset the RemoteClient's cached version by making a successful V2 call.
        let v2_result = server.remote_client().get_reconstruction_v2(&file.file_hash, None).await;
        assert!(v2_result.is_ok(), "V2 should work again after re-enabling");

        server.disable_v2_reconstruction(404);

        let v2_result = server.remote_client().get_reconstruction_v2(&file.file_hash, None).await;
        assert!(v2_result.is_err(), "V2 should return error when disabled with 404");

        let forced_v2 = server
            .remote_client()
            .get_reconstruction_with_version_override(&file.file_hash, None, Some(2))
            .await;
        assert!(forced_v2.is_err());
        assert_eq!(forced_v2.unwrap_err().status(), Some(reqwest::StatusCode::NOT_FOUND));

        let forced_v1 = server
            .remote_client()
            .get_reconstruction_with_version_override(&file.file_hash, None, Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(forced_v1.terms.len(), 2);

        let result = server
            .remote_client()
            .get_reconstruction(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.terms.len(), 2);
    }

    /// Runs all server checks for a given test server instance.
    async fn run_all_server_checks(server: &LocalTestServer) {
        check_basic_correctness(server).await;
        check_error_handling(server).await;
        check_url_transformation(server).await;
        check_reconstruction_term_hashes_match(server).await;
        check_downloaded_terms_match_expected_data(server).await;
        check_complete_file_reconstruction(server).await;
        check_chunk_hashes_correctness(server).await;
        check_v2_reconstruction(server).await;
        check_v2_url_transformation(server).await;
        check_v2_range_reconstruction(server).await;
        check_v2_max_ranges(server).await;
        check_v2_disabled_fallback(server).await;
    }

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

    /// Main test that runs all server checks with both in-memory and disk-backed storage.
    #[tokio::test]
    async fn test_local_server() {
        // Test with in-memory storage
        {
            tracing::info!("Testing with in-memory storage");
            let server = LocalTestServer::start(true).await;
            assert!(server.client().list_xorbs().await.unwrap().is_empty());
            run_all_server_checks(&server).await;
        }

        // Test with disk-backed storage
        {
            tracing::info!("Testing with disk-backed storage");
            let server = LocalTestServer::start(false).await;
            assert!(server.client().list_xorbs().await.unwrap().is_empty());
            run_all_server_checks(&server).await;
        }
    }

    /// Integration test: creates a LocalClient, wraps it in a LocalTestServer,
    /// uploads via remote client, then uses the held LocalClient reference for
    /// deletion controls.
    #[tokio::test]
    async fn test_deletion_lifecycle_via_server() {
        let lc = LocalClient::temporary().await.unwrap();
        let server = LocalTestServer::start_with_client(lc.clone()).await;

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

    /// Keeps a LocalTestServer alive for the duration of the tokio runtime by
    /// moving it into a spawned task. Returns the endpoint URL.
    fn detach_server(server: LocalTestServer) -> String {
        let endpoint = server.endpoint().to_string();
        tokio::spawn(async move {
            let _server = server;
            futures::future::pending::<()>().await;
        });
        endpoint
    }

    /// Runs the common DirectAccessClient test suite via SimulationControlClient.
    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_simulation_control_client_common_suite() {
        crate::cas_client::simulation::client_unit_testing::test_client_functionality(|| async {
            let lc = LocalClient::temporary().await.unwrap();
            let dc: Arc<dyn DeletionControlableClient> = lc.clone();
            let server = LocalTestServer::start_with_client_and_deletion(lc, Some(dc)).await;
            let endpoint = detach_server(server);
            Arc::new(SimulationControlClient::new(&endpoint)) as Arc<dyn DirectAccessClient>
        })
        .await;
    }

    /// Runs the common DeletionControlableClient test suite via SimulationControlClient.
    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_simulation_control_client_deletion_suite() {
        crate::cas_client::simulation::deletion_unit_testing::test_deletion_functionality(|| async {
            let lc = LocalClient::temporary().await.unwrap();
            let dc: Arc<dyn DeletionControlableClient> = lc.clone();
            let server = LocalTestServer::start_with_client_and_deletion(lc, Some(dc)).await;
            let endpoint = detach_server(server);
            Arc::new(SimulationControlClient::new(&endpoint))
        })
        .await;
    }

    /// Tests that deletion routes return 501 when the backend is MemoryClient.
    #[tokio::test]
    async fn test_simulation_control_client_501_on_memory_backend() {
        let server = LocalTestServer::start(true).await;
        let sc = SimulationControlClient::new(server.endpoint());

        // DirectAccessClient methods should still work
        let xorbs = DirectAccessClient::list_xorbs(&sc).await.unwrap();
        assert!(xorbs.is_empty());

        // DeletionControlableClient methods should return errors (501)
        assert!(DeletionControlableClient::list_shard_entries(&sc).await.is_err());
        assert!(DeletionControlableClient::list_file_shard_entries(&sc).await.is_err());
        assert!(DeletionControlableClient::verify_integrity(&sc).await.is_err());
        assert!(
            DeletionControlableClient::delete_shard_entry(&sc, &xet_core_structures::merklehash::MerkleHash::default())
                .await
                .is_err()
        );
        assert!(
            DeletionControlableClient::delete_file_entry(&sc, &xet_core_structures::merklehash::MerkleHash::default())
                .await
                .is_err()
        );
        assert!(
            DeletionControlableClient::get_shard_bytes(&sc, &xet_core_structures::merklehash::MerkleHash::default())
                .await
                .is_err()
        );
    }

    /// Tests that LocalTestServerBuilder with ephemeral disk correctly wires the deletion client,
    /// so deletion-control routes work through the HTTP layer (not 501).
    #[tokio::test]
    async fn test_builder_ephemeral_disk_deletion_wired() {
        use crate::cas_client::simulation::LocalTestServerBuilder;

        let server = LocalTestServerBuilder::new().with_ephemeral_disk().start().await;
        let sc = SimulationControlClient::new(server.http_endpoint());

        let file = sc.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

        let shards = DeletionControlableClient::list_shard_entries(&sc).await.unwrap();
        assert!(!shards.is_empty(), "list_shard_entries should work, not return 501");

        let file_entries = DeletionControlableClient::list_file_shard_entries(&sc).await.unwrap();
        assert_eq!(file_entries.len(), 1);
        assert_eq!(file_entries[0].0, file.file_hash);
        let shard_hash = file_entries[0].1;

        DeletionControlableClient::verify_integrity(&sc).await.unwrap();

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
