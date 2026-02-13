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
//! use cas_client::{LocalServer, LocalServerConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = LocalServerConfig {
//!         data_directory: "./data".into(),
//!         host: "127.0.0.1".to_string(),
//!         port: 8080,
//!         in_memory: false,
//!     };
//!     let server = LocalServer::new(config).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::routing::{get, head, post};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::cors::CorsLayer;

use super::handlers;
use crate::RemoteClient;
use crate::error::{CasClientError, Result};
use crate::interface::Client;
#[cfg(unix)]
use crate::simulation::socket_proxy::UnixSocketProxy;
use crate::simulation::{DirectAccessClient, LocalClient, MemoryClient};

/// Configuration for the local CAS server.
#[derive(Clone, Debug)]
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
}

impl LocalServer {
    /// Creates a new server with the given configuration.
    ///
    /// If `in_memory` is false, creates a new `LocalClient` pointing to the configured data directory.
    /// If `in_memory` is true, creates a new `MemoryClient` (data directory is ignored).
    pub async fn new(config: LocalServerConfig) -> Result<Self> {
        let client: Arc<dyn DirectAccessClient> = if config.in_memory {
            MemoryClient::new()
        } else {
            LocalClient::new(&config.data_directory).await?
        };
        Ok(Self { config, client })
    }

    /// Creates a server from an existing `DirectAccessClient`.
    ///
    /// Useful when you want to share a client instance between the server
    /// and other code (e.g., for testing where you want to verify server behavior
    /// against direct client access).
    pub fn from_client(client: Arc<dyn DirectAccessClient>, host: String, port: u16) -> Self {
        Self {
            config: LocalServerConfig {
                data_directory: PathBuf::new(),
                host,
                port,
                in_memory: false,
            },
            client,
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
            // Routes used by RemoteClient without /v1/ prefix
            .route("/reconstructions", get(handlers::batch_get_reconstruction))
            .route("/shards", post(handlers::post_shard))
            .layer(CorsLayer::very_permissive())
            .with_state(self.client.clone())
    }

    /// Runs the server, listening for incoming HTTP requests.
    ///
    /// This method blocks until the server is shut down via signal (Ctrl+C on Unix).
    /// For programmatic shutdown, use `run_until_stopped` instead.
    pub async fn run(&self) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| CasClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| CasClientError::Other(format!("Server error: {e}")))
    }

    /// Runs the server until a shutdown signal is received on the provided channel.
    ///
    /// This is useful for tests where you want programmatic control over server lifecycle.
    pub async fn run_until_stopped(&self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| CasClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| CasClientError::Other(format!("Server error: {e}")))
    }
}

/// Waits for a shutdown signal (currently blocks forever as there's no SIGTERM handling).
async fn shutdown_signal() {
    std::future::pending::<()>().await
}

/// A test server that wraps `LocalServer` and provides easy access to both
/// `RemoteClient` (for HTTP interactions) and `DirectAccessClient` (for direct state access).
///
/// This is useful for integration tests where you want to verify that operations
/// through the HTTP API produce the same results as direct client access.
///
/// The server runs as a spawned tokio task and automatically shuts down when dropped
/// (no explicit shutdown call needed).
///
/// # Example
///
/// ```ignore
/// // Start with disk-backed storage
/// let server = LocalTestServer::start(false).await;
///
/// // Start with in-memory storage
/// let server = LocalTestServer::start(true).await;
///
/// // Upload via RemoteClient
/// let file = server.remote_client().upload_random_file(&[(1, (0, 5))], 123).await?;
///
/// // Verify via DirectAccessClient
/// let stored = server.client().get_file_data(&file.file_hash, None).await?;
/// assert_eq!(file.data, stored);
/// // Server automatically shuts down when dropped
/// ```
pub struct LocalTestServer {
    endpoint: String,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
    remote_client: Arc<RemoteClient>,
    client: Arc<dyn DirectAccessClient>,

    #[cfg(unix)]
    _socket_proxy: Option<UnixSocketProxy>,
}

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
        let client: Arc<dyn DirectAccessClient> = if in_memory {
            MemoryClient::new()
        } else {
            LocalClient::temporary().await.unwrap()
        };
        Self::start_with_client_and_socket(client, socket_path).await
    }

    /// Starts a new test server using an existing `DirectAccessClient`.
    ///
    /// Useful when you need to pre-populate the client with data before starting the server.
    pub async fn start_with_client(client: Arc<dyn DirectAccessClient>) -> Self {
        #[cfg(unix)]
        {
            Self::start_with_client_and_socket(client, None).await
        }
        #[cfg(not(unix))]
        {
            Self::start_with_client_and_socket(client, None).await
        }
    }

    /// Starts a new test server using an existing `DirectAccessClient` with an optional socket proxy.
    ///
    /// Useful when you need to pre-populate the client with data before starting the server.
    async fn start_with_client_and_socket(client: Arc<dyn DirectAccessClient>, socket_path: Option<PathBuf>) -> Self {
        let port = Self::find_available_port();
        let host = "127.0.0.1".to_string();
        let tcp_endpoint = format!("http://{}:{}", host, port);

        let server = LocalServer::from_client(client.clone(), host, port);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = server.run_until_stopped(shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let (remote_client, socket_proxy) = {
            #[cfg(unix)]
            {
                if let Some(socket_path) = socket_path {
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
                        "test-agent",
                        Some(&socket_path_str),
                    );

                    (client, Some(proxy))
                } else {
                    let client = RemoteClient::new(&tcp_endpoint, &None, "test-session", false, "test-agent");
                    (client, None)
                }
            }

            #[cfg(not(unix))]
            {
                let client = RemoteClient::new(&tcp_endpoint, &None, "test-session", false, "test-agent");
                (client, Option::<()>::None)
            }
        };

        Self {
            endpoint: tcp_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_client,
            client,
            #[cfg(unix)]
            _socket_proxy: socket_proxy,
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

    fn find_available_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }
}

#[async_trait]
impl Client for LocalTestServer {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(mdb_shard::file_structs::MDBFileInfo, Option<merklehash::MerkleHash>)>> {
        self.remote_client.get_file_reconstruction_info(file_hash).await
    }

    async fn get_reconstruction(
        &self,
        file_id: &merklehash::MerkleHash,
        bytes_range: Option<cas_types::FileRange>,
    ) -> Result<Option<cas_types::QueryReconstructionResponse>> {
        self.remote_client.get_reconstruction(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(
        &self,
        file_ids: &[merklehash::MerkleHash],
    ) -> Result<cas_types::BatchQueryReconstructionResponse> {
        self.remote_client.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn crate::interface::URLProvider>,
        download_permit: crate::adaptive_concurrency::ConnectionPermit,
        progress_callback: Option<crate::ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.remote_client
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &merklehash::MerkleHash,
    ) -> Result<Option<bytes::Bytes>> {
        self.remote_client.query_for_global_dedup_shard(prefix, chunk_hash).await
    }

    async fn acquire_upload_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_upload_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: bytes::Bytes,
        upload_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.remote_client.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: cas_object::SerializedCasObject,
        progress_callback: Option<crate::ProgressCallback>,
        upload_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.remote_client
            .upload_xorb(prefix, serialized_cas_object, progress_callback, upload_permit)
            .await
    }
}

#[async_trait]
impl DirectAccessClient for LocalTestServer {
    fn set_fetch_term_url_expiration(&self, expiration: std::time::Duration) {
        self.client.set_fetch_term_url_expiration(expiration);
    }

    fn set_api_delay_range(&self, delay_range: Option<std::ops::Range<std::time::Duration>>) {
        self.client.set_api_delay_range(delay_range);
    }

    async fn list_xorbs(&self) -> Result<Vec<merklehash::MerkleHash>> {
        self.client.list_xorbs().await
    }

    async fn delete_xorb(&self, hash: &merklehash::MerkleHash) {
        self.client.delete_xorb(hash).await;
    }

    async fn get_full_xorb(&self, hash: &merklehash::MerkleHash) -> Result<bytes::Bytes> {
        self.client.get_full_xorb(hash).await
    }

    async fn get_xorb_ranges(
        &self,
        hash: &merklehash::MerkleHash,
        chunk_ranges: Vec<(u32, u32)>,
    ) -> Result<Vec<bytes::Bytes>> {
        self.client.get_xorb_ranges(hash, chunk_ranges).await
    }

    async fn xorb_length(&self, hash: &merklehash::MerkleHash) -> Result<u32> {
        self.client.xorb_length(hash).await
    }

    async fn xorb_exists(&self, hash: &merklehash::MerkleHash) -> Result<bool> {
        self.client.xorb_exists(hash).await
    }

    async fn xorb_footer(&self, hash: &merklehash::MerkleHash) -> Result<cas_object::CasObject> {
        self.client.xorb_footer(hash).await
    }

    async fn get_file_size(&self, hash: &merklehash::MerkleHash) -> Result<u64> {
        self.client.get_file_size(hash).await
    }

    async fn get_file_data(
        &self,
        hash: &merklehash::MerkleHash,
        byte_range: Option<cas_types::FileRange>,
    ) -> Result<bytes::Bytes> {
        self.client.get_file_data(hash, byte_range).await
    }

    async fn get_xorb_raw_bytes(
        &self,
        hash: &merklehash::MerkleHash,
        byte_range: Option<cas_types::FileRange>,
    ) -> Result<bytes::Bytes> {
        self.client.get_xorb_raw_bytes(hash, byte_range).await
    }

    async fn xorb_raw_length(&self, hash: &merklehash::MerkleHash) -> Result<u64> {
        self.client.xorb_raw_length(hash).await
    }

    async fn fetch_term_data(
        &self,
        hash: merklehash::MerkleHash,
        fetch_term: cas_types::CASReconstructionFetchInfo,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.client.fetch_term_data(hash, fetch_term).await
    }
}

impl Drop for LocalTestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use cas_types::FileRange;

    use super::super::super::ClientTestingUtils;
    use super::*;
    use crate::Client;

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

        // Full file reconstruction - compare remote and local
        let remote_recon = server
            .remote_client()
            .get_reconstruction(&file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        let local_recon = server
            .client()
            .get_reconstruction(&file.file_hash, None)
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
            .get_reconstruction(&file.file_hash, Some(range))
            .await
            .unwrap();
        assert!(range_recon.is_some());

        // Upload multi-xorb file
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let multi_file = server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap();
        let multi_recon = server
            .remote_client()
            .get_reconstruction(&multi_file.file_hash, None)
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
        let fake_hash =
            merklehash::MerkleHash::from_hex("d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da")
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
            .get_reconstruction(&file1.file_hash, None)
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
            .get_reconstruction(&multi_file.file_hash, None)
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
            .get_reconstruction(&multi_file.file_hash, Some(range))
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
            .get_reconstruction(&file.file_hash, None)
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
            .get_reconstruction(&file.file_hash, None)
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
        assert_eq!(raw_xorb.cas_info.chunks.len(), 3);
        for (i, chunk_hash) in file.terms[0].chunk_hashes.iter().enumerate() {
            assert_eq!(*chunk_hash, raw_xorb.cas_info.chunks[i].chunk_hash);
        }
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
}
