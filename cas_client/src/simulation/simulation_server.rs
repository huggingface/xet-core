//! Local Test Server for Simulation
//!
//! This module provides `LocalTestServer` and `LocalTestServerBuilder` for creating
//! test servers that wrap `LocalServer` and provide easy access to both
//! `RemoteSimulationClient` (for HTTP interactions) and `DirectAccessClient` (for direct state access).

use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::oneshot;

use crate::RemoteClient;
use crate::error::Result;
use crate::interface::Client;
use crate::simulation::local_server::{LocalServer, ServerDelayProfile};
#[cfg(unix)]
use crate::simulation::socket_proxy::UnixSocketProxy;
use crate::simulation::{DirectAccessClient, LocalClient, MemoryClient, RemoteSimulationClient};

/// Builder for creating a `LocalTestServer` with various configuration options.
///
/// # Example
///
/// ```ignore
/// // In-memory storage with ephemeral socket
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_socket()
///     .start()
///     .await;
///
/// // Ephemeral disk storage (temporary directory, auto-cleaned)
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_disk()
///     .start()
///     .await;
///
/// // Disk-backed storage with specific socket path
/// let server = LocalTestServerBuilder::new()
///     .with_disk_location("/tmp/test_cas")
///     .with_socket("/tmp/test.sock")
///     .start()
///     .await;
///
/// // Ephemeral disk with ephemeral socket
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_disk()
///     .with_ephemeral_socket()
///     .start()
///     .await;
///
/// // With server delay profile for simulation
/// use cas_client::simulation::local_server::ServerDelayProfile;
/// let delay_profile = ServerDelayProfile {
///     random_delay_ms: Some((10, 100)),
///     connection_threshold: Some(5),
///     min_congestion_penalty_ms: Some(50),
///     max_congestion_penalty_ms: Some(200),
///     congestion_error_rate: Some(0.1),
/// };
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_disk()
///     .with_server_delay_profile(delay_profile)
///     .start()
///     .await;
///
/// // Use existing client
/// let client = MemoryClient::new();
/// let server = LocalTestServerBuilder::new()
///     .with_client(client)
///     .with_ephemeral_socket()
///     .start()
///     .await;
/// ```
pub struct LocalTestServerBuilder {
    in_memory: bool,
    disk_location: Option<PathBuf>,
    socket_path: Option<PathBuf>,
    ephemeral_socket: bool,
    ephemeral_disk: bool,
    client: Option<Arc<dyn DirectAccessClient>>,
    server_delay_profile: Option<ServerDelayProfile>,
}

#[allow(dead_code)]
impl LocalTestServerBuilder {
    /// Creates a new builder with default settings (in-memory storage, no socket).
    pub fn new() -> Self {
        Self {
            in_memory: true,
            disk_location: None,
            socket_path: None,
            ephemeral_socket: false,
            ephemeral_disk: false,
            client: None,
            server_delay_profile: None,
        }
    }

    /// Configures the server to use a specific Unix socket path.
    ///
    /// This creates a proxy that forwards connections from the socket to the TCP server.
    /// Mutually exclusive with `with_ephemeral_socket`.
    #[cfg(unix)]
    pub fn with_socket(mut self, path: PathBuf) -> Self {
        self.socket_path = Some(path);
        self.ephemeral_socket = false;
        self
    }

    /// Configures the server to use disk-backed storage at the specified location.
    ///
    /// If not called, the server defaults to in-memory storage.
    /// Mutually exclusive with `with_ephemeral_disk`.
    pub fn with_disk_location(mut self, path: impl Into<PathBuf>) -> Self {
        self.disk_location = Some(path.into());
        self.in_memory = false;
        self.ephemeral_disk = false;
        self
    }

    /// Configures the server to use disk-backed storage in a temporary directory.
    ///
    /// The directory will be automatically cleaned up when the server is dropped.
    /// Mutually exclusive with `with_disk_location`.
    pub fn with_ephemeral_disk(mut self) -> Self {
        self.ephemeral_disk = true;
        self.in_memory = false;
        self.disk_location = None;
        self
    }

    /// Configures the server to create an ephemeral Unix socket in a temporary directory.
    ///
    /// The socket will be automatically cleaned up when the server is dropped.
    /// Mutually exclusive with `with_socket`.
    #[cfg(unix)]
    pub fn with_ephemeral_socket(mut self) -> Self {
        self.ephemeral_socket = true;
        self.socket_path = None;
        self
    }

    /// Configures the server to use an existing `DirectAccessClient`.
    ///
    /// Useful when you need to pre-populate the client with data before starting the server.
    /// This overrides any disk location or in-memory settings.
    pub fn with_client(mut self, client: Arc<dyn DirectAccessClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Configures the server's delay profile for simulation.
    ///
    /// This sets the delay and congestion simulation parameters that will be applied
    /// to all requests handled by the server. The profile is applied after the server starts.
    pub fn with_server_delay_profile(mut self, profile: ServerDelayProfile) -> Self {
        self.server_delay_profile = Some(profile);
        self
    }

    /// Builds and starts the test server.
    pub async fn start(self) -> LocalTestServer {
        #[cfg(unix)]
        let (socket_path, ephemeral_tempdir) = if self.ephemeral_socket {
            let tempdir = TempDir::new().expect("Failed to create temporary directory for ephemeral socket");
            let socket_path = tempdir.path().join("socket.sock");
            (Some(socket_path), Some(tempdir))
        } else {
            (self.socket_path, None)
        };

        #[cfg(not(unix))]
        let socket_path = if self.ephemeral_socket { None } else { self.socket_path };

        let client: Arc<dyn DirectAccessClient> = if let Some(client) = self.client {
            client
        } else if self.in_memory {
            MemoryClient::new()
        } else if self.ephemeral_disk {
            LocalClient::temporary()
                .await
                .expect("Failed to create LocalClient with temporary directory")
        } else {
            let disk_path = self.disk_location.unwrap_or_else(|| {
                panic!("with_disk_location must be called when in_memory is false and no client is provided")
            });
            LocalClient::new(&disk_path).await.expect("Failed to create LocalClient")
        };

        let port = LocalTestServer::find_available_port();
        let host = "127.0.0.1".to_string();
        let tcp_endpoint = format!("http://{}:{}", host, port);

        let server = LocalServer::from_client(client.clone(), host, port);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = server.run_until_stopped(shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        #[cfg(unix)]
        let actual_endpoint = tcp_endpoint.clone();

        #[cfg(unix)]
        let (remote_client, socket_proxy) = {
            if let Some(socket_path) = socket_path {
                // Extract host:port from http://host:port
                let tcp_addr = actual_endpoint.strip_prefix("http://").unwrap_or(&actual_endpoint).to_string();

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
        };

        #[cfg(not(unix))]
        let remote_client = RemoteClient::new(&tcp_endpoint, &None, "test-session", false, "test-agent");

        let remote_simulation_client = Arc::new(RemoteSimulationClient::new(remote_client));

        #[cfg(unix)]
        let server = LocalTestServer {
            http_endpoint: tcp_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_simulation_client,
            client,
            socket_proxy,
            _ephemeral_socket_tempdir: ephemeral_tempdir,
        };

        #[cfg(not(unix))]
        let server = LocalTestServer {
            http_endpoint: tcp_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_simulation_client,
            client,
        };

        // Apply delay profile if configured
        if let Some(profile) = self.server_delay_profile {
            server
                .remote_simulation_client()
                .simulation_set_delay_profile(profile)
                .await
                .expect("Failed to set server delay profile");
        }

        server
    }
}

impl Default for LocalTestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A test server that wraps `LocalServer` and provides easy access to both
/// `RemoteSimulationClient` (for HTTP interactions) and `DirectAccessClient` (for direct state access).
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
/// // Using the builder (recommended)
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_socket()
///     .start()
///     .await;
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
    http_endpoint: String,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
    remote_simulation_client: Arc<RemoteSimulationClient>,
    client: Arc<dyn DirectAccessClient>,

    #[cfg(unix)]
    socket_proxy: Option<UnixSocketProxy>,
    #[cfg(unix)]
    _ephemeral_socket_tempdir: Option<TempDir>,
}

impl LocalTestServer {
    /// Returns the HTTP endpoint URL (e.g., "http://127.0.0.1:12345").
    pub fn http_endpoint(&self) -> &str {
        &self.http_endpoint
    }

    #[cfg(unix)]
    pub fn socket_endpoint(&self) -> Option<PathBuf> {
        self.socket_proxy.as_ref().map(|proxy| proxy.socket_path().clone())
    }

    /// Returns the client configured to connect to this test server as a `Client` trait object.
    pub fn remote_client(&self) -> Arc<dyn Client> {
        self.remote_simulation_client.clone() as Arc<dyn Client>
    }

    /// Returns the `RemoteSimulationClient` configured to connect to this test server.
    pub fn remote_simulation_client(&self) -> &Arc<RemoteSimulationClient> {
        &self.remote_simulation_client
    }

    /// Returns the underlying `DirectAccessClient` for direct state access.
    pub fn client(&self) -> &Arc<dyn DirectAccessClient> {
        &self.client
    }

    /// Sets the server's delay profile for simulation.
    ///
    /// This sets the delay and congestion simulation parameters that will be applied
    /// to all requests handled by the server.
    ///
    /// # Arguments
    /// * `profile` - The delay profile to apply
    ///
    /// # Errors
    /// Returns an error if the profile cannot be applied.
    pub async fn set_server_delay_profile(&self, profile: ServerDelayProfile) -> Result<()> {
        self.remote_simulation_client().simulation_set_delay_profile(profile).await
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
        self.remote_simulation_client.get_file_reconstruction_info(file_hash).await
    }

    async fn get_reconstruction(
        &self,
        file_id: &merklehash::MerkleHash,
        bytes_range: Option<cas_types::FileRange>,
    ) -> Result<Option<cas_types::QueryReconstructionResponse>> {
        self.remote_simulation_client.get_reconstruction(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(
        &self,
        file_ids: &[merklehash::MerkleHash],
    ) -> Result<cas_types::BatchQueryReconstructionResponse> {
        self.remote_simulation_client.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.remote_simulation_client.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn crate::interface::URLProvider>,
        download_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.remote_simulation_client
            .get_file_term_data(url_info, download_permit)
            .await
    }

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &merklehash::MerkleHash,
    ) -> Result<Option<bytes::Bytes>> {
        self.remote_simulation_client
            .query_for_global_dedup_shard(prefix, chunk_hash)
            .await
    }

    async fn acquire_upload_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.remote_simulation_client.acquire_upload_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: bytes::Bytes,
        upload_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.remote_simulation_client.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: cas_object::SerializedCasObject,
        upload_tracker: Option<std::sync::Arc<progress_tracking::upload_tracking::CompletionTracker>>,
        upload_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.remote_simulation_client
            .upload_xorb(prefix, serialized_cas_object, upload_tracker, upload_permit)
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

    async fn apply_api_delay(&self) {
        self.client.apply_api_delay().await;
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

    use super::super::ClientTestingUtils;
    use super::*;

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
        let invalid_term_url = format!("{}/v1/fetch_term?term=aW52YWxpZF9wYXRo", server.http_endpoint());
        let response = http_client.get(&invalid_term_url).send().await.unwrap();
        assert!(response.status().is_client_error() || response.status().is_server_error());

        // Malformed base64 in fetch_term
        let malformed_url = format!("{}/v1/fetch_term?term=not-valid-base64!!!", server.http_endpoint());
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

    /// Tests the /simulation/set_config endpoint for setting random_delay.
    async fn check_simulation_set_config(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        // Test setting random_delay with valid value
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay&value=(10ms,100ms)", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Test case insensitivity
        let response = http_client
            .post(format!("{}/simulation/set_config?config=RANDOM_DELAY&value=(5ms,50ms)", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Test unknown config
        let response = http_client
            .post(format!("{}/simulation/set_config?config=unknown_config&value=test", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Test invalid random_delay value
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay&value=invalid", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Test missing config parameter
        let response = http_client
            .post(format!("{}/simulation/set_config?value=test", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Test missing value parameter
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Reset delay to zero for subsequent tests
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay&value=(0ms,0ms)", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    /// Tests the /simulation/dummy_upload endpoint.
    async fn check_simulation_dummy_upload(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        // Test basic upload (no delay configured)
        let test_data = vec![0u8; 1024];
        let start = std::time::Instant::now();
        let response = http_client
            .post(format!("{}/simulation/dummy_upload", server.http_endpoint()))
            .body(test_data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let no_delay_duration = start.elapsed();

        // Configure a delay
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay&value=(50ms,50ms)", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Test upload with delay
        let start = std::time::Instant::now();
        let response = http_client
            .post(format!("{}/simulation/dummy_upload", server.http_endpoint()))
            .body(test_data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let with_delay_duration = start.elapsed();

        // The delayed upload should take noticeably longer
        assert!(
            with_delay_duration >= Duration::from_millis(40),
            "Expected delay of ~50ms, but got {:?}",
            with_delay_duration
        );
        assert!(
            with_delay_duration > no_delay_duration,
            "Upload with delay ({:?}) should be slower than without ({:?})",
            with_delay_duration,
            no_delay_duration
        );

        // Test empty upload
        let response = http_client
            .post(format!("{}/simulation/dummy_upload", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Reset delay
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay&value=(0ms,0ms)", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
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
        check_simulation_set_config(server).await;
        check_simulation_dummy_upload(server).await;
    }

    /// Main test that runs all server checks with both in-memory and disk-backed storage.
    #[tokio::test]
    async fn test_local_server() {
        // Test with in-memory storage
        {
            tracing::info!("Testing with in-memory storage");
            let server = LocalTestServerBuilder::new().start().await;
            assert!(server.client().list_xorbs().await.unwrap().is_empty());
            run_all_server_checks(&server).await;
        }

        // Test with disk-backed storage
        {
            tracing::info!("Testing with disk-backed storage");
            let server = LocalTestServerBuilder::new().with_ephemeral_disk().start().await;
            assert!(server.client().list_xorbs().await.unwrap().is_empty());
            run_all_server_checks(&server).await;
        }
    }
}
