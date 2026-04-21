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
use http::header::{self, HeaderMap, HeaderValue};
#[cfg(unix)]
use tempfile::TempDir;
use tokio::sync::oneshot;
use xet_runtime::core::XetContext;

use super::super::RemoteClient;
use super::super::interface::Client;
use super::super::progress_tracked_streams::ProgressCallback;
use super::local_server::{LocalServer, ServerLatencyProfile};
use super::network_simulation::{NetworkProfile, NetworkSimulationProxy};
#[cfg(unix)]
use super::socket_proxy::UnixSocketProxy;
use super::{DeletionControlableClient, DirectAccessClient, LocalClient, MemoryClient, RemoteSimulationClient};
use crate::error::Result;

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
/// // With server latency profile for simulation
/// use cas_client::simulation::local_server::ServerLatencyProfile;
/// let latency_profile = ServerLatencyProfile::realistic();
/// let server = LocalTestServerBuilder::new()
///     .with_ephemeral_disk()
///     .with_server_latency_profile(latency_profile)
///     .start()
///     .await;
///
/// // Use existing client
/// let client = MemoryClient::new(ctx);
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
    server_latency_profile: Option<ServerLatencyProfile>,
    network_profile: Option<NetworkProfile>,
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
            server_latency_profile: None,
            network_profile: None,
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

    /// Configures the server's latency profile for simulation.
    ///
    /// This sets the latency simulation parameters (delays, congestion) that will be applied
    /// to all requests handled by the server. The profile is applied after the server starts.
    pub fn with_server_latency_profile(mut self, profile: ServerLatencyProfile) -> Self {
        self.server_latency_profile = Some(profile);
        self
    }

    /// Configures a dynamic network profile provider. When set, a bandwidth-limit proxy is started
    /// in front of the server and the client endpoint routes through it. Capacity stats are tracked for utilization.
    pub fn with_network_profile(mut self, provider: NetworkProfile) -> Self {
        self.network_profile = Some(provider);
        self
    }

    /// Builds and starts the test server.
    pub async fn start(self) -> LocalTestServer {
        let runtime = XetContext::default().expect("XetContext::new");
        #[cfg(unix)]
        let (socket_path, ephemeral_tempdir) = if self.ephemeral_socket {
            let tempdir = TempDir::new().expect("Failed to create temporary directory for ephemeral socket");
            let socket_path = tempdir.path().join("socket.sock");
            (Some(socket_path), Some(tempdir))
        } else {
            (self.socket_path, None)
        };

        #[cfg(not(unix))]
        let _socket_path = if self.ephemeral_socket { None } else { self.socket_path };

        // Build client + optional deletion_client. LocalClient and MemoryClient both support
        // DirectAccessClient + DeletionControlableClient; pre-supplied clients only support DirectAccessClient.
        let (client, deletion_client): (Arc<dyn DirectAccessClient>, Option<Arc<dyn DeletionControlableClient>>) =
            if let Some(client) = self.client {
                (client, None)
            } else if self.in_memory {
                let mc = MemoryClient::new(runtime.clone());
                let dc: Arc<dyn DeletionControlableClient> = mc.clone();
                (mc, Some(dc))
            } else if self.ephemeral_disk {
                let lc = LocalClient::temporary(runtime.clone())
                    .await
                    .expect("Failed to create LocalClient with temporary directory");
                let dc: Arc<dyn DeletionControlableClient> = lc.clone();
                (lc, Some(dc))
            } else {
                let disk_path = self.disk_location.unwrap_or_else(|| {
                    panic!("with_disk_location must be called when in_memory is false and no client is provided")
                });
                let lc = LocalClient::new(runtime.clone(), &disk_path)
                    .await
                    .expect("Failed to create LocalClient");
                let dc: Arc<dyn DeletionControlableClient> = lc.clone();
                (lc, Some(dc))
            };

        let port = LocalTestServer::find_available_port();
        let host = "127.0.0.1".to_string();
        let tcp_endpoint = format!("http://{}:{}", host, port);
        let server_host_port = format!("{}:{}", host, port);

        let dynamic_provider = self.network_profile.clone();
        let (proxy_guard, client_endpoint): (Option<Arc<NetworkSimulationProxy>>, String) =
            if let Some(provider) = dynamic_provider {
                let (proxy, listen_addr) = NetworkSimulationProxy::new(server_host_port.clone(), provider)
                    .await
                    .expect("Failed to create network simulation proxy");
                Arc::clone(&proxy).run_refill_loop();
                tokio::spawn({
                    let proxy = Arc::clone(&proxy);
                    async move {
                        if let Err(e) = proxy.run_accept_loop().await {
                            tracing::error!(error = %e, "bandwidth proxy accept loop failed");
                        }
                    }
                });
                (Some(proxy), format!("http://{}", listen_addr))
            } else {
                (None, tcp_endpoint.clone())
            };

        let server = LocalServer::from_client(client.clone(), deletion_client.clone(), host, port);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = server.run_until_stopped(shutdown_rx).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_static("test-agent"));
        let custom_headers = Some(Arc::new(headers));

        #[cfg(unix)]
        let (remote_client, socket_proxy) = {
            if let Some(socket_path) = socket_path {
                let tcp_addr = client_endpoint.strip_prefix("http://").unwrap_or(&client_endpoint).to_string();

                let proxy = UnixSocketProxy::new(socket_path.clone(), tcp_addr)
                    .await
                    .expect("Failed to create Unix socket proxy");

                tokio::time::sleep(Duration::from_millis(500)).await;

                let socket_path_str = socket_path.to_string_lossy().to_string();
                let client = RemoteClient::new_with_socket(
                    runtime.clone(),
                    &client_endpoint,
                    &None,
                    "test-session",
                    false,
                    Some(&socket_path_str),
                    custom_headers.clone(),
                );

                (client, Some(proxy))
            } else {
                let client = RemoteClient::new(
                    runtime.clone(),
                    &client_endpoint,
                    &None,
                    "test-session",
                    false,
                    custom_headers.clone(),
                );
                (client, None)
            }
        };

        #[cfg(not(unix))]
        let remote_client =
            RemoteClient::new(runtime.clone(), &client_endpoint, &None, "test-session", false, custom_headers.clone());

        let remote_simulation_client = Arc::new(RemoteSimulationClient::new(remote_client));

        #[cfg(unix)]
        let server = LocalTestServer {
            http_endpoint: client_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_simulation_client,
            client,
            deletion_client,
            socket_proxy,
            _ephemeral_socket_tempdir: ephemeral_tempdir,
            network_simulation_proxy: proxy_guard.clone(),
        };

        #[cfg(not(unix))]
        let server = LocalTestServer {
            http_endpoint: client_endpoint,
            server_shutdown_tx: Some(shutdown_tx),
            remote_simulation_client,
            client,
            deletion_client,
            network_simulation_proxy: proxy_guard,
        };

        if let Some(profile) = self.server_latency_profile {
            server
                .remote_simulation_client()
                .simulation_set_latency_profile(profile)
                .await
                .expect("Failed to set server latency profile");
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
    deletion_client: Option<Arc<dyn DeletionControlableClient>>,
    network_simulation_proxy: Option<Arc<NetworkSimulationProxy>>,

    #[cfg(unix)]
    socket_proxy: Option<UnixSocketProxy>,
    #[cfg(unix)]
    _ephemeral_socket_tempdir: Option<TempDir>,
}

impl LocalTestServer {
    /// Returns the HTTP endpoint URL. When a network simulation proxy is present, returns the proxy endpoint so traffic
    /// routes through it.
    pub fn http_endpoint(&self) -> &str {
        &self.http_endpoint
    }

    /// Returns total upload bytes possible since proxy start (0 if no proxy).
    pub fn total_upload_bytes_possible(&self) -> u64 {
        self.network_simulation_proxy
            .as_ref()
            .map(|p| p.total_upload_bytes_possible())
            .unwrap_or(0)
    }

    /// Returns total download bytes possible since proxy start (0 if no proxy).
    pub fn total_download_bytes_possible(&self) -> u64 {
        self.network_simulation_proxy
            .as_ref()
            .map(|p| p.total_download_bytes_possible())
            .unwrap_or(0)
    }

    /// Current bandwidth from the proxy's network profile (bytes/sec), or `None` if no proxy or unlimited.
    pub async fn current_bandwidth(&self) -> Option<u64> {
        match &self.network_simulation_proxy {
            Some(proxy) => proxy.current_bandwidth().await,
            None => None,
        }
    }

    /// Current base latency from the proxy's network profile (ms), or 0.0 if no proxy.
    pub async fn current_latency_ms(&self) -> f64 {
        match &self.network_simulation_proxy {
            Some(proxy) => proxy.current_latency_ms().await,
            None => 0.0,
        }
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

    /// Sets the server's load profile for simulation.
    ///
    /// This sets the load simulation parameters (delays, congestion) that will be applied
    /// to all requests handled by the server.
    ///
    /// # Arguments
    /// * `profile` - The latency profile to apply
    ///
    /// # Errors
    /// Returns an error if the profile cannot be applied.
    pub async fn set_server_latency_profile(&self, profile: ServerLatencyProfile) -> Result<()> {
        self.remote_simulation_client().simulation_set_latency_profile(profile).await
    }

    /// Verifies referential integrity by calling through to the underlying `DeletionControlableClient`.
    pub async fn verify_integrity(&self) -> Result<()> {
        match &self.deletion_client {
            Some(dc) => dc.verify_integrity().await,
            None => Ok(()),
        }
    }

    /// Verifies all on-disk data is reachable by calling through to the underlying `DeletionControlableClient`.
    pub async fn verify_all_reachable(&self) -> Result<()> {
        match &self.deletion_client {
            Some(dc) => dc.verify_all_reachable().await,
            None => Ok(()),
        }
    }

    fn find_available_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }
}

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
        self.remote_simulation_client.get_file_reconstruction_info(file_hash).await
    }

    async fn get_reconstruction(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponseV2>> {
        self.remote_simulation_client.get_reconstruction(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(
        &self,
        file_ids: &[xet_core_structures::merklehash::MerkleHash],
    ) -> Result<crate::cas_types::BatchQueryReconstructionResponse> {
        self.remote_simulation_client.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<super::super::adaptive_concurrency::ConnectionPermit> {
        self.remote_simulation_client.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn super::super::interface::URLProvider>,
        download_permit: super::super::adaptive_concurrency::ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(bytes::Bytes, Vec<u32>)> {
        self.remote_simulation_client
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<Option<bytes::Bytes>> {
        self.remote_simulation_client
            .query_for_global_dedup_shard(prefix, chunk_hash)
            .await
    }

    async fn acquire_upload_permit(&self) -> Result<super::super::adaptive_concurrency::ConnectionPermit> {
        self.remote_simulation_client.acquire_upload_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: bytes::Bytes,
        upload_permit: super::super::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.remote_simulation_client.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: xet_core_structures::xorb_object::SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: super::super::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.remote_simulation_client
            .upload_xorb(prefix, serialized_xorb_object, progress_callback, upload_permit)
            .await
    }
}

#[async_trait]
impl DirectAccessClient for LocalTestServer {
    fn set_fetch_term_url_expiration(&self, expiration: std::time::Duration) {
        self.client.set_fetch_term_url_expiration(expiration);
    }

    fn set_global_dedup_shard_expiration(&self, expiration: Option<std::time::Duration>) {
        self.client.set_global_dedup_shard_expiration(expiration);
    }

    fn set_api_delay_range(&self, delay_range: Option<std::ops::Range<std::time::Duration>>) {
        self.client.set_api_delay_range(delay_range);
    }

    fn set_max_ranges_per_fetch(&self, max_ranges: usize) {
        self.client.set_max_ranges_per_fetch(max_ranges);
    }

    fn disable_v2_reconstruction(&self, status_code: u16) {
        self.client.disable_v2_reconstruction(status_code);
    }

    async fn get_reconstruction_v1(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponse>> {
        self.client.get_reconstruction_v1(file_id, bytes_range).await
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponseV2>> {
        self.client.get_reconstruction_v2(file_id, bytes_range).await
    }

    async fn apply_api_delay(&self) {
        self.client.apply_api_delay().await;
    }

    async fn list_xorbs(&self) -> Result<Vec<xet_core_structures::merklehash::MerkleHash>> {
        self.client.list_xorbs().await
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

impl Drop for LocalTestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ClientTestingUtils;
    use super::*;
    use crate::cas_types::FileRange;

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
        for multi_range_fetches in remote_recon.xorbs.values() {
            for mrf in multi_range_fetches {
                assert!(mrf.url.starts_with("http://"));
                assert!(mrf.url.contains("/fetch_term?term="));
                let response = http_client.get(&mrf.url).send().await.unwrap();
                assert!(response.status().is_success());
                assert!(!response.bytes().await.unwrap().is_empty());
            }
        }

        // Verify V2 fetch URLs return consistent data across multiple requests.
        let first_mrf = &remote_recon.xorbs.values().next().unwrap()[0];
        let data_1 = http_client.get(&first_mrf.url).send().await.unwrap().bytes().await.unwrap();
        let data_2 = http_client.get(&first_mrf.url).send().await.unwrap().bytes().await.unwrap();
        assert_eq!(data_1, data_2);
        assert!(!data_1.is_empty());
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
        for (hash, multi_range_fetches) in &recon1.xorbs {
            for mrf in multi_range_fetches {
                assert!(
                    mrf.url.starts_with("http://") || mrf.url.starts_with("https://"),
                    "URL for hash {} should be HTTP, got: {}",
                    hash,
                    mrf.url
                );
                assert!(mrf.url.contains("/fetch_term?term="));
                assert!(!mrf.url.contains("\":"));
            }
        }

        // Verify multi-XORB file has HTTP URLs for all XORBs
        let multi_recon = server
            .remote_client()
            .get_reconstruction(&multi_file.file_hash, None)
            .await
            .unwrap()
            .unwrap();
        assert!(multi_recon.xorbs.len() >= 2);
        for multi_range_fetches in multi_recon.xorbs.values() {
            for mrf in multi_range_fetches {
                assert!(mrf.url.starts_with("http://"));
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
        for multi_range_fetches in range_recon.xorbs.values() {
            for mrf in multi_range_fetches {
                assert!(mrf.url.starts_with("http://"));
                assert!(mrf.url.contains("/fetch_term?term="));
            }
        }

        // Verify all term URLs are fetchable
        for term in &recon1.terms {
            let multi_range_fetches = recon1.xorbs.get(&term.hash).unwrap();
            for mrf in multi_range_fetches {
                let response = http_client.get(&mrf.url).send().await.unwrap();
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

            // Verify xorbs has entry for each term
            let multi_range_fetches = recon.xorbs.get(&recon_term.hash).unwrap();
            assert!(!multi_range_fetches.is_empty());
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

        // Verify chunk hashes match the RawXorbData xorb_info (keyed by xorb hash)
        let xorb_hash = file.terms[0].xorb_hash;
        let raw_xorb = file.xorbs.get(&xorb_hash).unwrap();
        assert_eq!(raw_xorb.xorb_info.chunks.len(), 3);
        for (i, chunk_hash) in file.terms[0].chunk_hashes.iter().enumerate() {
            assert_eq!(*chunk_hash, raw_xorb.xorb_info.chunks[i].chunk_hash);
        }
    }

    async fn post_set_config(server: &LocalTestServer, config: &str, value: &str) -> reqwest::StatusCode {
        let http_client = reqwest::Client::new();
        let url = format!(
            "{}/simulation/set_config?config={}&value={}",
            server.http_endpoint(),
            urlencoding::encode(config),
            urlencoding::encode(value),
        );
        http_client.post(&url).send().await.unwrap().status()
    }

    /// Tests the /simulation/set_config endpoint for all supported configs.
    async fn check_simulation_set_config(server: &LocalTestServer) {
        let http_client = reqwest::Client::new();

        // --- random_delay ---
        assert_eq!(post_set_config(server, "random_delay", "(10ms,100ms)").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "RANDOM_DELAY", "(5ms,50ms)").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "random_delay", "invalid").await, reqwest::StatusCode::BAD_REQUEST);

        // Unknown config
        assert_eq!(post_set_config(server, "unknown_config", "test").await, reqwest::StatusCode::BAD_REQUEST);

        // Missing config parameter
        let response = http_client
            .post(format!("{}/simulation/set_config?value=test", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Missing value parameter
        let response = http_client
            .post(format!("{}/simulation/set_config?config=random_delay", server.http_endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        // Reset delay
        assert_eq!(post_set_config(server, "random_delay", "(0ms,0ms)").await, reqwest::StatusCode::OK);

        // --- global_dedup_shard_expiration ---
        assert_eq!(post_set_config(server, "global_dedup_shard_expiration", "300").await, reqwest::StatusCode::OK);
        // Verify effect: upload a shard, query dedup, check expiry is set and file data stripped
        {
            use std::io::Cursor;

            use xet_core_structures::metadata_shard::MDBShardInfo;
            use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;

            let file = server.client().upload_random_file(&[(1, (0, 5))], CHUNK_SIZE).await.unwrap();
            let first_chunk = file.terms[0].chunk_hashes[0];
            let shard_bytes = server
                .client()
                .query_for_global_dedup_shard("default", &first_chunk)
                .await
                .unwrap()
                .unwrap();

            let minimal_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&shard_bytes), true, true).unwrap();
            assert_eq!(minimal_shard.num_files(), 0);
            assert_ne!(minimal_shard.num_xorb(), 0);

            let shard_info = MDBShardInfo::load_from_reader(&mut Cursor::new(&shard_bytes)).unwrap();
            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            assert_ne!(shard_info.metadata.shard_key_expiry, 0);
            assert!(shard_info.metadata.shard_key_expiry >= now_epoch + 295);
            assert!(shard_info.metadata.shard_key_expiry <= now_epoch + 305);
        }
        // Disable it again
        assert_eq!(post_set_config(server, "global_dedup_shard_expiration", "0").await, reqwest::StatusCode::OK);
        assert_eq!(
            post_set_config(server, "global_dedup_shard_expiration", "not_a_number").await,
            reqwest::StatusCode::BAD_REQUEST
        );

        // --- max_ranges_per_fetch ---
        assert_eq!(post_set_config(server, "max_ranges_per_fetch", "10").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "max_ranges_per_fetch", "abc").await, reqwest::StatusCode::BAD_REQUEST);
        // Reset
        assert_eq!(
            post_set_config(server, "max_ranges_per_fetch", &usize::MAX.to_string()).await,
            reqwest::StatusCode::OK
        );

        // --- disable_v2_reconstruction ---
        assert_eq!(post_set_config(server, "disable_v2_reconstruction", "503").await, reqwest::StatusCode::OK);
        // Verify effect: V2 reconstruction via HTTP should return 503
        {
            let file = server.client().upload_random_file(&[(1, (0, 3))], CHUNK_SIZE).await.unwrap();
            let v2_url = format!("{}/v2/reconstructions/{:?}", server.http_endpoint(), file.file_hash);
            let resp = http_client.get(&v2_url).send().await.unwrap();
            assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        }
        // Re-enable
        assert_eq!(post_set_config(server, "disable_v2_reconstruction", "0").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "disable_v2_reconstruction", "xyz").await, reqwest::StatusCode::BAD_REQUEST);

        // --- api_delay ---
        assert_eq!(post_set_config(server, "api_delay", "(50ms, 50ms)").await, reqwest::StatusCode::OK);
        // Verify effect: a Client-trait API call should take at least 40ms
        {
            let file = server.client().upload_random_file(&[(1, (0, 3))], CHUNK_SIZE).await.unwrap();
            let first_chunk = file.terms[0].chunk_hashes[0];
            let start = std::time::Instant::now();
            let _ = server.client().query_for_global_dedup_shard("default", &first_chunk).await;
            assert!(
                start.elapsed() >= std::time::Duration::from_millis(40),
                "Expected delay of ~50ms, but got {:?}",
                start.elapsed()
            );
        }
        // Disable
        assert_eq!(post_set_config(server, "api_delay", "(0ms, 0ms)").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "api_delay", "invalid").await, reqwest::StatusCode::BAD_REQUEST);

        // --- url_expiration ---
        assert_eq!(post_set_config(server, "url_expiration", "5000").await, reqwest::StatusCode::OK);
        assert_eq!(post_set_config(server, "url_expiration", "not_a_number").await, reqwest::StatusCode::BAD_REQUEST);
        // Reset to large value
        assert_eq!(post_set_config(server, "url_expiration", &u64::MAX.to_string()).await, reqwest::StatusCode::OK);
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
