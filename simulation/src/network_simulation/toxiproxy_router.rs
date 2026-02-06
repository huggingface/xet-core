//! Routes RemoteClient traffic through a Toxiproxy proxy to a LocalTestServer.
//!
//! Configure bandwidth and latency per direction via [`NetworkProfile`]:
//! apply with [`apply_upload_profile()`](Self::apply_upload_profile) /
//! [`apply_download_profile()`](Self::apply_download_profile), clear with
//! [`reset_upload_profile()`](Self::reset_upload_profile) /
//! [`reset_download_profile()`](Self::reset_download_profile).

use std::net::TcpListener;
use std::sync::Arc;

use cas_client::{LocalTestServer, RemoteClient, RemoteSimulationClient};
use toxiproxy_rust::client::Client as ToxiproxyClient;
use toxiproxy_rust::proxy::{Proxy, ProxyPack};
use tracing::info;

use super::error::{Result, SimulationError};
use super::network_profile::NetworkProfile;

/// Applies latency and slicer (congestion) to one direction. Bandwidth is enforced by
/// the custom proxy (bandwidth_limit_router) between Toxiproxy and LocalTestServer.
fn apply_profile_to_stream(proxy: &Proxy, profile: &NetworkProfile, stream: &str) {
    let stream = stream.to_string();
    if let (Some(latency), Some(jitter)) = (profile.latency_ms, profile.jitter_ms) {
        proxy.with_latency(stream.clone(), latency, jitter, 1.0);
    }
    if let Some(ref slicer) = profile.slicer {
        proxy.with_slicer(stream, slicer.average_size, slicer.size_variation, slicer.delay_ms, 1.0);
    }
}

/// Default Toxiproxy server address (localhost, default port).
const DEFAULT_TOXIPROXY_ADDR: &str = "127.0.0.1:8474";

/// Environment variable to override the Toxiproxy server address.
pub const TOXIPROXY_ADDR_ENV: &str = "TOXIPROXY_ADDR";

/// Routes traffic for a RemoteClient through a LocalTestServer by way of a Toxiproxy proxy.
///
/// The proxy listens on a local port and forwards all traffic to the test server's endpoint.
/// Configure upload and download separately via [`NetworkProfile`] and
/// [`apply_upload_profile()`](Self::apply_upload_profile) / [`apply_download_profile()`](Self::apply_download_profile).
pub struct NetworkSimulationProxy {
    toxiproxy_client: ToxiproxyClient,
    proxy_name: String,
    proxy_endpoint: String,
    upload_profile: NetworkProfile,
    download_profile: NetworkProfile,
}

impl NetworkSimulationProxy {
    /// Creates a new router that proxies traffic to the given server endpoint.
    ///
    /// The Toxiproxy server address is read from the `TOXIPROXY_ADDR` environment variable,
    /// or defaults to `127.0.0.1:8474`.
    ///
    /// # Arguments
    /// * `server_endpoint` - The HTTP endpoint of the LocalTestServer (e.g. `http://127.0.0.1:12345`)
    pub fn new(server_endpoint: &str) -> Result<Self> {
        Self::new_impl(server_endpoint)
    }

    /// Creates a new router that proxies traffic to the given `LocalTestServer`.
    pub fn from_local_test_server(server: &LocalTestServer) -> Result<Self> {
        Self::new_impl(server.http_endpoint())
    }

    fn new_impl(server_endpoint: &str) -> Result<Self> {
        let upstream = endpoint_to_host_port(server_endpoint)?;
        let proxy_port = find_available_port().ok_or(SimulationError::NoAvailablePort)?;
        let listen = format!("127.0.0.1:{}", proxy_port);

        let toxiproxy_addr = std::env::var(TOXIPROXY_ADDR_ENV).unwrap_or_else(|_| DEFAULT_TOXIPROXY_ADDR.to_string());
        let toxiproxy_client = ToxiproxyClient::new(toxiproxy_addr.as_str());

        if !toxiproxy_client.is_running() {
            return Err(SimulationError::Toxiproxy(
                "Toxiproxy server is not running; start it or set TOXIPROXY_ADDR".to_string(),
            ));
        }

        let proxy_name = format!("cas_simulation_{}", uuid::Uuid::new_v4().simple());
        let proxy_pack = ProxyPack::new(proxy_name.clone(), listen.clone(), upstream.clone());
        toxiproxy_client
            .populate(vec![proxy_pack])
            .map_err(SimulationError::Toxiproxy)?;

        let proxy_endpoint = format!("http://{}", listen);
        info!(
            proxy_name = %proxy_name,
            proxy_endpoint = %proxy_endpoint,
            upstream = %upstream,
            "Toxiproxy proxy created"
        );

        Ok(Self {
            toxiproxy_client,
            proxy_name,
            proxy_endpoint,
            upload_profile: NetworkProfile::none(),
            download_profile: NetworkProfile::none(),
        })
    }

    /// Returns the proxy endpoint URL that clients should connect to.
    pub fn proxy_endpoint(&self) -> &str {
        &self.proxy_endpoint
    }

    /// Returns the current upload profile.
    pub fn upload_profile(&self) -> &NetworkProfile {
        &self.upload_profile
    }

    /// Returns the current download profile.
    pub fn download_profile(&self) -> &NetworkProfile {
        &self.download_profile
    }

    /// Sets the upload profile and applies both upload and download profiles to the proxy.
    pub fn apply_upload_profile(&mut self, profile: NetworkProfile) -> Result<()> {
        self.upload_profile = profile;
        self.apply_all_toxics()
    }

    /// Sets the download profile and applies both upload and download profiles to the proxy.
    pub fn apply_download_profile(&mut self, profile: NetworkProfile) -> Result<()> {
        self.download_profile = profile;
        self.apply_all_toxics()
    }

    /// Clears the upload profile and reapplies (download profile is unchanged).
    pub fn reset_upload_profile(&mut self) -> Result<()> {
        self.upload_profile.clear();
        self.apply_all_toxics()
    }

    /// Clears the download profile and reapplies (upload profile is unchanged).
    pub fn reset_download_profile(&mut self) -> Result<()> {
        self.download_profile.clear();
        self.apply_all_toxics()
    }

    fn apply_all_toxics(&self) -> Result<()> {
        let proxy = self
            .toxiproxy_client
            .find_and_reset_proxy(&self.proxy_name)
            .map_err(SimulationError::Toxiproxy)?;

        apply_profile_to_stream(&proxy, &self.upload_profile, "upstream");
        apply_profile_to_stream(&proxy, &self.download_profile, "downstream");

        Ok(())
    }

    /// Returns the names of toxics currently applied on the proxy (for verification).
    pub fn applied_toxic_names(&self) -> Result<Vec<String>> {
        let proxy = self
            .toxiproxy_client
            .find_proxy(&self.proxy_name)
            .map_err(SimulationError::Toxiproxy)?;
        let toxics = proxy.toxics().map_err(SimulationError::Toxiproxy)?;
        Ok(toxics.into_iter().map(|t| t.name).collect())
    }

    /// Creates a `RemoteSimulationClient` that sends all traffic through this proxy to the test server.
    pub fn remote_test_client(&self) -> Arc<RemoteSimulationClient> {
        let remote_client =
            RemoteClient::new(&self.proxy_endpoint, &None, "test-session", false, "simulation-test-agent");
        Arc::new(RemoteSimulationClient::new(remote_client))
    }
}

pub fn endpoint_to_host_port(endpoint: &str) -> Result<String> {
    let s = endpoint.trim_end_matches('/');
    let host_port = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .ok_or_else(|| SimulationError::InvalidEndpoint(endpoint.to_string()))?;
    if host_port.is_empty() {
        return Err(SimulationError::InvalidEndpoint(endpoint.to_string()));
    }
    Ok(host_port.to_string())
}

fn find_available_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|addr| addr.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toxiproxy_available() -> bool {
        let addr = std::env::var(TOXIPROXY_ADDR_ENV).unwrap_or_else(|_| DEFAULT_TOXIPROXY_ADDR.to_string());
        ToxiproxyClient::new(addr.as_str()).is_running()
    }

    #[test]
    fn test_network_profile_presets() {
        assert!(NetworkProfile::none().is_empty());
        assert!(!NetworkProfile::medium().is_empty());
        assert!(!NetworkProfile::heavy_congestion().is_empty());
        assert!(!NetworkProfile::bursty().is_empty());
        assert!(NetworkProfile::heavy_congestion().slicer.is_some());
    }

    #[test]
    fn test_network_profile_with_bandwidth_latency_strings() {
        let profile = NetworkProfile::heavy_congestion()
            .with_bandwidth("100kbps")
            .unwrap()
            .with_latency("100ms", "100ms")
            .unwrap();
        assert_eq!(profile.bandwidth_kbps, Some(100));
        assert_eq!(profile.latency_ms, Some(100));
        assert_eq!(profile.jitter_ms, Some(100));
    }

    #[test]
    fn test_network_profile_parse_bandwidth() {
        let p = NetworkProfile::none().with_bandwidth("1mbps").unwrap();
        assert_eq!(p.bandwidth_kbps, Some(1000));
        let p = NetworkProfile::none().with_bandwidth("500").unwrap();
        assert_eq!(p.bandwidth_kbps, Some(500));
    }

    #[test]
    fn test_network_profile_parse_duration() {
        let p = NetworkProfile::none().with_latency("1s", "50ms").unwrap();
        assert_eq!(p.latency_ms, Some(1000));
        assert_eq!(p.jitter_ms, Some(50));
    }

    #[test]
    fn test_network_profile_clear() {
        let mut profile = NetworkProfile::medium();
        assert!(!profile.is_empty());
        profile.clear();
        assert!(profile.is_empty());
        assert_eq!(profile.bandwidth_kbps, None);
        assert_eq!(profile.latency_ms, None);
    }

    #[test]
    fn test_endpoint_to_host_port() {
        assert_eq!(endpoint_to_host_port("http://127.0.0.1:12345").unwrap(), "127.0.0.1:12345");
        assert_eq!(endpoint_to_host_port("https://localhost:8080/").unwrap(), "localhost:8080");
        assert!(endpoint_to_host_port("ftp://x").is_err());
        assert!(endpoint_to_host_port("http://").is_err());
    }

    #[test]
    fn test_apply_profile_requires_toxiproxy() {
        if !toxiproxy_available() {
            eprintln!(
                "\n*** SKIPPED: Toxiproxy server is not running. \
                 Start Toxiproxy (e.g. on {}) to run proxy_router tests. ***",
                DEFAULT_TOXIPROXY_ADDR
            );
            return;
        }

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let test_server = cas_client::LocalTestServerBuilder::new().with_ephemeral_disk().start().await;
            let endpoint = test_server.http_endpoint().to_string();
            tokio::task::spawn_blocking(move || {
                let mut router = NetworkSimulationProxy::new(&endpoint).expect("router creation");
                router.reset_upload_profile().expect("reset_upload");
                router.reset_download_profile().expect("reset_download");

                let names_before = router.applied_toxic_names().expect("applied_toxic_names");
                assert!(names_before.is_empty(), "expected no toxics after reset, got {:?}", names_before);

                router
                    .apply_upload_profile(
                        NetworkProfile::medium()
                            .with_bandwidth("1000kbps")
                            .unwrap()
                            .with_latency("25ms", "5ms")
                            .unwrap(),
                    )
                    .expect("apply_upload_profile");
                router
                    .apply_download_profile(
                        NetworkProfile::heavy_congestion()
                            .with_bandwidth("2000kbps")
                            .unwrap()
                            .with_latency("50ms", "10ms")
                            .unwrap(),
                    )
                    .expect("apply_download_profile");

                let names = router.applied_toxic_names().expect("applied_toxic_names");
                assert!(names.len() >= 2, "expected at least 2 toxics (upload/download latency), got {}", names.len());
                assert!(names.iter().any(|n| n.contains("latency")), "expected latency toxic, got {:?}", names);

                router.reset_upload_profile().expect("reset_upload after apply");
                router.reset_download_profile().expect("reset_download after apply");
                let names_after_reset = router.applied_toxic_names().expect("applied_toxic_names");
                assert!(
                    names_after_reset.is_empty(),
                    "expected no toxics after reset, got {}",
                    names_after_reset.len()
                );
            })
            .await
            .expect("spawn_blocking");
        });
    }

    #[test]
    fn test_router_creation_fails_without_toxiproxy() {
        let original = std::env::var(TOXIPROXY_ADDR_ENV).ok();
        unsafe { std::env::set_var(TOXIPROXY_ADDR_ENV, "127.0.0.1:19999") };
        let result = NetworkSimulationProxy::new("http://127.0.0.1:12345");
        if let Some(ref s) = original {
            unsafe { std::env::set_var(TOXIPROXY_ADDR_ENV, s) };
        } else {
            unsafe { std::env::remove_var(TOXIPROXY_ADDR_ENV) };
        }
        if result.is_ok() {
            eprintln!("\n*** SKIPPED: Port 19999 had a listener; cannot assert creation failure. ***");
            return;
        }
        assert!(matches!(result, Err(SimulationError::Toxiproxy(_))));
    }
}
