//! Scenario definitions and runner: LocalTestServer + bandwidth proxy + upload clients + reporting.
//!
//! Each scenario is a separate async function that mirrors the original bash scripts:
//! it calls `run_upload_clients` one or more times (possibly in parallel or staggered).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cas_client::simulation::local_server::ServerDelayProfile;
use cas_client::{LocalTestServer, LocalTestServerBuilder, RemoteSimulationClient};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tracing::{debug, info};

use super::client_runner::run_upload_clients;
use super::common::NetworkStats;
use super::reporting::{generate_summary_csv, generate_timeline_csv};
use crate::network_simulation::{
    BandwidthLimitProxyGuard, BandwidthRecording, DelayProfileProvider, FixedDelayProfileProvider, NetworkProfile,
    NetworkProfileProvider, ProxyConfig, endpoint_to_host_port, start_bandwidth_limit_proxy_with_config,
    start_bandwidth_limit_proxy_with_provider,
};

const DELAY_PROFILE_POLL_INTERVAL_MS: u64 = 250;

/// Scenario identifier; each corresponds to a scenario function below.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    SanityCheck,
    SingleUpload,
    GitxetUploadBurst,
    AddedUploads,
}

impl Scenario {
    pub fn name(self) -> &'static str {
        match self {
            Scenario::SanityCheck => "sanity_check",
            Scenario::SingleUpload => "single_upload",
            Scenario::GitxetUploadBurst => "gitxet_upload_burst",
            Scenario::AddedUploads => "added_uploads",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "sanity_check" => Some(Scenario::SanityCheck),
            "single_upload" => Some(Scenario::SingleUpload),
            "gitxet_upload_burst" => Some(Scenario::GitxetUploadBurst),
            "added_uploads" => Some(Scenario::AddedUploads),
            _ => None,
        }
    }
}

const MIN_DATA_KB: u64 = 49152;
const MAX_DATA_KB: u64 = 65536;

/// Sanity check: one client, short duration (mirrors test_scenarios/sanity_check).
pub async fn scenario_sanity_check(
    proxy_addr: &str,
    scenario_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let duration = duration_override.unwrap_or(60);
    run_upload_clients(proxy_addr, scenario_dir, MIN_DATA_KB, MAX_DATA_KB, duration).await?;
    Ok(())
}

/// Single client upload for an extended period (mirrors scenarios/single_upload).
pub async fn scenario_single_upload(
    proxy_addr: &str,
    scenario_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let duration = duration_override.unwrap_or(600);
    run_upload_clients(proxy_addr, scenario_dir, MIN_DATA_KB, MAX_DATA_KB, duration).await?;
    Ok(())
}

/// Burst of simultaneous clients with different run lengths (mirrors scenarios/gitxet_upload_burst).
pub async fn scenario_gitxet_upload_burst(
    proxy_addr: &str,
    scenario_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_duration = duration_override.unwrap_or(600);
    let client_durations = [600u64, 500, 500, 400, 400, 300, 300, 200, 200, 100];
    let mut handles = Vec::with_capacity(client_durations.len());
    for &d in &client_durations {
        let duration = d.min(base_duration);
        let addr = proxy_addr.to_string();
        let dir = scenario_dir.to_path_buf();
        handles.push(tokio::spawn(
            async move { run_upload_clients(&addr, &dir, MIN_DATA_KB, MAX_DATA_KB, duration).await },
        ));
    }
    for h in handles {
        h.await??;
    }
    Ok(())
}

/// Staggered client starts (mirrors scenarios/added_uploads): start clients in waves with sleep between.
pub async fn scenario_added_uploads(
    proxy_addr: &str,
    scenario_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _base = duration_override.unwrap_or(600);
    let addr = proxy_addr.to_string();
    let dir = scenario_dir.to_path_buf();
    let mut all_handles = vec![];

    all_handles.push(tokio::spawn({
        let a = addr.clone();
        let d = dir.clone();
        async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, 600).await }
    }));
    sleep(std::time::Duration::from_secs(100)).await;

    for duration in [500u64, 150, 100] {
        let a = addr.clone();
        let d = dir.clone();
        all_handles
            .push(tokio::spawn(async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, duration).await }));
    }
    sleep(std::time::Duration::from_secs(100)).await;

    for _ in 0..2 {
        let a = addr.clone();
        let d = dir.clone();
        all_handles.push(tokio::spawn(async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, 200).await }));
    }
    sleep(std::time::Duration::from_secs(100)).await;

    for _ in 0..6 {
        let a = addr.clone();
        let d = dir.clone();
        all_handles.push(tokio::spawn(async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, 30).await }));
    }
    sleep(std::time::Duration::from_secs(100)).await;

    all_handles.push(tokio::spawn({
        let a = addr.clone();
        let d = dir.clone();
        async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, 200).await }
    }));
    sleep(std::time::Duration::from_secs(100)).await;

    all_handles.push(tokio::spawn({
        let a = addr.clone();
        let d = dir.clone();
        async move { run_upload_clients(&a, &d, MIN_DATA_KB, MAX_DATA_KB, 100).await }
    }));
    sleep(std::time::Duration::from_secs(100)).await;

    for h in all_handles {
        h.await??;
    }
    Ok(())
}

/// Build upload NetworkProfile from optional bandwidth/latency/congestion strings.
/// Congestion: "none" | "medium" | "heavy" | "bursty". Bandwidth e.g. "10mbps", latency e.g. "50ms".
/// "realistic" (varying conditions over time) is not supported; would require a
/// background profile-update loop.
pub fn build_upload_profile(
    bandwidth: Option<&str>,
    latency: Option<&str>,
    jitter: Option<&str>,
    congestion: Option<&str>,
) -> Result<NetworkProfile, Box<dyn std::error::Error + Send + Sync>> {
    let base = match congestion {
        None | Some("none") => NetworkProfile::none(),
        Some("medium") => NetworkProfile::medium(),
        Some("heavy") => NetworkProfile::heavy_congestion(),
        Some("bursty") => NetworkProfile::bursty(),
        Some("realistic") => {
            return Err("congestion 'realistic' (varying latency/bandwidth over time) is not supported; would require a background profile-update loop".into());
        },
        Some(_) => NetworkProfile::none(),
    };
    let mut p = base;
    if let Some(b) = bandwidth {
        p = p.with_bandwidth(b)?;
    }
    if let (Some(l), Some(j)) = (latency, jitter) {
        p = p.with_latency(l, j)?;
    } else if let Some(l) = latency {
        p = p.with_latency(l, "0ms")?;
    }
    Ok(p)
}

/// Default scenario duration in seconds (used for network_stats schedule writing).
fn default_duration_sec(scenario: Scenario) -> u64 {
    match scenario {
        Scenario::SanityCheck => 60,
        Scenario::SingleUpload => 600,
        Scenario::GitxetUploadBurst => 600,
        Scenario::AddedUploads => 600,
    }
}

/// Writes network_stats.json so summary.csv can compute network_utilization_percent.
/// When `recording` is Some (from provider-based proxy), writes one line per sample (elapsed_ms, bandwidth, latency).
/// Otherwise uses `profile` for a single fixed segment. `duration_sec` is the run length for the last segment.
fn write_network_stats(
    scenario_dir: &Path,
    profile: Option<&NetworkProfile>,
    recording: Option<&BandwidthRecording>,
    duration_sec: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = scenario_dir.join("network_stats.json");
    let lines = if let Some(rec) = recording {
        let duration_ms = (duration_sec * 1000.0) as u64;
        let mut out = Vec::new();
        for (elapsed, bandwidth_bytes_per_sec, latency_ms) in rec {
            let time_ms = elapsed.as_millis() as u64;
            if time_ms >= duration_ms {
                break;
            }
            let stat = NetworkStats {
                timestamp: time_ms.to_string(),
                latency_ms: *latency_ms,
                bandwidth_bytes_per_sec: *bandwidth_bytes_per_sec,
                congestion_mode: None,
                interface: None,
            };
            out.push(serde_json::to_string(&stat)?);
        }
        out.join("\n") + "\n"
    } else {
        let bandwidth_bytes_per_sec = profile.and_then(|p| p.bandwidth_kbps).map(|k| k as u64 * 1000 / 8).unwrap_or(0);
        let latency_ms = profile.and_then(|p| p.latency_ms).map(f64::from).unwrap_or(0.0);
        let stat = NetworkStats {
            timestamp: "0".to_string(),
            latency_ms,
            bandwidth_bytes_per_sec,
            congestion_mode: None,
            interface: None,
        };
        serde_json::to_string(&stat)? + "\n"
    };
    std::fs::write(path, lines)?;
    Ok(())
}

/// Runs the delay profile update loop: every 250ms sets the server delay profile from the provider.
async fn delay_profile_update_loop(
    client: Arc<RemoteSimulationClient>,
    provider: Arc<dyn DelayProfileProvider + Send + Sync>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let start = Instant::now();
    if let Err(e) = client.simulation_set_delay_profile(provider.profile_at(Duration::ZERO)).await {
        tracing::warn!(error = %e, "initial delay profile set failed");
    }
    let mut ticker = interval(Duration::from_millis(DELAY_PROFILE_POLL_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("delay profile update loop shutting down");
                break;
            }
            _ = ticker.tick() => {
                let elapsed = start.elapsed();
                let profile = provider.profile_at(elapsed);
                if let Err(e) = client.simulation_set_delay_profile(profile).await {
                    tracing::warn!(error = %e, "delay profile update failed");
                }
            }
        }
    }
}

/// Holds server, optional bandwidth-limit proxy guard, and optional delay-loop shutdown sender.
pub struct ScenarioCleanup {
    #[allow(dead_code)]
    server: LocalTestServer,
    #[allow(dead_code)]
    bandwidth_guard: Option<BandwidthLimitProxyGuard>,
    delay_shutdown_tx: Option<broadcast::Sender<()>>,
}

impl Drop for ScenarioCleanup {
    fn drop(&mut self) {
        if let Some(tx) = self.delay_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Server API delay range (min_ms, max_ms). (0, 0) means no delay.
pub type ServerDelayMs = (u64, u64);

/// Default server delay: 400–1000 ms for API calls.
pub const DEFAULT_SERVER_DELAY_MS: ServerDelayMs = (400, 1000);

/// Starts LocalTestServer and bandwidth-limit proxy, applies upload profile, runs the given scenario
/// (which may call `run_upload_clients` multiple times), then generates timeline.csv.
/// Returns a cleanup guard; drop it after `block_on` returns so the proxy is dropped in sync context.
///
/// When `profile_provider` is Some, the bandwidth proxy uses it (profile at elapsed time every 250ms)
/// and records bandwidth for baseline; network_stats.json is written from the recording after the run.
/// When `delay_profile_provider` is Some, the server's delay profile is updated every 250ms from it.
/// `server_delay_ms`: fixed random delay range (min_ms, max_ms) when delay_profile_provider is None. (0, 0) = no delay.
/// Default when None is (400, 1000).
pub async fn run_scenario(
    scenario: Scenario,
    duration_override: Option<u64>,
    upload_profile: Option<NetworkProfile>,
    profile_provider: Option<Arc<dyn NetworkProfileProvider + Send + Sync>>,
    server_delay_ms: Option<ServerDelayMs>,
    delay_profile_provider: Option<Arc<dyn DelayProfileProvider + Send + Sync>>,
    scenario_dir: &Path,
) -> Result<ScenarioCleanup, Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(scenario_dir)?;

    let delay_provider: Option<Arc<dyn DelayProfileProvider + Send + Sync>> =
        if let Some(provider) = delay_profile_provider {
            Some(provider)
        } else {
            server_delay_ms
                .or(Some(DEFAULT_SERVER_DELAY_MS))
                .filter(|(min, max)| *min > 0 || *max > 0)
                .map(|(min_ms, max_ms)| {
                    Arc::new(FixedDelayProfileProvider(ServerDelayProfile {
                        random_delay_ms: Some((min_ms, max_ms.max(min_ms))),
                        ..ServerDelayProfile::default()
                    })) as Arc<dyn DelayProfileProvider + Send + Sync>
                })
        };

    let builder = LocalTestServerBuilder::new().with_ephemeral_disk();
    let server = builder.start().await;

    let delay_shutdown_tx = if let Some(provider) = &delay_provider {
        let (tx, rx) = broadcast::channel(1);
        let client = server.remote_simulation_client().clone();
        tokio::spawn(delay_profile_update_loop(client, provider.clone(), rx));
        Some(tx)
    } else {
        None
    };
    let endpoint = server.http_endpoint().to_string();
    let server_host_port = endpoint_to_host_port(&endpoint)
        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;

    let (client_endpoint, bandwidth_guard) = if let Some(provider) = &profile_provider {
        let (listen_addr, guard) = start_bandwidth_limit_proxy_with_provider(&server_host_port, provider.clone())
            .await
            .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;
        (format!("http://{}", listen_addr), Some(guard))
    } else if let Some(ref profile) = upload_profile {
        if !profile.is_empty() {
            let config = ProxyConfig::from_profile(profile);
            let (listen_addr, guard) = start_bandwidth_limit_proxy_with_config(&server_host_port, &config)
                .await
                .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;
            (format!("http://{}", listen_addr), Some(guard))
        } else {
            (endpoint.clone(), None)
        }
    } else {
        (endpoint.clone(), None)
    };

    let proxy_addr = client_endpoint.trim_start_matches("http://").to_string();
    let duration_sec = duration_override.unwrap_or_else(|| default_duration_sec(scenario)) as f64;
    if profile_provider.is_none() {
        write_network_stats(scenario_dir, upload_profile.as_ref(), None, duration_sec)?;
    }
    let start = Instant::now();

    match scenario {
        Scenario::SanityCheck => scenario_sanity_check(&proxy_addr, scenario_dir, duration_override).await?,
        Scenario::SingleUpload => scenario_single_upload(&proxy_addr, scenario_dir, duration_override).await?,
        Scenario::GitxetUploadBurst => {
            scenario_gitxet_upload_burst(&proxy_addr, scenario_dir, duration_override).await?
        },
        Scenario::AddedUploads => scenario_added_uploads(&proxy_addr, scenario_dir, duration_override).await?,
    }

    info!(scenario = scenario.name(), elapsed_sec = start.elapsed().as_secs_f64(), "Scenario run finished");

    if let Some(ref guard) = bandwidth_guard {
        if let Some(recording) = guard.take_bandwidth_recording() {
            write_network_stats(scenario_dir, None, Some(&recording), duration_sec)?;
        }
    }
    generate_timeline_csv(scenario_dir)?;
    Ok(ScenarioCleanup {
        server,
        bandwidth_guard,
        delay_shutdown_tx,
    })
}

/// Generates summary.csv for a results directory (timestamp dir containing scenario subdirs).
pub fn aggregate_summary(results_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    generate_summary_csv(results_dir)
}
