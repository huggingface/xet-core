//! Scenario definitions and runner: LocalTestServer + Toxiproxy + upload clients + reporting.
//!
//! Each scenario is a separate async function that mirrors the original bash scripts:
//! it calls `run_upload_clients` one or more times (possibly in parallel or staggered).

use std::path::Path;
use std::time::Instant;

use cas_client::simulation::local_server::ServerDelayProfile;
use cas_client::{LocalTestServer, LocalTestServerBuilder};
use tokio::time::sleep;
use tracing::info;

use super::client_runner::run_upload_clients;
use super::reporting::{generate_summary_csv, generate_timeline_csv};
use crate::network_simulation::{NetworkProfile, NetworkSimulationProxy};

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
    let duration = duration_override.unwrap_or(20);
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
/// "realistic" (varying conditions over time) is not supported; Toxiproxy would require a
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

/// Holds server and proxy so the caller can drop them in sync context (avoids dropping
/// Toxiproxy's blocking client from within async context).
pub struct ScenarioCleanup {
    #[allow(dead_code)]
    server: LocalTestServer,
    #[allow(dead_code)]
    proxy: NetworkSimulationProxy,
}

/// Server API delay range (min_ms, max_ms). (0, 0) means no delay.
pub type ServerDelayMs = (u64, u64);

/// Default server delay: 400–1000 ms for API calls.
pub const DEFAULT_SERVER_DELAY_MS: ServerDelayMs = (400, 1000);

/// Starts LocalTestServer and Toxiproxy proxy, applies upload profile, runs the given scenario
/// (which may call `run_upload_clients` multiple times), then generates timeline.csv.
/// Returns a cleanup guard; drop it after `block_on` returns so the proxy is dropped in sync context.
///
/// `server_delay_ms`: random delay range (min_ms, max_ms) for server API calls. (0, 0) = no delay.
/// Default when None is (400, 1000).
pub async fn run_scenario(
    scenario: Scenario,
    duration_override: Option<u64>,
    upload_profile: Option<NetworkProfile>,
    server_delay_ms: Option<ServerDelayMs>,
    scenario_dir: &Path,
) -> Result<ScenarioCleanup, Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(scenario_dir)?;

    let delay_profile = server_delay_ms
        .or(Some(DEFAULT_SERVER_DELAY_MS))
        .filter(|(min, max)| *min > 0 || *max > 0)
        .map(|(min_ms, max_ms)| ServerDelayProfile {
            random_delay_ms: Some((min_ms, max_ms.max(min_ms))),
            ..ServerDelayProfile::default()
        });

    let mut builder = LocalTestServerBuilder::new().with_ephemeral_disk();
    if let Some(profile) = delay_profile {
        builder = builder.with_server_delay_profile(profile);
    }
    let server = builder.start().await;
    let endpoint = server.http_endpoint().to_string();
    let profile_opt = upload_profile.clone();

    let proxy: NetworkSimulationProxy = tokio::task::spawn_blocking(move || {
        let mut p = NetworkSimulationProxy::new(&endpoint).map_err(|e| {
            let msg = format!("Toxiproxy not available: {e}. Start Toxiproxy or set TOXIPROXY_ADDR.");
            Box::<dyn std::error::Error + Send + Sync>::from(msg)
        })?;
        if let Some(profile) = profile_opt {
            p.apply_upload_profile(profile)
                .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(p)
    })
    .await
    .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))??;

    let proxy_addr = proxy.proxy_endpoint().trim_start_matches("http://").to_string();
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

    generate_timeline_csv(scenario_dir)?;
    Ok(ScenarioCleanup { server, proxy })
}

/// Generates summary.csv for a results directory (timestamp dir containing scenario subdirs).
pub fn aggregate_summary(results_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    generate_summary_csv(results_dir)
}
