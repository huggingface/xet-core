//! Runs a single adaptive-concurrency scenario.
//!
//! Starts a LocalTestServer with optional network shaping and server load simulation,
//! runs upload clients according to the chosen scenario, then writes
//! network_stats.json and timeline.csv to the output directory.

#![cfg(not(target_family = "wasm"))]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cas_client::simulation::local_server::ServerLoadProfile;
use cas_client::simulation::{
    BandwidthRecording, CongestionProfile, NetworkProfile, NetworkProfileBuilder, NetworkProfileProvider,
    parse_duration,
};
use cas_client::{CasClientError, LocalTestServerBuilder};
use clap::Parser;
use simulation::adaptive_concurrency::{NetworkStats, generate_timeline_csv, run_upload_clients};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::info;
use xet_logging::{LoggingConfig, init as init_logging};
use xet_runtime::XetRuntime;

const VALID_SCENARIOS: &[&str] = &["sanity_check", "single_upload", "gitxet_upload_burst", "added_uploads"];

const MIN_DATA_KB: u64 = 49152;
const MAX_DATA_KB: u64 = 65536;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, default_value = "sanity_check")]
    scenario: String,

    #[arg(long)]
    duration_sec: Option<u64>,

    /// Single value or "default". e.g. 10mbps
    #[arg(long)]
    bandwidth: Option<String>,

    /// Single value or "default". e.g. 20ms
    #[arg(long)]
    latency: Option<String>,

    #[arg(long)]
    jitter: Option<String>,

    #[arg(long, default_value = "none")]
    congestion: String,

    /// Server load profile: none, light, realistic, heavy.
    #[arg(long, default_value = "realistic")]
    server_load_profile: String,

    /// Connection degradation threshold (number of concurrent connections).
    /// When set, congestion penalties and error rates from the load profile activate
    /// once active connections exceed this value.
    #[arg(long)]
    server_connection_degradation_threshold: Option<u64>,

    /// Directory for this run (timeline.csv, client_stats_*.json, and log file).
    #[arg(long)]
    out_dir: PathBuf,
}

// ── Scenario functions ───────────────────────────────────────────────────────

/// Sanity check: one client, short duration.
async fn run_sanity_check(
    proxy_addr: &str,
    out_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_upload_clients(proxy_addr, out_dir, MIN_DATA_KB, MAX_DATA_KB, duration_override.unwrap_or(60)).await
}

/// Single client upload for an extended period.
async fn run_single_upload(
    proxy_addr: &str,
    out_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_upload_clients(proxy_addr, out_dir, MIN_DATA_KB, MAX_DATA_KB, duration_override.unwrap_or(600)).await
}

/// Burst of simultaneous clients with different run lengths.
async fn run_gitxet_upload_burst(
    proxy_addr: &str,
    out_dir: &Path,
    duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_duration = duration_override.unwrap_or(600);
    let client_durations = [600u64, 500, 500, 400, 400, 300, 300, 200, 200, 100];
    let mut handles = Vec::with_capacity(client_durations.len());
    for &d in &client_durations {
        handles.push(spawn_upload(proxy_addr, out_dir, d.min(base_duration)));
    }
    for h in handles {
        h.await??;
    }
    Ok(())
}

/// Staggered client starts: launch clients in waves with pauses between.
async fn run_added_uploads(
    proxy_addr: &str,
    out_dir: &Path,
    _duration_override: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut handles = vec![];

    // Wave 1: single long-running client
    handles.push(spawn_upload(proxy_addr, out_dir, 600));
    sleep(Duration::from_secs(100)).await;

    // Wave 2: three clients with varying durations
    for duration in [500u64, 150, 100] {
        handles.push(spawn_upload(proxy_addr, out_dir, duration));
    }
    sleep(Duration::from_secs(100)).await;

    // Wave 3: two medium clients
    for _ in 0..2 {
        handles.push(spawn_upload(proxy_addr, out_dir, 200));
    }
    sleep(Duration::from_secs(100)).await;

    // Wave 4: six short clients
    for _ in 0..6 {
        handles.push(spawn_upload(proxy_addr, out_dir, 30));
    }
    sleep(Duration::from_secs(100)).await;

    // Wave 5: one medium client
    handles.push(spawn_upload(proxy_addr, out_dir, 200));
    sleep(Duration::from_secs(100)).await;

    // Wave 6: one short client
    handles.push(spawn_upload(proxy_addr, out_dir, 100));
    sleep(Duration::from_secs(100)).await;

    for h in handles {
        h.await??;
    }
    Ok(())
}

fn spawn_upload(
    addr: &str,
    dir: &Path,
    duration: u64,
) -> JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    let addr = addr.to_string();
    let dir = dir.to_path_buf();
    tokio::spawn(async move { run_upload_clients(&addr, &dir, MIN_DATA_KB, MAX_DATA_KB, duration).await })
}

// ── Network profile ──────────────────────────────────────────────────────────

fn build_upload_profile(
    bandwidth: Option<&str>,
    latency: Option<&str>,
    jitter: Option<&str>,
    congestion: Option<&str>,
) -> Result<
    (NetworkProfile, Option<Arc<dyn NetworkProfileProvider + Send + Sync>>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let to_err = |e: CasClientError| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() };

    let lat_ms = latency.map(|s| parse_duration(s).map_err(to_err)).transpose()?.unwrap_or(50);
    let jit_ms = jitter.map(|s| parse_duration(s).map_err(to_err)).transpose()?.unwrap_or(10);
    let min_lat = lat_ms.saturating_sub(jit_ms);
    let max_lat = lat_ms + jit_ms;

    let mut builder = NetworkProfileBuilder::new()
        .with_base_latency_window(min_lat, max_lat)
        .with_congestion_strength(1.0);

    if let Some(b) = bandwidth {
        builder = builder.with_max_bandwidth(b).map_err(to_err)?;
    } else {
        builder = builder.with_max_bandwidth("10mbps").map_err(to_err)?;
    }

    let congestion_profile = match congestion.map(|s| s.trim().to_lowercase()).as_deref() {
        Some("medium") => CongestionProfile::medium(),
        Some("heavy") => CongestionProfile::heavy(),
        Some("bursty") => CongestionProfile::bursty(),
        _ => CongestionProfile::None,
    };

    let prov = builder.with_congestion_profile(congestion_profile).build_provider();
    let profile = prov.normal_profile();
    let provider = Some(Arc::new(prov) as Arc<dyn NetworkProfileProvider + Send + Sync>);

    Ok((profile, provider))
}

// ── Server setup ─────────────────────────────────────────────────────────────

const VALID_LOAD_PROFILES: &[&str] = &["none", "light", "realistic", "heavy"];

fn parse_load_profile(
    name: &str,
    degradation_threshold: Option<u64>,
) -> Result<ServerLoadProfile, Box<dyn std::error::Error + Send + Sync>> {
    let mut profile = match name {
        "none" => ServerLoadProfile::none(),
        "light" => ServerLoadProfile::light(),
        "realistic" => ServerLoadProfile::realistic(),
        "heavy" => ServerLoadProfile::heavy(),
        _ => return Err(format!("Unknown server load profile: {name}. Valid: {:?}", VALID_LOAD_PROFILES).into()),
    };
    if let Some(threshold) = degradation_threshold {
        profile = profile.with_connection_degradation_threshold(threshold);
    }
    Ok(profile)
}

// ── Reporting ────────────────────────────────────────────────────────────────

fn default_duration_sec(scenario: &str) -> u64 {
    if scenario == "sanity_check" { 60 } else { 600 }
}

fn write_network_stats(
    out_dir: &Path,
    profile: Option<&NetworkProfile>,
    recording: Option<&BandwidthRecording>,
    duration_sec: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = out_dir.join("network_stats.json");
    let content = if let Some(rec) = recording {
        let duration_ms = (duration_sec * 1000.0) as u64;
        let mut lines = Vec::new();
        for (elapsed, bandwidth_bytes_per_sec, latency_ms) in rec {
            if elapsed.as_millis() as u64 >= duration_ms {
                break;
            }
            lines.push(serde_json::to_string(&NetworkStats {
                timestamp: (elapsed.as_millis() as u64).to_string(),
                latency_ms: *latency_ms,
                bandwidth_bytes_per_sec: *bandwidth_bytes_per_sec,
                congestion_mode: None,
                interface: None,
            })?);
        }
        lines.join("\n") + "\n"
    } else {
        let bandwidth_bytes_per_sec = profile
            .and_then(|p| p.bandwidth_kbps())
            .map(|k| k as u64 * 1000 / 8)
            .unwrap_or(0);
        let latency_ms = profile.and_then(|p| p.latency_ms()).map(f64::from).unwrap_or(0.0);
        serde_json::to_string(&NetworkStats {
            timestamp: "0".to_string(),
            latency_ms,
            bandwidth_bytes_per_sec,
            congestion_mode: None,
            interface: None,
        })? + "\n"
    };
    std::fs::write(path, content)?;
    Ok(())
}

// ── Utilities ────────────────────────────────────────────────────────────────

fn normalize_optional_arg(s: Option<&str>) -> Option<String> {
    s.and_then(|s| {
        let s = s.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("default") {
            None
        } else {
            Some(s.to_string())
        }
    })
}

fn setup_logging(out_dir: &Path) {
    let log_dest = format!("{}/", out_dir.display());
    unsafe { std::env::set_var("HF_XET_LOG_DEST", &log_dest) };
    init_logging(LoggingConfig::default_to_directory("run_ac_scenario".to_string(), out_dir));
}

/// Runs an async future on a fresh multi-threaded tokio runtime, using XetRuntime for initialization.
fn run_async<F>(future: F) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
{
    let xet = XetRuntime::new().map_err(|e| e.to_string())?;
    xet.external_run_async_task(async move {
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(future)
        })
        .await
        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?
    })
    .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?
}

// ── Entry point ──────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out_dir)?;
    setup_logging(&args.out_dir);

    let scenario = args.scenario.as_str();
    if !VALID_SCENARIOS.contains(&scenario) {
        return Err(format!("Unknown scenario: {}. Valid: {:?}", scenario, VALID_SCENARIOS).into());
    }

    let bandwidth = normalize_optional_arg(args.bandwidth.as_deref());
    let latency = normalize_optional_arg(args.latency.as_deref());
    let (profile, profile_provider) = build_upload_profile(
        bandwidth.as_deref(),
        latency.as_deref(),
        args.jitter.as_deref(),
        Some(args.congestion.trim()),
    )?;

    let has_profile_provider = profile_provider.is_some();
    let load_profile =
        parse_load_profile(args.server_load_profile.trim(), args.server_connection_degradation_threshold)?;
    let duration_override = args.duration_sec;
    let duration_sec = duration_override.unwrap_or_else(|| default_duration_sec(scenario)) as f64;
    let scenario_name = args.scenario;
    let out_dir = args.out_dir;

    run_async(async move {
        let mut builder = LocalTestServerBuilder::new()
            .with_ephemeral_disk()
            .with_server_load_profile(load_profile);
        if let Some(provider) = profile_provider {
            builder = builder.with_dynamic_network_profile(provider);
        }
        let server = builder.start().await;
        let proxy_addr = server.http_endpoint().trim_start_matches("http://").to_string();

        if !has_profile_provider {
            write_network_stats(&out_dir, Some(&profile), None, duration_sec)?;
        }

        let start = Instant::now();
        if scenario_name == "sanity_check" {
            run_sanity_check(&proxy_addr, &out_dir, duration_override).await?;
        } else if scenario_name == "single_upload" {
            run_single_upload(&proxy_addr, &out_dir, duration_override).await?;
        } else if scenario_name == "gitxet_upload_burst" {
            run_gitxet_upload_burst(&proxy_addr, &out_dir, duration_override).await?;
        } else if scenario_name == "added_uploads" {
            run_added_uploads(&proxy_addr, &out_dir, duration_override).await?;
        }
        info!(scenario = scenario_name.as_str(), elapsed_sec = start.elapsed().as_secs_f64(), "Scenario finished");

        if let Some(recording) = server.take_bandwidth_recording() {
            write_network_stats(&out_dir, None, Some(&recording), duration_sec)?;
        }
        generate_timeline_csv(&out_dir)?;
        drop(server);
        Ok(())
    })
}
