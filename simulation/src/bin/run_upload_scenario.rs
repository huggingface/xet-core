//! Runs a single upload-concurrency scenario.
//!
//! Starts a LocalTestServer with optional network shaping and server latency simulation,
//! runs upload clients according to the chosen scenario, then writes
//! network_stats.json and timeline.csv to the output directory.

#![cfg(not(target_family = "wasm"))]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cas_client::simulation::local_server::ServerLatencyProfile;
use clap::Parser;
use simulation::scenario::{
    MAX_DATA_KB, MIN_DATA_KB, ScenarioError, ScenarioResult, SimulationScenario, SimulationScenarioBuilder,
    VALID_SCENARIOS,
};
use simulation::upload_concurrency::run_upload_clients_until_cancelled;
use tokio::time::sleep;
use tracing::info;
use xet_logging::{LoggingConfig, init as init_logging};
use xet_runtime::XetRuntime;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, default_value = "sanity_check")]
    scenario: String,

    #[arg(long)]
    duration_sec: Option<u64>,

    /// e.g. 1gbps
    #[arg(long, default_value = "1gbps")]
    bandwidth: String,

    /// e.g. 50ms
    #[arg(long, default_value = "50ms")]
    latency: String,

    /// e.g. 20ms
    #[arg(long, default_value = "20ms")]
    jitter: String,

    #[arg(long, default_value = "none")]
    congestion: String,

    /// Server latency profile: none, light, realistic, heavy.
    #[arg(long, default_value = "realistic")]
    server_latency_profile: String,

    /// Connection degradation threshold (number of concurrent connections).
    /// When set, congestion penalties and error rates from the latency profile activate
    /// once active connections exceed this value.
    #[arg(long)]
    server_connection_degradation_threshold: Option<u64>,

    /// Directory for this run (timeline.csv, client_stats_*.json, and log file).
    #[arg(long)]
    out_dir: PathBuf,
}

// ── Scenario functions ───────────────────────────────────────────────────────

/// Adds an upload client task that runs until the scenario's finish (or its token is cancelled).
/// Returns the cancellation token for this task so the caller can cancel it early if desired.
fn add_upload_client_task(scenario: &mut SimulationScenario) -> tokio_util::sync::CancellationToken {
    let addr = scenario.server().http_endpoint().to_string();
    let out_dir = scenario.out_dir().to_path_buf();
    scenario.add_task(move |token| {
        tokio::spawn(async move {
            run_upload_clients_until_cancelled(&addr, &out_dir, MIN_DATA_KB, MAX_DATA_KB, token)
                .await
                .map_err(|e| ScenarioError::Client(e.to_string()))
        })
    })
}

/// Single client upload for a given duration (used by both sanity_check and single_upload scenarios).
async fn run_single_upload(mut scenario: SimulationScenario, duration_sec: u64) -> ScenarioResult<()> {
    add_upload_client_task(&mut scenario);
    sleep(Duration::from_secs(duration_sec)).await;
    scenario.finish().await
}

/// Burst of simultaneous clients.
async fn run_gitxet_upload_burst(mut scenario: SimulationScenario, duration_sec: u64) -> ScenarioResult<()> {
    for _ in 0..10 {
        add_upload_client_task(&mut scenario);
    }
    sleep(Duration::from_secs(duration_sec)).await;
    scenario.finish().await
}

/// Staggered client starts: launch clients in waves with pauses between.
async fn run_added_uploads(mut scenario: SimulationScenario, _duration_sec: u64) -> ScenarioResult<()> {
    add_upload_client_task(&mut scenario);
    sleep(Duration::from_secs(100)).await;

    for _ in 0..3 {
        add_upload_client_task(&mut scenario);
    }
    sleep(Duration::from_secs(100)).await;

    for _ in 0..2 {
        add_upload_client_task(&mut scenario);
    }
    sleep(Duration::from_secs(100)).await;

    for _ in 0..6 {
        add_upload_client_task(&mut scenario);
    }
    sleep(Duration::from_secs(100)).await;

    add_upload_client_task(&mut scenario);
    sleep(Duration::from_secs(100)).await;

    add_upload_client_task(&mut scenario);
    sleep(Duration::from_secs(100)).await;

    scenario.finish().await
}

// ── Utilities ────────────────────────────────────────────────────────────────

/// Parses a duration string (duration_str crate: e.g. "10ms", "1s").
fn parse_duration(s: &str) -> ScenarioResult<Duration> {
    duration_str::parse(s).map_err(|e| ScenarioError::Scenario(format!("invalid duration {:?}: {}", s, e)))
}

fn setup_logging(out_dir: &Path) {
    let log_dest = format!("{}/", out_dir.display());
    // SAFETY: Called from main() before any threads are spawned.
    unsafe { std::env::set_var("HF_XET_LOG_DEST", &log_dest) };
    init_logging(LoggingConfig::default_to_directory("run_upload_scenario".to_string(), out_dir));
}

/// Runs an async future on a fresh multi-threaded tokio runtime, using XetRuntime for initialization.
fn run_async<F>(future: F) -> ScenarioResult<()>
where
    F: Future<Output = ScenarioResult<()>> + Send + 'static,
{
    let xet = XetRuntime::new().map_err(|e| ScenarioError::Runtime(e.to_string()))?;
    xet.external_run_async_task(async move {
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| ScenarioError::Runtime(e.to_string()))?
                .block_on(future)
        })
        .await
        .map_err(ScenarioError::from)?
    })
    .map_err(|e| ScenarioError::Runtime(e.to_string()))?
}

// ── Entry point ──────────────────────────────────────────────────────────────

fn main() -> ScenarioResult<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out_dir)?;
    setup_logging(&args.out_dir);

    let scenario = args.scenario.as_str();
    if !VALID_SCENARIOS.contains(&scenario) {
        return Err(ScenarioError::UnknownScenario {
            name: scenario.to_string(),
            valid: VALID_SCENARIOS,
        });
    }

    let lat = parse_duration(&args.latency)?;
    let jit = parse_duration(&args.jitter)?;
    let latency_profile_name = args.server_latency_profile.trim().to_string();
    let mut latency_profile = ServerLatencyProfile::from_name(&latency_profile_name).ok_or_else(|| {
        ScenarioError::Scenario(format!(
            "Unknown server latency profile: {latency_profile_name}. Valid: {:?}",
            ServerLatencyProfile::VALID_NAMES
        ))
    })?;
    if let Some(threshold) = args.server_connection_degradation_threshold {
        latency_profile = latency_profile.with_connection_degradation_threshold(threshold);
    }

    let duration_sec = args
        .duration_sec
        .unwrap_or_else(|| if scenario == "sanity_check" { 60 } else { 600 });
    let scenario_name = args.scenario.clone();
    let out_dir = args.out_dir.clone();
    let bandwidth = args.bandwidth.clone();
    let congestion = args.congestion.trim().to_lowercase();

    run_async(async move {
        let mut builder = SimulationScenarioBuilder::new()
            .with_out_dir(out_dir)
            .with_latency_profile(latency_profile)
            .with_server_latency_profile_name(&latency_profile_name)
            .with_latency(lat, jit)
            .with_congestion_strength(1.0);
        builder = builder.with_bandwidth_str(&bandwidth)?;
        builder = builder.with_congestion(&congestion)?;
        let scenario = builder.start().await?;

        let start = Instant::now();
        let result = match scenario_name.as_str() {
            "sanity_check" | "single_upload" => run_single_upload(scenario, duration_sec).await,
            "gitxet_upload_burst" => run_gitxet_upload_burst(scenario, duration_sec).await,
            "added_uploads" => run_added_uploads(scenario, duration_sec).await,
            _ => unreachable!("scenario already validated"),
        };
        info!(scenario = scenario_name.as_str(), elapsed_sec = start.elapsed().as_secs_f64(), "Scenario finished");
        result
    })
}
