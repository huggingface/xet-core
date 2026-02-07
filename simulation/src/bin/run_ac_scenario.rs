//! Runs a single adaptive-concurrency scenario. Writes timeline and client stats to --out-dir
//! and directs logs there via xet_config (HF_XET_LOG_DEST).
//!
//! Bandwidth is enforced by the custom proxy (bandwidth_limit_router) when --bandwidth is set;
//! run_scenario starts the bandwidth-limit proxy in front of the test server.

#![cfg(not(target_family = "wasm"))]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use simulation::adaptive_concurrency::{DEFAULT_SERVER_DELAY_MS, Scenario, build_upload_profile, run_scenario};
use simulation::network_simulation::{CyclingProfileProvider, NetworkProfile, NetworkProfileProvider};
use xet_logging::{LoggingConfig, init as init_logging};
use xet_runtime::XetRuntime;

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

    #[arg(long, default_value_t = DEFAULT_SERVER_DELAY_MS.0)]
    server_delay_min_ms: u64,

    #[arg(long, default_value_t = DEFAULT_SERVER_DELAY_MS.1)]
    server_delay_max_ms: u64,

    /// Directory for this run (timeline.csv, client_stats_*.json, and log file).
    #[arg(long)]
    out_dir: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    std::fs::create_dir_all(&args.out_dir)?;

    let log_dest = format!("{}/", args.out_dir.display());
    unsafe { std::env::set_var("HF_XET_LOG_DEST", &log_dest) };
    init_logging(LoggingConfig::default_to_directory("run_ac_scenario".to_string(), &args.out_dir));

    let scenario = Scenario::from_name(&args.scenario).ok_or_else(|| format!("Unknown scenario: {}", args.scenario))?;

    let bandwidth = args.bandwidth.as_deref().and_then(|s| {
        let s = s.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("default") {
            None
        } else {
            Some(s.to_string())
        }
    });
    let latency = args.latency.as_deref().and_then(|s| {
        let s = s.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("default") {
            None
        } else {
            Some(s.to_string())
        }
    });
    let congestion = args.congestion.trim();
    let profile: NetworkProfile =
        build_upload_profile(bandwidth.as_deref(), latency.as_deref(), args.jitter.as_deref(), Some(congestion))?;

    let profile_provider: Option<Arc<dyn NetworkProfileProvider + Send + Sync>> =
        if (congestion.eq_ignore_ascii_case("heavy") || congestion.eq_ignore_ascii_case("medium"))
            && !profile.is_empty()
        {
            let degraded = if congestion.eq_ignore_ascii_case("heavy") {
                profile.for_heavy_degraded_phase()
            } else {
                profile.for_medium_degraded_phase()
            };
            Some(Arc::new(CyclingProfileProvider {
                normal: profile.clone(),
                degraded,
                period_secs: 30,
            }))
        } else {
            None
        };

    let server_delay_ms = Some((args.server_delay_min_ms, args.server_delay_max_ms));
    let xet = XetRuntime::new().map_err(|e| e.to_string())?;
    let run_result = xet.external_run_async_task(async move {
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(run_scenario(
                scenario,
                args.duration_sec,
                Some(profile),
                profile_provider,
                server_delay_ms,
                None,
                &args.out_dir,
            ))
        })
        .await
        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?
        .map(|_| ())
    });
    run_result.map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))??;

    Ok(())
}
