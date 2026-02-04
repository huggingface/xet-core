//! Adaptive concurrency simulation binary: run scenarios against LocalTestServer dummy_upload
//! through Toxiproxy, with timeline and summary CSV output.
//!
//! Each scenario runs in its own std thread with a dedicated XetRuntime; parallelism is
//! limited by a std::sync-style semaphore.

#![cfg(not(target_family = "wasm"))]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use simulation::adaptive_concurrency::{
    DEFAULT_SERVER_DELAY_MS, Scenario, aggregate_summary, build_upload_profile, run_scenario,
};
use simulation::network_simulation::NetworkProfile;
use xet_runtime::XetRuntime;

/// Blocking semaphore using std::sync primitives (limits how many scenarios run at once).
struct StdSemaphore {
    permits: std::sync::Mutex<usize>,
    condvar: std::sync::Condvar,
}

impl StdSemaphore {
    fn new(permits: usize) -> Self {
        Self {
            permits: std::sync::Mutex::new(permits),
            condvar: std::sync::Condvar::new(),
        }
    }

    fn acquire(&self) -> StdSemaphorePermit<'_> {
        let mut g = self.permits.lock().expect("semaphore mutex");
        while *g == 0 {
            g = self.condvar.wait(g).expect("condvar wait");
        }
        *g -= 1;
        StdSemaphorePermit { sem: self }
    }
}

struct StdSemaphorePermit<'a> {
    sem: &'a StdSemaphore,
}

impl Drop for StdSemaphorePermit<'_> {
    fn drop(&mut self) {
        let mut g = self.sem.permits.lock().expect("semaphore mutex");
        *g += 1;
        self.sem.condvar.notify_one();
    }
}

/// Parse comma-separated list; "default" or empty string means None (use default).
fn parse_list(s: &str) -> Vec<Option<String>> {
    if s.trim().is_empty() {
        return vec![None];
    }
    s.split(',')
        .map(|v| {
            let v = v.trim();
            if v.is_empty() || v.eq_ignore_ascii_case("default") {
                None
            } else {
                Some(v.to_string())
            }
        })
        .collect()
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Scenario name: sanity_check, single_upload, gitxet_upload_burst, added_uploads
    #[arg(long, default_value = "sanity_check")]
    scenario: String,

    /// Override duration in seconds (default from scenario)
    #[arg(long)]
    duration_sec: Option<u64>,

    /// Bandwidth(s), comma-separated. "default" or omit = no limit. e.g. 10mbps,100mbps
    #[arg(long)]
    bandwidth: Option<String>,

    /// Latency(s), comma-separated. "default" or omit = no added latency. e.g. 10ms,50ms
    #[arg(long)]
    latency: Option<String>,

    /// Jitter e.g. 5ms (used with latency; single value for all latency runs)
    #[arg(long)]
    jitter: Option<String>,

    /// Congestion mode(s), comma-separated: none, medium, heavy, bursty
    #[arg(long, default_value = "none")]
    congestion: String,

    /// Server API delay min (ms). With server_delay_max, sets random_delay_ms on LocalTestServer. 0,0 = no delay.
    #[arg(long, default_value_t = DEFAULT_SERVER_DELAY_MS.0)]
    server_delay_min_ms: u64,

    /// Server API delay max (ms). With server_delay_min, sets random_delay_ms on LocalTestServer.
    #[arg(long, default_value_t = DEFAULT_SERVER_DELAY_MS.1)]
    server_delay_max_ms: u64,

    /// Output directory (default: results/). A timestamped subdir is created.
    #[arg(long, default_value = "results")]
    out_dir: String,

    /// Skip generating summary.csv (only generate timeline.csv for this run)
    #[arg(long)]
    no_summary: bool,

    /// Maximum number of scenario runs in parallel (1 = sequential).
    #[arg(long, default_value_t = 1)]
    max_parallel: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(false)
        .try_init()
        .ok();

    let args = Args::parse();

    let scenario = Scenario::from_name(&args.scenario).ok_or_else(|| format!("Unknown scenario: {}", args.scenario))?;

    let bandwidths = parse_list(args.bandwidth.as_deref().unwrap_or(""));
    let latencies = parse_list(args.latency.as_deref().unwrap_or(""));
    let congestions: Vec<String> = args
        .congestion
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let congestions = if congestions.is_empty() {
        vec!["none".to_string()]
    } else {
        congestions
    };

    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let results_base = PathBuf::from(&args.out_dir).join(&timestamp);
    std::fs::create_dir_all(&results_base)?;

    let semaphore = Arc::new(StdSemaphore::new(args.max_parallel));
    let mut handles = Vec::new();

    for bandwidth in &bandwidths {
        for latency in &latencies {
            for congestion in &congestions {
                let profile: NetworkProfile = build_upload_profile(
                    bandwidth.as_deref(),
                    latency.as_deref(),
                    args.jitter.as_deref(),
                    Some(congestion),
                )?;
                let bandwidth_label = bandwidth.as_deref().unwrap_or("default");
                let latency_label = latency.as_deref().unwrap_or("default");
                let scenario_subdir =
                    format!("{}-{}-{}-{}", scenario.name(), bandwidth_label, latency_label, congestion);
                let scenario_dir = results_base.join(&scenario_subdir);
                std::fs::create_dir_all(&scenario_dir)?;

                let scenario = scenario;
                let duration_sec = args.duration_sec;
                let server_delay_ms = Some((args.server_delay_min_ms, args.server_delay_max_ms));
                let sem = Arc::clone(&semaphore);
                let scenario_dir = scenario_dir.clone();
                let bandwidth_label = bandwidth_label.to_string();
                let latency_label = latency_label.to_string();
                let congestion = congestion.clone();

                handles.push(std::thread::spawn(move || {
                    let _permit = sem.acquire();
                    eprintln!(
                        "Running {} (bandwidth={}, latency={}, congestion={})",
                        scenario.name(),
                        bandwidth_label,
                        latency_label,
                        congestion
                    );
                    let xet = XetRuntime::new().map_err(|e| e.to_string())?;
                    xet.external_run_async_task(async move {
                        tokio::task::spawn_blocking(move || {
                            let rt = tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .expect("runtime");
                            rt.block_on(run_scenario(
                                scenario,
                                duration_sec,
                                Some(profile),
                                server_delay_ms,
                                &scenario_dir,
                            ))
                        })
                        .await
                        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?
                        .map(|_| ())
                    })
                    .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?
                }));
            }
        }
    }

    for h in handles {
        h.join()
            .map_err(|_| Box::<dyn std::error::Error + Send + Sync>::from("scenario thread panicked"))??;
    }

    if !args.no_summary {
        aggregate_summary(&results_base)?;
    }

    eprintln!("Results in {}", results_base.display());
    Ok(())
}
