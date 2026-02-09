//! Runs multiple adaptive-concurrency scenarios in parallel by spawning run_ac_scenario for each.
//! Limits concurrency with a semaphore and reports each completion. Writes aggregate summary.csv.
//!
//! Bandwidth limits are enforced by the custom proxy inside each run_ac_scenario run (see
//! run_scenario and bandwidth_limit_router); --bandwidth is passed through to the scenario.

#![cfg(not(target_family = "wasm"))]

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread;

use clap::Parser;
use simulation::adaptive_concurrency::generate_summary_csv;

const VALID_SCENARIOS: &[&str] = &["sanity_check", "single_upload", "gitxet_upload_burst", "added_uploads"];

/// Blocking semaphore (limits how many scenario processes run at once).
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
    #[arg(long, default_value = "sanity_check")]
    scenario: String,

    #[arg(long)]
    duration_sec: Option<u64>,

    #[arg(long)]
    bandwidth: Option<String>,

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
    #[arg(long)]
    server_connection_degradation_threshold: Option<u64>,

    #[arg(long, default_value = "results")]
    out_dir: String,

    #[arg(long, default_value_t = 1)]
    max_parallel: usize,

    /// Path to run_ac_scenario binary (default: same directory as this binary).
    #[arg(long)]
    scenario_bin: Option<PathBuf>,
}

fn scenario_binary() -> PathBuf {
    let current = std::env::current_exe().expect("current exe");
    let dir = current.parent().expect("exe has parent");
    #[cfg(windows)]
    let name = "run_ac_scenario.exe";
    #[cfg(not(windows))]
    let name = "run_ac_scenario";
    dir.join(name)
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(false)
        .try_init()
        .ok();

    let args = Args::parse();
    let start_time = std::time::Instant::now();

    if !VALID_SCENARIOS.contains(&args.scenario.as_str()) {
        return Err(format!("Unknown scenario: {}. Valid: {:?}", args.scenario, VALID_SCENARIOS).into());
    }

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

    let bin = args.scenario_bin.unwrap_or_else(scenario_binary);
    if !bin.exists() {
        return Err(format!(
            "run_ac_scenario binary not found at {}; build with cargo build --release -p simulation",
            bin.display()
        )
        .into());
    }

    eprintln!("=== Adaptive concurrency simulation ===");
    eprintln!("Scenario: {}", args.scenario);
    eprintln!(
        "Matrix: {} bandwidth × {} latency × {} congestion = {} runs",
        bandwidths.len(),
        latencies.len(),
        congestions.len(),
        bandwidths.len() * latencies.len() * congestions.len()
    );
    eprintln!("Max parallel: {}", args.max_parallel);
    eprintln!("Results directory: {}", results_base.display());
    if let Some(d) = args.duration_sec {
        eprintln!("Duration per run: {}s", d);
    }
    eprintln!("----------------------------------------");

    let mut tasks = Vec::new();
    for bandwidth in &bandwidths {
        for latency in &latencies {
            for congestion in &congestions {
                let bandwidth_label = bandwidth.as_deref().unwrap_or("default");
                let latency_label = latency.as_deref().unwrap_or("default");
                let scenario_subdir = format!("{}-{}-{}-{}", args.scenario, bandwidth_label, latency_label, congestion);
                let scenario_dir = results_base.join(&scenario_subdir);
                std::fs::create_dir_all(&scenario_dir)?;

                let bandwidth_arg = bandwidth.as_ref().map(|s| s.as_str()).unwrap_or("default");
                let latency_arg = latency.as_ref().map(|s| s.as_str()).unwrap_or("default");

                let jitter_arg = args.jitter.clone();
                tasks.push((
                    scenario_subdir,
                    scenario_dir,
                    args.scenario.clone(),
                    bandwidth_arg.to_string(),
                    latency_arg.to_string(),
                    congestion.clone(),
                    jitter_arg,
                    args.duration_sec,
                    args.server_load_profile.clone(),
                    args.server_connection_degradation_threshold,
                ));
            }
        }
    }

    let semaphore = Arc::new(StdSemaphore::new(args.max_parallel));
    let (tx, rx) = mpsc::channel::<(String, std::result::Result<std::process::ExitStatus, std::io::Error>)>();

    let mut handles = Vec::with_capacity(tasks.len());
    for (
        label,
        scenario_dir,
        scenario_name,
        bandwidth_arg,
        latency_arg,
        congestion_arg,
        jitter_arg,
        duration_sec,
        server_load_profile,
        server_degradation_threshold,
    ) in tasks
    {
        let bin = bin.clone();
        let sem = Arc::clone(&semaphore);
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            let _permit = sem.acquire();
            let mut cmd = Command::new(&bin);
            cmd.arg("--scenario")
                .arg(&scenario_name)
                .arg("--out-dir")
                .arg(&scenario_dir)
                .arg("--bandwidth")
                .arg(&bandwidth_arg)
                .arg("--latency")
                .arg(&latency_arg)
                .arg("--congestion")
                .arg(&congestion_arg)
                .arg("--server-load-profile")
                .arg(&server_load_profile);
            if let Some(threshold) = server_degradation_threshold {
                cmd.arg("--server-connection-degradation-threshold").arg(threshold.to_string());
            }
            if let Some(ref j) = jitter_arg {
                cmd.arg("--jitter").arg(j);
            }
            if let Some(d) = duration_sec {
                cmd.arg("--duration-sec").arg(d.to_string());
            }
            let status = cmd.status();
            let _ = tx.send((label, status));
        }));
    }
    drop(tx);

    let total = handles.len();
    let mut completed = 0usize;
    let mut succeeded = 0usize;
    let mut failed_labels = Vec::new();
    for (label, status) in rx {
        completed += 1;
        let msg = match &status {
            Ok(s) if s.success() => {
                succeeded += 1;
                format!("OK ({}/{}): {}", completed, total, label)
            },
            Ok(s) => {
                failed_labels.push(label.clone());
                format!("FAIL ({}/{}): {} (exit code {:?})", completed, total, label, s.code())
            },
            Err(e) => {
                failed_labels.push(label.clone());
                format!("FAIL ({}/{}): {} (error: {})", completed, total, label, e)
            },
        };
        eprintln!("{}", msg);
    }

    for h in handles {
        h.join().map_err(|_| "scenario thread panicked")?;
    }

    generate_summary_csv(&results_base)?;
    let elapsed = start_time.elapsed();
    eprintln!("----------------------------------------");
    eprintln!("=== Summary ===");
    eprintln!("Total: {} | Succeeded: {} | Failed: {}", total, succeeded, total - succeeded);
    eprintln!("Elapsed: {:.1}s", elapsed.as_secs_f64());
    eprintln!("Results: {}", results_base.display());
    eprintln!("Summary CSV: {}", results_base.join("summary.csv").display());
    if !failed_labels.is_empty() {
        eprintln!("Failed runs: {}", failed_labels.join(", "));
    }
    Ok(())
}
