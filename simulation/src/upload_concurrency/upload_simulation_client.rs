//! Upload simulation client: runs one or more upload clients against a server (proxy + LocalTestServer dummy_upload).

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::{AdaptiveConcurrencyController, CCLatencyModelState, CCSuccessModelState};
use cas_client::http_client::build_http_client;
use cas_client::progress_tracked_streams::UploadProgressStream;
use cas_client::retry_wrapper::RetryWrapper;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use rand::Rng;
use reqwest::Body;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use xet_runtime::xet_config;

use crate::scenario::base_url;

/// Metrics for a single client, written every 200ms to client_stats_<id>.json (JSON lines).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMetrics {
    pub client_id: u64,
    pub server_tag: String,
    pub timestamp: String,
    pub elapsed_seconds: f64,
    pub interval_bytes: u64,
    pub total_bytes: u64,
    pub interval_sec: f64,
    pub interval_throughput_bps: f64,
    pub total_throughput_bps: f64,
    pub average_round_trip_time_ms: f64,
    pub total_server_calls: u64,
    pub total_retries: u64,
    pub total_successful_transmissions: u64,
    pub current_max_concurrency: usize,
    pub current_active_connections: usize,
    pub concurrency_controller_stats: Option<CCSuccessModelState>,
    pub latency_model_stats: Option<CCLatencyModelState>,
}

const DUMMY_UPLOAD_PATH: &str = "/simulation/dummy_upload";
const STATS_INTERVAL_MS: u64 = 200;
const UPLOAD_TASKS: usize = 100;

/// Shared counters for tracking upload progress across tasks.
#[derive(Clone)]
struct UploadCounters {
    bytes_sent: Arc<AtomicU64>,
    retry_wrapper_calls: Arc<AtomicU64>,
    http_calls: Arc<AtomicU64>,
    round_trip_time_ms: Arc<AtomicU64>,
    successful_transmissions: Arc<AtomicU64>,
}

impl UploadCounters {
    fn new() -> Self {
        Self {
            bytes_sent: Arc::new(AtomicU64::new(0)),
            retry_wrapper_calls: Arc::new(AtomicU64::new(0)),
            http_calls: Arc::new(AtomicU64::new(0)),
            round_trip_time_ms: Arc::new(AtomicU64::new(0)),
            successful_transmissions: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Runs upload clients until the given cancellation token is triggered, writing client_parameters_<id>.json and
/// client_stats_<id>.json (JSON lines) into `output_dir`.
pub async fn run_upload_clients_until_cancelled(
    server_addr: &str,
    output_dir: &Path,
    min_data_kb: u64,
    max_data_kb: u64,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_upload_clients_impl(server_addr, output_dir, min_data_kb, max_data_kb, None, Some(cancel)).await
}

/// Runs upload clients for the given duration, writing client_parameters_<id>.json and
/// client_stats_<id>.json (JSON lines) into `output_dir`.
pub async fn run_upload_clients(
    server_addr: &str,
    output_dir: &Path,
    min_data_kb: u64,
    max_data_kb: u64,
    repeat_duration_seconds: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_upload_clients_impl(server_addr, output_dir, min_data_kb, max_data_kb, Some(repeat_duration_seconds), None)
        .await
}

/// Spawns a background task that periodically writes `ClientMetrics` to a JSON lines file.
fn spawn_stats_reporter(
    client_id: u64,
    output_dir: &Path,
    counters: UploadCounters,
    concurrency_controller: Arc<AdaptiveConcurrencyController>,
    start_instant: Arc<Mutex<Instant>>,
    end_duration: Duration,
    cancel: Option<CancellationToken>,
) {
    let stats_output_dir = output_dir.to_path_buf();
    tokio::spawn(async move {
        let mut last_reported_bytes = 0u64;
        let interval = Duration::from_millis(STATS_INTERVAL_MS);
        loop {
            let elapsed = start_instant.lock().unwrap().elapsed();
            let done = if let Some(ref c) = cancel {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => elapsed >= end_duration,
                    _ = c.cancelled() => true,
                }
            } else {
                tokio::time::sleep(interval).await;
                elapsed >= end_duration
            };
            if done {
                break;
            }
            let current_bytes = counters.bytes_sent.load(Ordering::Relaxed);
            let interval_bytes = current_bytes.saturating_sub(last_reported_bytes);
            let interval_sec = STATS_INTERVAL_MS as f64 / 1000.0;
            let interval_throughput_bps = if interval_sec > 0.0 {
                interval_bytes as f64 / interval_sec
            } else {
                0.0
            };
            let elapsed_sec = start_instant.lock().unwrap().elapsed().as_secs_f64();
            let total_throughput_bps = if elapsed_sec > 0.0 {
                current_bytes as f64 / elapsed_sec
            } else {
                0.0
            };
            let retry_wrapper_calls = counters.retry_wrapper_calls.load(Ordering::Relaxed);
            let http_calls = counters.http_calls.load(Ordering::Relaxed);
            let total_retries = http_calls.saturating_sub(retry_wrapper_calls);
            let total_rtt_ms = counters.round_trip_time_ms.load(Ordering::Relaxed);
            let average_rtt_ms = if retry_wrapper_calls > 0 {
                total_rtt_ms as f64 / retry_wrapper_calls as f64
            } else {
                0.0
            };
            let successful_transmissions = counters.successful_transmissions.load(Ordering::Relaxed);
            let success_state = concurrency_controller.success_model_state().await;
            let latency_state = concurrency_controller.latency_model_state().await;

            let metrics = ClientMetrics {
                client_id,
                server_tag: "upload-server".to_string(),
                timestamp: chrono::Utc::now().timestamp_millis().to_string(),
                elapsed_seconds: start_instant.lock().unwrap().elapsed().as_secs_f64(),
                interval_bytes,
                total_bytes: current_bytes,
                interval_sec,
                interval_throughput_bps,
                total_throughput_bps,
                average_round_trip_time_ms: average_rtt_ms,
                total_server_calls: retry_wrapper_calls,
                total_retries,
                total_successful_transmissions: successful_transmissions,
                current_max_concurrency: concurrency_controller.total_permits(),
                current_active_connections: concurrency_controller.active_permits(),
                concurrency_controller_stats: Some(success_state),
                latency_model_stats: Some(latency_state),
            };

            let stats_path = stats_output_dir.join(format!("client_stats_{}.json", client_id));
            let line = serde_json::to_string(&metrics).unwrap();
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stats_path)
                .expect("open client_stats");
            writeln!(f, "{}", line).unwrap();
            f.flush().unwrap();
            last_reported_bytes = current_bytes;
        }
    });
}

/// Shared context for upload workers.
struct UploadContext {
    url: String,
    http_client: reqwest_middleware::ClientWithMiddleware,
    base_data: Bytes,
    min_data_size: u64,
    max_data_size: u64,
    counters: UploadCounters,
    concurrency_controller: Arc<AdaptiveConcurrencyController>,
    start_instant: Arc<Mutex<Instant>>,
    end_duration: Duration,
    cancel: Option<CancellationToken>,
}

/// Spawns `UPLOAD_TASKS` concurrent upload workers into the given `JoinSet`.
fn spawn_upload_tasks(join_set: &mut JoinSet<()>, ctx: &UploadContext) {
    for _ in 0..UPLOAD_TASKS {
        let concurrency_controller = ctx.concurrency_controller.clone();
        let http_client = ctx.http_client.clone();
        let url = ctx.url.clone();
        let counters = ctx.counters.clone();
        let base_data = ctx.base_data.clone();
        let task_cancel = ctx.cancel.clone();
        let task_start = Arc::clone(&ctx.start_instant);
        let end_duration = ctx.end_duration;
        let min_data_size = ctx.min_data_size;
        let max_data_size = ctx.max_data_size;

        join_set.spawn(async move {
            loop {
                let elapsed = task_start.lock().unwrap().elapsed();
                if elapsed >= end_duration {
                    break;
                }
                if let Some(ref c) = task_cancel
                    && c.is_cancelled()
                {
                    break;
                }
                let permit = match concurrency_controller.acquire_connection_permit().await {
                    Ok(p) => p,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    },
                };

                let payload_size = if min_data_size == max_data_size {
                    min_data_size
                } else {
                    rand::rng().random_range(min_data_size..=max_data_size)
                };
                let payload_data = base_data.slice(0..payload_size as usize);

                let do_one_upload = async {
                    counters.retry_wrapper_calls.fetch_add(1, Ordering::Relaxed);
                    let request_start = Instant::now();
                    let result = RetryWrapper::new("upload_benchmark")
                        .with_connection_permit(permit, Some(payload_size))
                        .run({
                            let http_client = http_client.clone();
                            let url = url.clone();
                            let payload_data = payload_data.clone();
                            let http_calls = counters.http_calls.clone();
                            let block_size = xet_config().client.upload_reporting_block_size;
                            move || {
                                let http_client = http_client.clone();
                                let url = url.clone();
                                let http_calls = http_calls.clone();
                                let upload_stream = UploadProgressStream::new(payload_data.clone(), block_size);
                                async move {
                                    http_calls.fetch_add(1, Ordering::Relaxed);
                                    http_client
                                        .post(&url)
                                        .header(CONTENT_LENGTH, HeaderValue::from(payload_size))
                                        .body(Body::wrap_stream(upload_stream))
                                        .send()
                                        .await
                                }
                            }
                        })
                        .await;
                    (result, request_start)
                };

                let (result, request_start) = if let Some(ref c) = task_cancel {
                    tokio::select! {
                        r = do_one_upload => r,
                        _ = c.cancelled() => break,
                    }
                } else {
                    do_one_upload.await
                };

                counters
                    .round_trip_time_ms
                    .fetch_add(request_start.elapsed().as_millis() as u64, Ordering::Relaxed);

                let success = result.as_ref().map(|r| r.status().is_success()).unwrap_or(false);
                if success {
                    counters.successful_transmissions.fetch_add(1, Ordering::Relaxed);
                    counters.bytes_sent.fetch_add(payload_size, Ordering::Relaxed);
                } else if task_start.lock().unwrap().elapsed() >= end_duration {
                    break;
                }
            }
        });
    }
}

async fn run_upload_clients_impl(
    server_addr: &str,
    output_dir: &Path,
    min_data_kb: u64,
    max_data_kb: u64,
    repeat_duration_seconds: Option<u64>,
    cancel: Option<CancellationToken>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let min_data_size = min_data_kb * 1024;
    let max_data_size = max_data_kb * 1024;
    let client_id = rand::rng().random_range(0..1000000000_u64);

    let http_client = build_http_client("test_session", None, None).map_err(|e| e.to_string())?;

    let duration_sec = repeat_duration_seconds.unwrap_or(u64::MAX);
    let client_params = serde_json::json!({
        "client_id": client_id,
        "min_data_kb": min_data_kb,
        "max_data_kb": max_data_kb,
        "min_data_size_bytes": min_data_size,
        "max_data_size_bytes": max_data_size,
        "repeat_duration_seconds": duration_sec,
        "server_addr": server_addr
    });
    let params_path = output_dir.join(format!("client_parameters_{}.json", client_id));
    std::fs::write(&params_path, serde_json::to_string_pretty(&client_params)?)?;

    let concurrency_controller = AdaptiveConcurrencyController::new_upload("test_uploads");
    let start_instant = Arc::new(Mutex::new(Instant::now()));
    let end_duration = Duration::from_secs(duration_sec);
    let counters = UploadCounters::new();

    let base_data = Bytes::from({
        let mut rng = rand::rng();
        let mut data = vec![0u8; max_data_size as usize];
        rng.fill(&mut data[..]);
        data
    });

    spawn_stats_reporter(
        client_id,
        output_dir,
        counters.clone(),
        concurrency_controller.clone(),
        Arc::clone(&start_instant),
        end_duration,
        cancel.clone(),
    );

    let url_base = base_url(server_addr);
    let url = format!("{}{}", url_base.trim_end_matches('/'), DUMMY_UPLOAD_PATH);

    let upload_ctx = UploadContext {
        url,
        http_client,
        base_data,
        min_data_size,
        max_data_size,
        counters,
        concurrency_controller,
        start_instant,
        end_duration,
        cancel: cancel.clone(),
    };

    let mut join_set = JoinSet::new();
    spawn_upload_tasks(&mut join_set, &upload_ctx);

    if let Some(c) = cancel {
        tokio::select! {
            _ = tokio::time::sleep(end_duration) => {}
            _ = c.cancelled() => {}
        }
    } else {
        tokio::time::sleep(end_duration).await;
    }
    while join_set.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cas_client::simulation::local_server::ServerLatencyProfile;

    use super::run_upload_clients_until_cancelled;
    use crate::scenario::{ScenarioError, SimulationScenarioBuilder};

    /// Runs a full scenario: start server, add upload client, run 3s, shutdown, then verify
    /// output directory has network_stats.json, timeline.csv, client_parameters_*.json, client_stats_*.json.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scenario_run_3_seconds_shutdown_verify_output() {
        let temp = tempfile::tempdir().unwrap();
        let out_dir = temp.path().to_path_buf();
        let latency_profile = ServerLatencyProfile::from_name("none").unwrap();
        let mut scenario = SimulationScenarioBuilder::new()
            .with_out_dir(&out_dir)
            .with_latency_profile(latency_profile)
            .with_server_latency_profile_name("none")
            .with_bandwidth_str("1gbps")
            .unwrap()
            .with_latency(Duration::from_millis(20), Duration::ZERO)
            .with_congestion("none")
            .unwrap()
            .start()
            .await
            .unwrap();
        let addr = scenario.server().http_endpoint().to_string();
        let out_dir_clone = out_dir.clone();
        const TEST_MIN_KB: u64 = 1;
        const TEST_MAX_KB: u64 = 2;
        scenario.add_task(move |token| {
            tokio::spawn(async move {
                run_upload_clients_until_cancelled(&addr, &out_dir_clone, TEST_MIN_KB, TEST_MAX_KB, token)
                    .await
                    .map_err(|e| ScenarioError::Client(e.to_string()))
            })
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        let result = scenario.finish().await;
        assert!(result.is_ok(), "finish() should succeed: {:?}", result.err());

        assert!(out_dir.join("network_stats.json").exists(), "network_stats.json should exist");
        let network_stats_content = std::fs::read_to_string(out_dir.join("network_stats.json")).unwrap();
        assert!(!network_stats_content.trim().is_empty(), "network_stats.json should not be empty");
        let network_stats_lines = network_stats_content.lines().filter(|l| !l.trim().is_empty()).count();
        assert!(network_stats_lines >= 1, "network_stats.json should have at least one sample");

        assert!(out_dir.join("timeline.csv").exists(), "timeline.csv should exist");
        let timeline_content = std::fs::read_to_string(out_dir.join("timeline.csv")).unwrap();
        assert!(!timeline_content.trim().is_empty(), "timeline.csv should not be empty");

        let mut client_params = 0;
        let mut client_stats = 0;
        for entry in std::fs::read_dir(&out_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.starts_with("client_parameters_") && name.ends_with(".json") {
                client_params += 1;
            }
            if name.starts_with("client_stats_") && name.ends_with(".json") {
                client_stats += 1;
            }
        }
        assert!(client_params >= 1, "at least one client_parameters_*.json should exist");
        assert!(client_stats >= 1, "at least one client_stats_*.json should exist");
    }
}
