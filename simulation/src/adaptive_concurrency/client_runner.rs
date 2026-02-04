//! Runs one or more upload clients against a server (proxy + LocalTestServer dummy_upload).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::AdaptiveConcurrencyController;
use cas_client::http_client::build_http_client;
use cas_client::retry_wrapper::RetryWrapper;
use cas_client::upload_progress_stream::UploadProgressStream;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use rand::Rng;
use reqwest::Body;
use reqwest_middleware::ClientWithMiddleware;
use tokio::task::JoinSet;
use xet_runtime::xet_config;

use super::common::ClientMetrics;

const DUMMY_UPLOAD_PATH: &str = "/simulation/dummy_upload";
const STATS_INTERVAL_MS: u64 = 200;
const UPLOAD_TASKS: usize = 100;

/// Normalizes server address to a base URL (e.g. "127.0.0.1:8080" -> "http://127.0.0.1:8080").
fn base_url(server_addr: &str) -> String {
    let s = server_addr.trim();
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{}", s)
    }
}

/// Waits for the server to be ready by POSTing to dummy_upload. Errors if not ready within 30s.
pub async fn wait_for_server_ready(
    http_client: &ClientWithMiddleware,
    server_addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}{}", base_url(server_addr).trim_end_matches('/'), DUMMY_UPLOAD_PATH);
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    let check_interval = Duration::from_millis(500);

    loop {
        match http_client.post(&url).body(vec![]).timeout(Duration::from_secs(1)).send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let is_connect_error = match &e {
                    reqwest_middleware::Error::Reqwest(we) => we.is_connect() || we.is_timeout(),
                    _ => false,
                };
                if is_connect_error {
                    if start.elapsed() >= timeout {
                        return Err(format!("Server at {} not ready within 30s", server_addr).into());
                    }
                    tokio::time::sleep(check_interval).await;
                } else {
                    return Ok(());
                }
            },
        }
    }
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
    let min_data_size = min_data_kb * 1024;
    let max_data_size = max_data_kb * 1024;
    let client_id = rand::rng().random_range(0..1000000000_u64);

    let http_client = build_http_client("test_session", "test_user_agent", None).map_err(|e| e.to_string())?;

    wait_for_server_ready(&http_client, server_addr).await?;

    let client_params = serde_json::json!({
        "client_id": client_id,
        "min_data_kb": min_data_kb,
        "max_data_kb": max_data_kb,
        "min_data_size_bytes": min_data_size,
        "max_data_size_bytes": max_data_size,
        "repeat_duration_seconds": repeat_duration_seconds,
        "server_addr": server_addr
    });
    let params_path = output_dir.join(format!("client_parameters_{}.json", client_id));
    std::fs::write(&params_path, serde_json::to_string_pretty(&client_params)?)?;

    let concurrency_controller = AdaptiveConcurrencyController::new_upload("test_uploads");
    let start_time_loop = Instant::now();
    let end_duration = Duration::from_secs(repeat_duration_seconds);

    let bytes_sent = Arc::new(AtomicU64::new(0));
    let total_retry_wrapper_calls = Arc::new(AtomicU64::new(0));
    let total_http_calls = Arc::new(AtomicU64::new(0));
    let total_round_trip_time_ms = Arc::new(AtomicU64::new(0));
    let total_successful_transmissions = Arc::new(AtomicU64::new(0));

    let base_data = Bytes::from({
        let mut rng = rand::rng();
        let mut data = vec![0u8; max_data_size as usize];
        rng.fill(&mut data[..]);
        data
    });

    let stats_output_dir = output_dir.to_path_buf();
    let stats_concurrency_controller = concurrency_controller.clone();
    let stats_bytes_sent = Arc::clone(&bytes_sent);
    let stats_total_retry_wrapper_calls = Arc::clone(&total_retry_wrapper_calls);
    let stats_total_http_calls = Arc::clone(&total_http_calls);
    let stats_total_round_trip_time_ms = Arc::clone(&total_round_trip_time_ms);
    let stats_total_successful_transmissions = Arc::clone(&total_successful_transmissions);

    tokio::spawn(async move {
        let mut last_reported_bytes = 0u64;
        let interval = Duration::from_millis(STATS_INTERVAL_MS);
        loop {
            tokio::time::sleep(interval).await;
            if start_time_loop.elapsed() >= end_duration {
                break;
            }
            let current_bytes = stats_bytes_sent.load(Ordering::Relaxed);
            let interval_bytes = current_bytes.saturating_sub(last_reported_bytes);
            let interval_sec = STATS_INTERVAL_MS as f64 / 1000.0;
            let interval_throughput_bps = if interval_sec > 0.0 {
                interval_bytes as f64 / interval_sec
            } else {
                0.0
            };
            let total_throughput_bps = if start_time_loop.elapsed().as_secs_f64() > 0.0 {
                current_bytes as f64 / start_time_loop.elapsed().as_secs_f64()
            } else {
                0.0
            };
            let retry_wrapper_calls = stats_total_retry_wrapper_calls.load(Ordering::Relaxed);
            let http_calls = stats_total_http_calls.load(Ordering::Relaxed);
            let total_retries = http_calls.saturating_sub(retry_wrapper_calls);
            let total_rtt_ms = stats_total_round_trip_time_ms.load(Ordering::Relaxed);
            let average_rtt_ms = if retry_wrapper_calls > 0 {
                total_rtt_ms as f64 / retry_wrapper_calls as f64
            } else {
                0.0
            };
            let successful_transmissions = stats_total_successful_transmissions.load(Ordering::Relaxed);
            let success_state = stats_concurrency_controller.success_model_state().await;
            let latency_state = stats_concurrency_controller.latency_model_state().await;

            let metrics = ClientMetrics {
                client_id,
                server_tag: "upload-server".to_string(),
                timestamp: chrono::Utc::now().timestamp_millis().to_string(),
                elapsed_seconds: start_time_loop.elapsed().as_secs_f64(),
                interval_bytes,
                total_bytes: current_bytes,
                interval_sec,
                interval_throughput_bps,
                total_throughput_bps,
                average_round_trip_time_ms: average_rtt_ms,
                total_server_calls: retry_wrapper_calls,
                total_retries,
                total_successful_transmissions: successful_transmissions,
                current_max_concurrency: stats_concurrency_controller.total_permits(),
                current_active_connections: stats_concurrency_controller.active_permits(),
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
            use std::io::Write;
            writeln!(f, "{}", line).unwrap();
            f.flush().unwrap();
            last_reported_bytes = current_bytes;
        }
    });

    let url_base = base_url(server_addr);
    let url = format!("{}{}", url_base.trim_end_matches('/'), DUMMY_UPLOAD_PATH);

    let mut join_set = JoinSet::new();
    for _task_id in 0..UPLOAD_TASKS {
        let concurrency_controller = concurrency_controller.clone();
        let http_client = http_client.clone();
        let url = url.clone();
        let bytes_sent = Arc::clone(&bytes_sent);
        let total_retry_wrapper_calls = Arc::clone(&total_retry_wrapper_calls);
        let total_http_calls = Arc::clone(&total_http_calls);
        let total_round_trip_time_ms = Arc::clone(&total_round_trip_time_ms);
        let total_successful_transmissions = Arc::clone(&total_successful_transmissions);
        let base_data = base_data.clone();

        join_set.spawn(async move {
            loop {
                if start_time_loop.elapsed() >= end_duration {
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

                total_retry_wrapper_calls.fetch_add(1, Ordering::Relaxed);
                let request_start = Instant::now();

                let result = RetryWrapper::new("upload_benchmark")
                    .with_connection_permit(permit, Some(payload_size))
                    .run({
                        let http_client = http_client.clone();
                        let url = url.clone();
                        let payload_data = payload_data.clone();
                        let total_http_calls = total_http_calls.clone();
                        let total_size = payload_size;
                        move |partial_report_fn| {
                            let http_client = http_client.clone();
                            let url = url.clone();
                            let payload_data = payload_data.clone();
                            let total_http_calls = total_http_calls.clone();
                            let progress_callback = move |_delta: u64, total_bytes: u64| {
                                if let Some(ref f) = partial_report_fn
                                    && total_size > 0
                                {
                                    let portion = (total_bytes as f64 / total_size as f64).min(1.0);
                                    f(portion, total_bytes);
                                }
                            };
                            let upload_stream = UploadProgressStream::new_with_progress_callback(
                                payload_data,
                                xet_config().client.upload_reporting_block_size,
                                progress_callback,
                            )
                            .clone_with_reset();
                            async move {
                                total_http_calls.fetch_add(1, Ordering::Relaxed);
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

                total_round_trip_time_ms.fetch_add(request_start.elapsed().as_millis() as u64, Ordering::Relaxed);

                let success = result.as_ref().map(|r| r.status().is_success()).unwrap_or(false);
                if success {
                    total_successful_transmissions.fetch_add(1, Ordering::Relaxed);
                    bytes_sent.fetch_add(payload_size, Ordering::Relaxed);
                } else if start_time_loop.elapsed() >= end_duration {
                    break;
                }
            }
        });
    }

    tokio::time::sleep(end_duration).await;
    Ok(())
}
