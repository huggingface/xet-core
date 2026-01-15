#![cfg(not(target_family = "wasm"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::AdaptiveConcurrencyController;
use cas_client::http_client::build_http_client;
use cas_client::retry_wrapper::RetryWrapper;
use cas_client::upload_progress_stream::UploadProgressStream;
use clap::Parser;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use rand::Rng;
use reqwest::Body;
use reqwest_middleware::ClientWithMiddleware;
use tokio::task::JoinSet;
use xet_runtime::xet_config;

mod common;
use common::ClientMetrics;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "65536")]
    min_data_kb: u64,

    #[arg(long, default_value = "65536")]
    max_data_kb: u64,

    #[arg(long, default_value = "120")]
    repeat_duration_seconds: u64,

    #[arg(long, default_value = "127.0.0.1:8080")]
    server_addr: String,
}

// Use full realistic constants for testing (no scaling)
// test_set_globals! {
//     CLIENT_RETRY_BASE_DELAY = Duration::from_millis(300); // 3000ms -> 300ms
//     CLIENT_RETRY_MAX_DURATION = Duration::from_secs(36); // 6min -> 36sec
//     CONCURRENCY_CONTROL_MAX_WITHIN_TARGET_TRANSFER_TIME_MS = 4500; // 45sec -> 4.5sec
//     CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS = 50; // 500ms -> 50ms
//     CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS = 25; // 250ms -> 25ms
//     CONCURRENCY_CONTROL_LATENCY_TRACKING_HALF_LIFE_MS = 3000; // 30sec -> 3sec
//     CONCURRENCY_CONTROL_SUCCESS_TRACKING_HALF_LIFE_MS = 1000; // 10sec -> 1sec
//     CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS = 1000; // 10sec -> 1sec
//     CLIENT_IDLE_CONNECTION_TIMEOUT = Duration::from_secs(6); // 60sec -> 6sec
// }

/// Wait for the server to be ready by attempting to connect to it.
/// Errors out if server is not ready within 30 seconds.
async fn wait_for_server_ready(
    http_client: &ClientWithMiddleware,
    server_addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("http://{}/data", server_addr);
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    let check_interval = Duration::from_millis(500);

    eprintln!("Waiting for server at {} to be ready...", server_addr);

    loop {
        match http_client.post(&url).body(vec![]).timeout(Duration::from_secs(1)).send().await {
            Ok(_) => {
                eprintln!("Server is ready");
                return Ok(());
            },
            Err(e) => {
                // Check if it's a connection error (server not ready)
                let is_connect_error = match &e {
                    reqwest_middleware::Error::Reqwest(reqwest_err) => {
                        reqwest_err.is_connect() || reqwest_err.is_timeout()
                    },
                    _ => false,
                };

                if is_connect_error {
                    if start.elapsed() >= timeout {
                        return Err(format!("Server at {} did not become ready within 30 seconds", server_addr).into());
                    }
                    tokio::time::sleep(check_interval).await;
                } else {
                    // Other error means server is up (just returning an error response)
                    eprintln!("Server is ready");
                    return Ok(());
                }
            },
        }
    }
}

async fn run_client(min_data_kb: u64, max_data_kb: u64, repeat_duration_seconds: u64, server_addr: &str) {
    // Convert KB to bytes
    let min_data_size = min_data_kb * 1024;
    let max_data_size = max_data_kb * 1024;
    let client_id = rand::rng().random_range(0..1000000000) as u64;

    eprintln!("Connecting to server at {}", server_addr);

    // Create HTTP client
    let http_client = build_http_client("test_session", "test_user_agent").expect("Failed to create HTTP client");

    // Wait for server to be ready before starting
    wait_for_server_ready(&http_client, server_addr)
        .await
        .expect("Failed to connect to server within 30 seconds");

    // Log client parameters to JSON file (after server is ready)
    let client_params = serde_json::json!({
        "client_id": client_id,
        "min_data_kb": min_data_kb,
        "max_data_kb": max_data_kb,
        "min_data_size_bytes": min_data_size,
        "max_data_size_bytes": max_data_size,
        "repeat_duration_seconds": repeat_duration_seconds,
        "server_addr": server_addr
    });
    let params_filename = format!("client_parameters_{}.json", client_id);
    let params_json = serde_json::to_string_pretty(&client_params).unwrap();
    std::fs::write(&params_filename, params_json).expect("Failed to write client parameters");

    eprintln!(
        "Starting to send payloads (size: {} - {} KB, {} - {} bytes) using AdaptiveConcurrencyController for {} seconds",
        min_data_kb, max_data_kb, min_data_size, max_data_size, repeat_duration_seconds
    );

    // Create the adaptive concurrency controller
    let concurrency_controller = AdaptiveConcurrencyController::new_upload("test_uploads");

    let start_time_loop = Instant::now();
    let end_duration = Duration::from_secs(repeat_duration_seconds);

    // Atomic counters for thread-safe counting
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

    // Spawn a separate task to write statistics every 200ms
    let stats_concurrency_controller = concurrency_controller.clone();
    let stats_bytes_sent = bytes_sent.clone();
    let stats_total_retry_wrapper_calls = total_retry_wrapper_calls.clone();
    let stats_total_http_calls = total_http_calls.clone();
    let stats_total_round_trip_time_ms = total_round_trip_time_ms.clone();
    let stats_total_successful_transmissions = total_successful_transmissions.clone();
    let stats_start_time_loop = start_time_loop;
    let stats_end_duration = end_duration;
    let stats_client_id = client_id;

    tokio::spawn(async move {
        let mut last_reported_bytes = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Check if test duration is done
            if stats_start_time_loop.elapsed() >= stats_end_duration {
                break;
            }

            let current_bytes = stats_bytes_sent.load(Ordering::Relaxed);
            let interval_bytes = current_bytes.saturating_sub(last_reported_bytes);
            let interval_sec = 0.2; // 200ms
            let interval_throughput_bps = if interval_sec > 0.0 {
                interval_bytes as f64 / interval_sec
            } else {
                0.0
            };
            let total_throughput_bps = if stats_start_time_loop.elapsed().as_secs_f64() > 0.0 {
                current_bytes as f64 / stats_start_time_loop.elapsed().as_secs_f64()
            } else {
                0.0
            };

            let retry_wrapper_calls = stats_total_retry_wrapper_calls.load(Ordering::Relaxed);
            let http_calls = stats_total_http_calls.load(Ordering::Relaxed);
            let total_retries_count = http_calls.saturating_sub(retry_wrapper_calls);
            let total_rtt_ms = stats_total_round_trip_time_ms.load(Ordering::Relaxed);
            let average_rtt_ms = if retry_wrapper_calls > 0 {
                total_rtt_ms as f64 / retry_wrapper_calls as f64
            } else {
                0.0
            };
            let successful_transmissions = stats_total_successful_transmissions.load(Ordering::Relaxed);

            // Get concurrency controller state for reporting
            let success_state = stats_concurrency_controller.success_model_state().await;
            let latency_state = stats_concurrency_controller.latency_model_state().await;

            let client_metrics = ClientMetrics {
                client_id: stats_client_id,
                server_tag: "upload-server".to_string(),
                timestamp: chrono::Utc::now().timestamp_millis().to_string(),
                elapsed_seconds: stats_start_time_loop.elapsed().as_secs_f64(),
                interval_bytes,
                total_bytes: current_bytes,
                interval_sec,
                interval_throughput_bps,
                total_throughput_bps,
                average_round_trip_time_ms: average_rtt_ms,
                total_server_calls: retry_wrapper_calls,
                total_retries: total_retries_count,
                total_successful_transmissions: successful_transmissions,
                current_max_concurrency: stats_concurrency_controller.total_permits(),
                current_active_connections: stats_concurrency_controller.active_permits(),
                concurrency_controller_stats: Some(success_state.clone()),
                latency_model_stats: Some(latency_state.clone()),
            };

            // Append metrics to file
            let stats_filename = format!("client_stats_{}.json", stats_client_id);
            let metrics_json = serde_json::to_string(&client_metrics).unwrap();
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stats_filename)
                .expect("Failed to open client stats file");
            use std::io::Write;
            writeln!(file, "{}", metrics_json).expect("Failed to write client stats");
            file.flush().expect("Failed to flush client stats file");

            last_reported_bytes = current_bytes;
        }
    });

    // Spawn 200 concurrent tasks
    let mut join_set = JoinSet::new();
    for task_id in 0..100 {
        let concurrency_controller = concurrency_controller.clone();
        let http_client = http_client.clone();
        let server_addr = server_addr.to_string();
        let bytes_sent = bytes_sent.clone();
        let total_retry_wrapper_calls = total_retry_wrapper_calls.clone();
        let total_http_calls = total_http_calls.clone();
        let total_round_trip_time_ms = total_round_trip_time_ms.clone();
        let total_successful_transmissions = total_successful_transmissions.clone();
        let base_data = base_data.clone();

        join_set.spawn(async move {
            let mut has_success = false;
            let mut request_count = 0u64;
            loop {
                // Check if we should exit before acquiring a permit
                let elapsed = start_time_loop.elapsed();
                if elapsed >= end_duration {
                    eprintln!(
                        "{task_id}: Duration expired (elapsed={:.2}s >= {:.2}s), exiting after {} requests",
                        elapsed.as_secs_f64(),
                        end_duration.as_secs_f64(),
                        request_count
                    );
                    break;
                }

                // Acquire a connection permit
                let permit = match concurrency_controller.acquire_connection_permit().await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("{task_id}: Failed to acquire connection permit: {}", e);
                        // Continue trying instead of breaking
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    },
                };

                if !has_success {
                    eprintln!("{task_id}: Acquired connection permit");
                }

                request_count += 1;

                // Randomly choose a payload size between min and max
                let payload_size = if min_data_size == max_data_size {
                    min_data_size
                } else {
                    rand::rng().random_range(min_data_size..=max_data_size)
                };

                // Create payload data to upload
                let payload_data = base_data.slice(0..payload_size as usize);

                // Use RetryWrapper to upload data with adaptive concurrency
                let api_tag = "upload_benchmark";
                let request_start = Instant::now();
                total_retry_wrapper_calls.fetch_add(1, Ordering::Relaxed);

                let result: Result<serde_json::Value, _> = RetryWrapper::new(api_tag)
                    .with_connection_permit(permit, Some(payload_size as u64))
                    .run_and_extract_json({
                        let http_client = http_client.clone();
                        let server_addr = server_addr.clone();
                        let payload_data = payload_data.clone();
                        let total_http_calls = total_http_calls.clone();
                        move |partial_report_fn| {
                            let http_client = http_client.clone();
                            let server_addr = server_addr.clone();
                            let payload_data = payload_data.clone();
                            let total_http_calls = total_http_calls.clone();
                            let partial_report_fn = partial_report_fn.clone();
                            let total_size = payload_size as u64;

                            let progress_callback = move |_delta: u64, total_bytes: u64| {
                                // Report partial progress to concurrency controller
                                if let Some(ref partial_report_fn) = partial_report_fn
                                    && total_size > 0
                                {
                                    let portion = (total_bytes as f64 / total_size as f64).min(1.0);
                                    partial_report_fn(portion, total_bytes);
                                }
                            };

                            let upload_stream = UploadProgressStream::new_with_progress_callback(
                                payload_data.clone(),
                                xet_config().client.upload_reporting_block_size,
                                progress_callback,
                            )
                            .clone_with_reset();

                            async move {
                                // Send the upload request as HTTP POST
                                // Add explicit timeout to prevent hanging connections
                                // Use 60 seconds as a reasonable timeout for uploads
                                let url = format!("http://{}/data", server_addr);
                                total_http_calls.fetch_add(1, Ordering::Relaxed);

                                if !has_success {
                                    eprintln!("{task_id}: About to call server.");
                                }
                                let start_time = Instant::now();

                                let res = http_client
                                    .post(&url)
                                    .header(CONTENT_LENGTH, HeaderValue::from(payload_size))
                                    .body(Body::wrap_stream(upload_stream))
                                    .send()
                                    .await;

                                if !has_success {
                                    eprintln!(
                                        "{task_id}: Server response: {res:?}: time = {:?}ms",
                                        start_time.elapsed().as_millis()
                                    );
                                }

                                res
                            }
                        }
                    })
                    .await
                    .inspect_err(|e| eprintln!("Error uploading data: {}", e));

                let request_duration = request_start.elapsed();
                total_round_trip_time_ms.fetch_add(request_duration.as_millis() as u64, Ordering::Relaxed);

                match result {
                    Ok(_response) => {
                        // Track successful transmission
                        total_successful_transmissions.fetch_add(1, Ordering::Relaxed);

                        // Track uploaded bytes
                        bytes_sent.fetch_add(payload_size, Ordering::Relaxed);

                        if total_successful_transmissions.load(Ordering::Relaxed) > 0 {
                            has_success = true;
                        }
                    },
                    Err(e) => {
                        eprintln!("{task_id}: Upload failed after {}ms: {}", request_duration.as_millis(), e);
                        // Continue the loop instead of breaking - we want to keep trying until duration expires
                        // Only check if we should exit due to time
                        if start_time_loop.elapsed() >= end_duration {
                            eprintln!("{task_id}: Exiting due to duration expiry");
                            break;
                        }
                        continue;
                    },
                }
            }
        });
    }

    // Wait for the full duration, not just for tasks to complete
    // This ensures the stats task continues running for the full test duration
    eprintln!("Main: Waiting for full duration of {:.2}s...", end_duration.as_secs_f64());

    // Wait for the full duration - tasks will continue running/retrying in the background
    tokio::time::sleep(end_duration).await;

    eprintln!(
        "Main: Full duration elapsed ({:.2}s), exiting. Stats task should have already exited.",
        start_time_loop.elapsed().as_secs_f64()
    );

    // Note: We don't wait for join_set here because:
    // 1. Tasks should continue retrying until duration expires (they check elapsed time)
    // 2. The stats task is a separate spawned task that will exit when duration expires
    // 3. When main exits, the runtime will clean up remaining tasks
}

fn init_logging() {
    // Initialize tracing subscriber to log to console at debug level
    // This bridges the log crate (used by warp) to tracing
    // LogTracer::init() returns a Result, so we can safely ignore errors if already initialized
    if tracing_log::LogTracer::init().is_err() {
        // Log tracer already initialized, that's fine - another part of the code may have set it up
    }

    // Set up console logging with debug level by default
    // Include warp=debug to capture warp's internal logging (if used by client)
    // Use try_init instead of init to avoid panicking if subscriber is already set
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(false)
        .try_init()
        .is_err()
    {
        // Subscriber already initialized, that's fine - another part of the code may have set it up
        // This can happen if tracing-test or another test framework has already initialized it
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let args = Args::parse();

    // Validate that min_data_kb <= max_data_kb
    if args.min_data_kb > args.max_data_kb {
        eprintln!("Error: min_data_kb ({}) must be <= max_data_kb ({})", args.min_data_kb, args.max_data_kb);
        std::process::exit(1);
    }

    eprintln!(
        "Starting client with data_size={}-{} KB, repeat_duration={}s, server_addr={}",
        args.min_data_kb, args.max_data_kb, args.repeat_duration_seconds, args.server_addr
    );

    run_client(args.min_data_kb, args.max_data_kb, args.repeat_duration_seconds, &args.server_addr).await;

    Ok(())
}
