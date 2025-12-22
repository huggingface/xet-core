#![cfg(not(target_family = "wasm"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use rand::Rng;
use warp::Filter;

mod common;
use common::{ServerMetrics, ServerSimulationParameters};

// Global statics for shared state
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static TOTAL_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static CONGESTION_BYTES_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0")]
    min_reply_delay_ms: u64,

    #[arg(long, default_value = "0")]
    max_reply_delay_ms: u64,

    #[arg(long, default_value = "0")]
    test_duration_seconds: u64,

    #[arg(long, default_value = "0")]
    congested_bytes_per_second: u64,

    #[arg(long, default_value = "0")]
    min_congested_penalty_ms: u64,

    #[arg(long, default_value = "0")]
    max_congested_penalty_ms: u64,

    #[arg(long, default_value = "0.0")]
    congested_error_rate: f64,
}

async fn handle_data_upload(
    body: warp::hyper::body::Bytes,
    params: Arc<ServerSimulationParameters>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let uploaded_size = body.len() as u64;

    // Drop the body to free up memory
    drop(body);

    // Check if test duration has elapsed
    if params.test_duration_seconds > 0 {
        let elapsed_seconds = START_TIME.get().unwrap().elapsed().as_secs();
        if elapsed_seconds >= params.test_duration_seconds {
            // Test duration has elapsed, return error
            let error_response = serde_json::json!({
                "status": "error",
                "error": "Test duration expired",
                "elapsed_seconds": elapsed_seconds,
                "test_duration_seconds": params.test_duration_seconds
            });
            return Ok(Box::new(warp::reply::with_status(
                warp::reply::with_header(error_response.to_string(), "Content-Type", "application/json"),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            )));
        }
    }

    // Simple connection counting
    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);

    TOTAL_BYTES.fetch_add(uploaded_size, Ordering::Relaxed);

    // Update congestion counter and check for congestion
    if params.congested_bytes_per_second > 0 {
        // Increment congestion counter by payload size
        CONGESTION_BYTES_COUNTER.fetch_add(uploaded_size, Ordering::Relaxed);

        // Check if we're over the congestion threshold
        let current_counter = CONGESTION_BYTES_COUNTER.load(Ordering::Relaxed);
        if current_counter > params.congested_bytes_per_second {
            // Decide whether to return error or apply delay penalty based on error rate
            // Clamp error rate to [0.0, 1.0]
            let error_rate = params.congested_error_rate.clamp(0.0, 1.0);
            let should_return_error = rand::rng().random_bool(error_rate);

            if should_return_error {
                // Return 504 Gateway Timeout with congestion information
                let error_response = serde_json::json!({
                    "status": "error",
                    "error": "Server congested",
                    "congested_bytes_per_second": params.congested_bytes_per_second,
                    "current_counter": current_counter
                });

                return Ok(Box::new(warp::reply::with_status(
                    warp::reply::with_header(error_response.to_string(), "Content-Type", "application/json"),
                    warp::http::StatusCode::GATEWAY_TIMEOUT,
                )));
            } else {
                // Apply congestion penalty delay
                if params.min_congested_penalty_ms > 0 || params.max_congested_penalty_ms > 0 {
                    let penalty_range = params.max_congested_penalty_ms - params.min_congested_penalty_ms;
                    let random_penalty = if penalty_range > 0 {
                        params.min_congested_penalty_ms + rand::rng().random_range(0..=penalty_range)
                    } else {
                        params.min_congested_penalty_ms
                    };

                    if random_penalty > 0 {
                        tokio::time::sleep(Duration::from_millis(random_penalty)).await;
                    }
                }
            }
        }
    }

    // Apply random delay if configured
    if params.min_reply_delay_ms > 0 || params.max_reply_delay_ms > 0 {
        let delay_range = params.max_reply_delay_ms - params.min_reply_delay_ms;
        let random_delay = if delay_range > 0 {
            params.min_reply_delay_ms + rand::rng().random_range(0..=delay_range)
        } else {
            params.min_reply_delay_ms
        };

        if random_delay > 0 {
            tokio::time::sleep(Duration::from_millis(random_delay)).await;
        }
    }

    // Return a simple JSON response indicating success
    let response = serde_json::json!({
        "status": "success",
        "uploaded_bytes": uploaded_size
    });

    Ok(Box::new(warp::reply::with_header(response.to_string(), "Content-Type", "application/json")))
}

async fn decrease_congestion_counter_periodically(congested_bytes_per_second: u64) {
    if congested_bytes_per_second == 0 {
        return;
    }

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let decrease_amount = congested_bytes_per_second / 10; // 1/10th of capacity every 100ms

    loop {
        interval.tick().await;

        // Decrease counter by 1/10th of capacity, minimum 0
        CONGESTION_BYTES_COUNTER
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(decrease_amount)))
            .ok();
    }
}

async fn report_metrics_periodically() {
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    let mut last_bytes = 0u64;
    let mut last_time = Instant::now();

    loop {
        interval.tick().await;

        let elapsed_seconds = START_TIME.get().unwrap().elapsed().as_secs_f64();
        let total_bytes = TOTAL_BYTES.load(Ordering::Relaxed);
        let total_connections = TOTAL_CONNECTIONS.load(Ordering::Relaxed);

        // Calculate interval metrics
        let interval_bytes = total_bytes - last_bytes;
        let interval_sec = last_time.elapsed().as_secs_f64();
        let current_throughput_bps = if interval_sec > 0.0 {
            interval_bytes as f64 / interval_sec
        } else {
            0.0
        };

        let average_throughput_bps = if elapsed_seconds > 0.0 {
            total_bytes as f64 / elapsed_seconds
        } else {
            0.0
        };

        let average_connections_per_second = if elapsed_seconds > 0.0 {
            total_connections as f64 / elapsed_seconds
        } else {
            0.0
        };

        let server_metrics = ServerMetrics {
            timestamp: chrono::Utc::now().timestamp_millis().to_string(),
            elapsed_seconds,
            interval_time_sec: 0.2,
            total_bytes_transferred: total_bytes,
            total_connections,
            average_connections_per_second,
            current_throughput_bps,
            average_throughput_bps,
        };

        // Append metrics to file
        let metrics_json = serde_json::to_string(&server_metrics).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("server_stats.json")
            .expect("Failed to open server stats file");
        use std::io::Write;
        writeln!(file, "{}", metrics_json).expect("Failed to write server stats");
        file.flush().expect("Failed to flush server stats file");

        // Update for next interval
        last_bytes = total_bytes;
        last_time = Instant::now();
    }
}

fn init_logging() {
    // Initialize tracing subscriber to log to console at debug level
    // This bridges the log crate (used by warp) to tracing
    // LogTracer::init() returns a Result, so we can safely ignore errors if already initialized
    if tracing_log::LogTracer::init().is_err() {
        // Log tracer already initialized, that's fine - another part of the code may have set it up
    }

    // Set up console logging with debug level by default
    // Include warp=debug to capture warp's internal logging
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

    // Initialize start time
    START_TIME.set(Instant::now()).unwrap();

    let params = Arc::new(ServerSimulationParameters {
        min_reply_delay_ms: args.min_reply_delay_ms,
        max_reply_delay_ms: args.max_reply_delay_ms,
        test_duration_seconds: args.test_duration_seconds,
        congested_bytes_per_second: args.congested_bytes_per_second,
        min_congested_penalty_ms: args.min_congested_penalty_ms,
        max_congested_penalty_ms: args.max_congested_penalty_ms,
        congested_error_rate: args.congested_error_rate,
    });

    eprintln!("Server starting with parameters: {:?}", *params);

    // Log server parameters to JSON file
    let params_json = serde_json::to_string_pretty(&*params).unwrap();
    std::fs::write("server_parameters.json", params_json).expect("Failed to write server parameters");

    // Start congestion counter decrease task
    let congested_bytes_per_second = params.congested_bytes_per_second;
    tokio::spawn(async move {
        decrease_congestion_counter_periodically(congested_bytes_per_second).await;
    });

    // Start metrics reporting task
    tokio::spawn(async move {
        report_metrics_periodically().await;
    });

    // Create warp routes
    let data_route = warp::path("data")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(warp::any().map(move || params.clone()))
        .and_then(handle_data_upload);

    let routes = data_route;

    // Start the server
    warp::serve(routes).run(([127, 0, 0, 1], 8080)).await;

    Ok(())
}
