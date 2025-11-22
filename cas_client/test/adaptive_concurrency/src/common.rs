#![allow(dead_code)]
// Re-export the NetworkModelState from the adaptive_concurrency module
pub use cas_client::adaptive_concurrency::{CCLatencyModelState, CCSuccessModelState};
use serde::{Deserialize, Serialize};

/// Metrics for a single client to report data transfer performance.
///
/// These are dumped to stdout as json every 200ms to track the performance of the client.
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSimulationParameters {
    pub min_reply_delay_ms: u64,
    pub max_reply_delay_ms: u64,
    pub test_duration_seconds: u64,
    pub congested_bytes_per_second: u64,
    pub min_congested_penalty_ms: u64,
    pub max_congested_penalty_ms: u64,
    pub congested_error_rate: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerMetrics {
    pub timestamp: String,
    pub elapsed_seconds: f64,
    pub interval_time_sec: f64,
    pub total_bytes_transferred: u64,
    pub total_connections: u64,
    pub average_connections_per_second: f64,
    pub current_throughput_bps: f64,
    pub average_throughput_bps: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkStats {
    pub timestamp: String,
    pub latency_ms: f64,
    pub bandwidth_bytes_per_sec: u64,
    pub congestion_mode: Option<String>,
    pub interface: Option<String>,
}
