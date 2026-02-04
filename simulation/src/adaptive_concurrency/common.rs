//! Shared types for adaptive concurrency simulation (metrics, parameters).

use cas_client::adaptive_concurrency::{CCLatencyModelState, CCSuccessModelState};
use serde::{Deserialize, Serialize};

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

/// Network stats for a scenario (optional; used for summary network_utilization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    pub timestamp: String,
    pub latency_ms: f64,
    pub bandwidth_bytes_per_sec: u64,
    pub congestion_mode: Option<String>,
    pub interface: Option<String>,
}
