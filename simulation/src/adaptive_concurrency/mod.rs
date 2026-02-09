//! Adaptive concurrency simulation: run upload clients through the bandwidth proxy to LocalTestServer dummy_upload,
//! with timeline and summary CSV reporting.

mod client_runner;
mod common;
mod reporting;

pub use client_runner::{run_upload_clients, wait_for_server_ready};
pub use common::{ClientMetrics, NetworkStats};
pub use reporting::{generate_summary_csv, generate_timeline_csv};
