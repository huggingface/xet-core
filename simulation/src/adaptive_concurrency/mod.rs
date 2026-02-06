//! Adaptive concurrency simulation: run upload clients through the bandwidth proxy to LocalTestServer dummy_upload,
//! with timeline and summary CSV reporting.

mod client_runner;
mod common;
mod reporting;
mod scenarios;

pub use client_runner::{run_upload_clients, wait_for_server_ready};
pub use common::{ClientMetrics, NetworkStats};
pub use reporting::{generate_summary_csv, generate_timeline_csv};
pub use scenarios::{
    DEFAULT_SERVER_DELAY_MS, Scenario, ServerDelayMs, aggregate_summary, build_heavy_congestion_schedule,
    build_upload_profile, run_scenario, scenario_added_uploads, scenario_gitxet_upload_burst, scenario_sanity_check,
    scenario_single_upload,
};
