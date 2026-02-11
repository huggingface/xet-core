//! Upload concurrency simulation: run upload clients through the bandwidth proxy to LocalTestServer dummy_upload,
//! with timeline and summary CSV reporting.

mod reporting;
mod upload_simulation_client;

pub use reporting::{generate_summary_csv, generate_timeline_csv};
pub use upload_simulation_client::{ClientMetrics, run_upload_clients, run_upload_clients_until_cancelled};
