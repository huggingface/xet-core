use std::env;
use std::sync::{Arc, OnceLock};

use pyo3::Python;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use xet_threadpool::ThreadPool;

use crate::log_buffer::{get_telemetry_task, LogBufferLayer, TelemetryTaskInfo, TELEMETRY_PRE_ALLOC_BYTES};

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "warn";

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "warn";

fn init_global_logging(py: Python) -> Option<TelemetryTaskInfo> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    // Client-side telemetry, default is OFF
    // To enable telemetry set env var HF_HUB_ENABLE_TELEMETRY
    if env::var("HF_HUB_ENABLE_TELEMETRY").is_err_and(|e| e == env::VarError::NotPresent) {
        tracing_subscriber::registry().with(fmt_layer).with(filter_layer).init();
        None
    } else {
        let telemetry_buffer_layer = LogBufferLayer::new(py, TELEMETRY_PRE_ALLOC_BYTES);
        let telemetry_task_info: TelemetryTaskInfo =
            (telemetry_buffer_layer.buffer.clone(), telemetry_buffer_layer.stats.clone());

        let telemetry_filter_layer =
            telemetry_buffer_layer.with_filter(FilterFn::new(|meta| meta.target() == "client_telemetry"));

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(filter_layer)
            .with(telemetry_filter_layer)
            .init();

        Some(telemetry_task_info)
    }
}

pub fn initialize_runtime_logging(py: Python, runtime: Arc<ThreadPool>) {
    static GLOBAL_TELEMETRY_TASK_INFO: OnceLock<Option<TelemetryTaskInfo>> = OnceLock::new();

    // First get or init the global logging componenents.
    let telemetry_task_info = GLOBAL_TELEMETRY_TASK_INFO.get_or_init(move || init_global_logging(py));

    // Spawn the telemetry logging.
    if let Some(ref tti) = telemetry_task_info {
        let telemetry_task = get_telemetry_task(tti.clone());
        let _telemetry_task = runtime.spawn(telemetry_task);
    }
}
