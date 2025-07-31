use std::env;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use pyo3::Python;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use utils::normalized_path_from_user_string;
use xet_threadpool::ThreadPool;

use crate::log_buffer::{get_telemetry_task, LogBufferLayer, TelemetryTaskInfo, TELEMETRY_PRE_ALLOC_BYTES};

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "warn";

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "warn";

fn use_json() -> Option<bool> {
    env::var("HF_XET_LOG_FORMAT").ok().map(|s| s.eq_ignore_ascii_case("json"))
}

fn init_logging_to_file(path: &Path) -> Result<(), std::io::Error> {
    // Set up logging to a file.
    use std::ffi::OsStr;

    use tracing_appender::{non_blocking, rolling};

    let (path, file_name) = match path.file_name() {
        Some(name) => (path.to_path_buf(), name),
        None => (path.join("xet.log"), OsStr::new("xet.log")),
    };

    let log_directory = match path.parent() {
        Some(parent) => {
            std::fs::create_dir_all(parent)?;
            parent
        },
        None => Path::new("."),
    };

    // Make sure the log location is writeable so we error early here and dump to stderr on failure.
    std::fs::write(&path, &[])?;

    // Build a non‑blocking file appender. • `rolling::never` = one static file, no rotation. • Keep the
    // `WorkerGuard` alive so the background thread doesn’t shut down and drop messages.
    let file_appender = rolling::never(log_directory, file_name);

    let (writer, guard) = non_blocking(file_appender);

    // Store the guard globally so it isn’t dropped.
    static FILE_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();
    let _ = FILE_GUARD.set(guard); // ignore error if already initialised

    // Build the fmt layer.
    let fmt_layer_base = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .with_writer(writer);

    // Standard filter layer: RUST_LOG env var or DEFAULT_LOG_LEVEL fallback.
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    // Initialise the subscriber.
    if use_json().unwrap_or(true) {
        tracing_subscriber::registry()
            .with(fmt_layer_base.json())
            .with(filter_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(fmt_layer_base.pretty())
            .with(filter_layer)
            .init();
    }

    Ok(())
}

fn init_global_logging(py: Python) -> Option<TelemetryTaskInfo> {
    if let Ok(log_path_s) = env::var("HF_XET_LOG_FILE") {
        let log_path = normalized_path_from_user_string(log_path_s);
        match init_logging_to_file(&log_path) {
            Ok(_) => return None,
            Err(e) => {
                eprintln!("Error opening log file {log_path:?} for writing: {e:?}.  Reverting to logging to console.");
            },
        }
    }

    let fmt_layer_base = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false);

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    // Client-side telemetry, default is OFF
    // To enable telemetry set env var HF_HUB_ENABLE_TELEMETRY
    if env::var("HF_HUB_ENABLE_TELEMETRY").is_err_and(|e| e == env::VarError::NotPresent) {
        let tr_sub = tracing_subscriber::registry().with(filter_layer);

        if use_json().unwrap_or(false) {
            tr_sub.with(fmt_layer_base.json()).init();
        } else {
            tr_sub.with(fmt_layer_base.pretty()).init();
        }

        None
    } else {
        let telemetry_buffer_layer = LogBufferLayer::new(py, TELEMETRY_PRE_ALLOC_BYTES);
        let telemetry_task_info: TelemetryTaskInfo =
            (telemetry_buffer_layer.buffer.clone(), telemetry_buffer_layer.stats.clone());

        let telemetry_filter_layer =
            telemetry_buffer_layer.with_filter(FilterFn::new(|meta| meta.target() == "client_telemetry"));

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer_base.json())
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
