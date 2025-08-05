use std::env;
use std::path::Path;
use std::sync::OnceLock;

use pyo3::types::PyAnyMethods;
use pyo3::Python;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use utils::normalized_path_from_user_string;

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
    std::fs::write(&path, [])?;

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

fn get_version_info_string(py: Python<'_>) -> String {
    // populate remote telemetry calls with versions for python and hf_hub if possible
    let mut version_info = String::new();

    // Get Python version
    if let Ok(sys) = py.import("sys") {
        if let Ok(version) = sys.getattr("version").and_then(|v| v.extract::<String>()) {
            if let Some(python_version_number) = version.split_whitespace().next() {
                version_info.push_str(&format!("python/{python_version_number}; "));
            }
        }
    }

    // Get huggingface_hub+hf_xet versions
    let package_names = ["huggingface_hub", "hfxet"];
    if let Ok(importlib_metadata) = py.import("importlib.metadata") {
        for package_name in package_names.iter() {
            if let Ok(version) = importlib_metadata
                .call_method1("version", (package_name,))
                .and_then(|v| v.extract::<String>())
            {
                version_info.push_str(&format!("{package_name}/{version}; "));
            }
        }
    }
    version_info
}

pub fn init_logging(py: Python) {
    let version_info = get_version_info_string(py);

    if let Ok(log_path_s) = env::var("HF_XET_LOG_FILE") {
        let log_path = normalized_path_from_user_string(log_path_s);
        match init_logging_to_file(&log_path) {
            Ok(_) => {
                info!("hf_xet version info: {version_info}");
                return;
            },
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

    // Now, just use basic console logging.
    let tr_sub = tracing_subscriber::registry().with(filter_layer);

    if use_json().unwrap_or(false) {
        tr_sub.with(fmt_layer_base.json()).init();
    } else {
        tr_sub.with(fmt_layer_base.pretty()).init();
    }

    info!("hf_xet version info: {version_info}");
}
