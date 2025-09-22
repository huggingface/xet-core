use pyo3::Python;
use pyo3::types::PyAnyMethods;
use tracing::info;
use xet_runtime::logging::LoggingConfig;

fn get_version_info_string(py: Python<'_>) -> String {
    // populate remote telemetry calls with versions for python and hf_hub if possible
    let mut version_info = String::new();

    // Get Python version
    if let Ok(sys) = py.import("sys")
        && let Ok(version) = sys.getattr("version").and_then(|v| v.extract::<String>())
        && let Some(python_version_number) = version.split_whitespace().next()
    {
        version_info.push_str(&format!("python/{python_version_number}; "));
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

/// Wrap the core runtime logging functions.
pub fn init_logging(py: Python) {
    let version_info = get_version_info_string(py);

    let cfg = LoggingConfig::new(version_info);

    xet_runtime::logging::init_logging(cfg);

    info!("hf_xet logging cofigured.");
}
