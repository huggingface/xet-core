use pyo3::Python;
use pyo3::types::PyAnyMethods;
use tracing::info;

fn get_version_info_string(py: Python<'_>) -> String {
    // populate version info for the User-Agent header
    let mut version_info = String::new();

    // Get Python version
    if let Ok(sys) = py.import("sys")
        && let Ok(version) = sys.getattr("version").and_then(|v| v.extract::<String>())
        && let Some(python_version_number) = version.split_whitespace().next()
    {
        version_info.push_str(&format!("python/{python_version_number}; "));
    }

    // Get huggingface_hub+hf_xet versions
    let package_names = ["huggingface_hub", "hf_xet"];
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

/// Initialize the global tracing subscriber.
pub fn init_logging(py: Python<'_>) {
    let version_info = get_version_info_string(py);
    xet_pkg::init_logging(version_info);
    info!("hf_xet logging configured.");
}
