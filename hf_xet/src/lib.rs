pub mod config;
mod headers;
mod legacy;
mod logging;
mod py_download_stream_group;
mod py_download_stream_handle;
mod py_file_download_group;
mod py_file_download_handle;
mod py_file_upload_handle;
mod py_stream_upload_handle;
mod py_upload_commit;
mod py_xet_session;
pub(crate) mod utils;

use pyo3::prelude::*;
pub(crate) use utils::{blocking_call_with_signal_check, convert_xet_error};
use xet_runtime::core::file_handle_limits;

use crate::logging::init_logging;

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

// ── XetTaskState Python enum ──────────────────────────────────────────────────

/// Task state returned by ``status()`` on sessions, commits, and download groups.
///
/// Raises on the ``Error`` variant.  Compare with class-level constants:
///
/// ```python
/// from hf_xet import XetTaskState
/// if session.status() == XetTaskState.Running:
///     ...
/// ```
///
/// # Why not expose `xet_pkg::xet_session::XetTaskState` directly?
///
/// PyO3 0.26 requires that every variant in a `#[pyclass]` enum be either all
/// unit variants or all "complex" (tuple/struct) variants — mixing is not yet
/// supported.  The internal `XetTaskState` has both unit variants (`Running`,
/// `Completed`, …) and a complex variant (`Error(String)`), so it cannot be
/// annotated with `#[pyclass]` as-is.  Rather than restructuring the internal
/// enum, we expose this four-variant unit-only wrapper.  The `Error` case is
/// surfaced as a raised Python exception by `task_state_to_pystate` instead.
#[pyclass(eq, name = "XetTaskState")]
#[derive(Clone, Debug, PartialEq)]
pub enum PyXetTaskState {
    Running,
    Finalizing,
    Completed,
    UserCancelled,
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule(gil_used = false)]
#[allow(unused_variables)]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ── Configuration ────────────────────────────────────────────────────────
    m.add_class::<config::PyXetConfig>()?;
    m.add_class::<config::PyXetConfigIter>()?;

    // ── New XetSession API ───────────────────────────────────────────────────
    m.add_class::<py_xet_session::PyXetSession>()?;
    m.add_class::<py_upload_commit::PyComputeSha256>()?;
    m.add_class::<py_upload_commit::PySkipSha256>()?;
    m.add("COMPUTE_SHA256", py_upload_commit::PyComputeSha256)?;
    m.add("SKIP_SHA256", py_upload_commit::PySkipSha256)?;
    m.add_class::<py_upload_commit::PyXetUploadCommit>()?;
    m.add_class::<py_file_upload_handle::PyXetFileUpload>()?;
    m.add_class::<py_stream_upload_handle::PyXetStreamUpload>()?;
    m.add_class::<py_file_download_group::PyXetFileDownloadGroup>()?;
    m.add_class::<py_file_download_handle::PyXetFileDownload>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStreamGroup>()?;
    m.add_class::<py_download_stream_handle::PyXetDownloadStream>()?;
    m.add_class::<py_download_stream_handle::PyXetUnorderedDownloadStream>()?;

    // ── Python-facing task state enum ────────────────────────────────────────
    m.add_class::<PyXetTaskState>()?;

    // ── Report types (pyclass-annotated in xet_pkg with "python" feature) ────
    m.add_class::<xet_pkg::xet_session::UniqueId>()?;
    m.add_class::<xet_pkg::xet_session::XetFileInfo>()?;
    m.add_class::<xet_pkg::xet_session::DeduplicationMetrics>()?;
    m.add_class::<xet_pkg::xet_session::XetFileMetadata>()?;
    m.add_class::<xet_pkg::xet_session::XetCommitReport>()?;
    m.add_class::<xet_pkg::xet_session::XetDownloadReport>()?;
    m.add_class::<xet_pkg::xet_session::XetDownloadGroupReport>()?;
    m.add_class::<xet_pkg::xet_session::GroupProgressReport>()?;
    m.add_class::<xet_pkg::xet_session::ItemProgressReport>()?;

    // ── Legacy types and functions (kept for backward compatibility) ─────────
    m.add_class::<legacy::PyXetDownloadInfo>()?;
    m.add_class::<legacy::PyXetUploadInfo>()?;
    m.add_class::<legacy::PyPointerFile>()?;
    m.add_class::<legacy::PyItemProgressUpdate>()?;
    m.add_class::<legacy::PyTotalProgressUpdate>()?;
    m.add_function(wrap_pyfunction!(legacy::upload_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(legacy::upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(legacy::hash_files, m)?)?;
    m.add_function(wrap_pyfunction!(legacy::download_files, m)?)?;
    m.add_function(wrap_pyfunction!(legacy::force_sigint_shutdown, m)?)?;

    // ── Exceptions ───────────────────────────────────────────────────────────
    xet_pkg::register_exceptions(m)?;

    // ── Logging ──────────────────────────────────────────────────────────────
    init_logging(py);

    // Raise the soft file handle limits if possible
    file_handle_limits::raise_nofile_soft_to_hard();

    #[cfg(feature = "profiling")]
    {
        profiling::start_profiler();

        #[pyfunction]
        fn profiler_cleanup() {
            profiling::save_profiler_report();
        }

        m.add_function(wrap_pyfunction!(profiler_cleanup, m)?)?;

        let atexit = PyModule::import(py, "atexit")?;
        atexit.call_method1("register", (m.getattr("profiler_cleanup")?,))?;
    }

    Ok(())
}
