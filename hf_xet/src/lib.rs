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

use pyo3::prelude::*;
use xet_pkg::XetError;
use xet_runtime::core::file_handle_limits;

use crate::logging::init_logging;

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

pub(crate) fn convert_xet_error(e: impl Into<XetError>) -> PyErr {
    PyErr::from(e.into())
}

/// Run `f` on a background thread while periodically calling `py.check_signals()`
/// so that Ctrl-C is delivered to Python promptly during long-running operations.
///
/// Background: Python handles SIGINT by setting a flag that is only checked when
/// control returns to the interpreter.  While a blocking Rust call holds the GIL
/// (even via `py.detach()`), that flag is never observed.  By running the blocking
/// work on a separate thread and polling `py.check_signals()` every 100 ms, we
/// give CPython a chance to raise `KeyboardInterrupt` during calls like
/// `commit_blocking()` and `finish_blocking()` that can run for many seconds.
///
/// Reference: <https://pyo3.rs/latest/faq.html> — "Ctrl-C doesn't do anything
/// while my Rust code is executing"
///
/// When `KeyboardInterrupt` is raised here:
/// - It propagates to the Python caller's `except KeyboardInterrupt:` block.
/// - The caller calls `session.sigint_abort()` → sets `sigint_shutdown = true`.
/// - The background thread's next await checkpoint returns immediately.
/// - The thread exits cleanly.
///
/// This is the most Pythonic approach: `KeyboardInterrupt` is a standard exception
/// that propagates through `try/except` like any other.  The cleanup
/// (`sigint_abort()`) lives in Python where it is visible and auditable — no
/// hidden global state, no surprise interactions with `signal.signal()`.
pub(crate) fn blocking_call_with_signal_check<T, E, F>(py: Python<'_>, f: F) -> PyResult<T>
where
    T: Send + 'static,
    E: Into<XetError> + Send + 'static,
    F: FnOnce() -> Result<T, E> + Send + 'static,
{
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Duration;

    let (tx, mut rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        tx.send(f()).ok();
    });
    loop {
        // Release the GIL while waiting so that other threads that need it
        // (e.g. the progress-callback thread, other Python threads) can run.
        // After `detach` returns we re-hold the GIL for the signal check.
        //
        // `Receiver<T>: Send` but `!Sync`, so `&Receiver` is `!Ungil`.  We
        // satisfy the `Ungil` bound by moving `rx` into a `move` closure and
        // returning it alongside the result so it can be rebound for the next
        // iteration.
        let (rx_back, recv_result) = py.detach(move || {
            let result = rx.recv_timeout(Duration::from_millis(100));
            (rx, result)
        });
        rx = rx_back;
        match recv_result {
            Ok(result) => return result.map_err(|e| convert_xet_error(e)),
            Err(RecvTimeoutError::Timeout) => py.check_signals()?,
            // The sender was dropped without sending — the background thread panicked.
            // Return a recoverable error rather than panicking a second time, which
            // would crash the Python interpreter in a PyO3 context.
            Err(RecvTimeoutError::Disconnected) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "blocking operation panicked on background thread",
                ));
            },
        }
    }
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule(gil_used = false)]
#[allow(unused_variables)]
pub fn hf_xet(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ── New XetSession API ───────────────────────────────────────────────────
    m.add_class::<py_xet_session::PyXetSession>()?;
    m.add_class::<py_upload_commit::PySha256Policy>()?;
    m.add_class::<py_upload_commit::PyXetUploadCommitBuilder>()?;
    m.add_class::<py_upload_commit::PyXetUploadCommit>()?;
    m.add_class::<py_file_upload_handle::PyXetFileUpload>()?;
    m.add_class::<py_stream_upload_handle::PyXetStreamUpload>()?;
    m.add_class::<py_file_download_group::PyXetFileDownloadGroupBuilder>()?;
    m.add_class::<py_file_download_group::PyXetFileDownloadGroup>()?;
    m.add_class::<py_file_download_handle::PyXetFileDownload>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStreamGroupBuilder>()?;
    m.add_class::<py_download_stream_group::PyXetDownloadStreamGroup>()?;
    m.add_class::<py_download_stream_handle::PyXetDownloadStream>()?;
    m.add_class::<py_download_stream_handle::PyXetUnorderedDownloadStream>()?;

    // ── Report types (pyclass-annotated in xet_pkg with "python" feature) ────
    m.add_class::<xet_pkg::xet_session::UniqueID>()?;
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
