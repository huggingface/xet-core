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

use crate::logging::init_logging;

// For profiling
#[cfg(feature = "profiling")]
pub(crate) mod profiling;

pub(crate) fn convert_xet_error(e: impl Into<XetError>) -> PyErr {
    PyErr::from(e.into())
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
