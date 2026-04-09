use pyo3::prelude::*;
use xet_pkg::xet_session::{XetSession, XetSessionBuilder, XetTaskState};

use crate::convert_xet_error;
use crate::py_download_group::PyXetFileDownloadGroupBuilder;
use crate::py_upload_commit::PyXetUploadCommitBuilder;

// ── PyXetSession ─────────────────────────────────────────────────────────────

/// Manages a Xet runtime context and connection pool.
///
/// Session objects are cheap to clone — all clones share the same underlying state.
#[pyclass(name = "XetSession")]
#[derive(Clone)]
pub struct PyXetSession {
    pub(crate) inner: XetSession,
}

#[pymethods]
impl PyXetSession {
    /// Create a new XetSession.
    #[new]
    pub fn new(py: Python<'_>) -> PyResult<Self> {
        let session = py.detach(|| XetSessionBuilder::new().build().map_err(PyErr::from))?;
        Ok(Self { inner: session })
    }

    /// Return a builder for a new upload commit.
    ///
    /// Configure auth on the builder (``with_endpoint``, ``with_token_info``,
    /// ``with_token_refresh_url``), then call ``build()`` to get an
    /// :class:`XetUploadCommit`.
    pub fn new_upload_commit(&self) -> PyResult<PyXetUploadCommitBuilder> {
        let builder = self.inner.new_upload_commit().map_err(convert_xet_error)?;
        Ok(PyXetUploadCommitBuilder { inner: Some(builder) })
    }

    /// Return a builder for a new file download group.
    ///
    /// Configure auth on the builder, then call ``build()`` to get an
    /// :class:`XetFileDownloadGroup`.
    pub fn new_file_download_group(&self) -> PyResult<PyXetFileDownloadGroupBuilder> {
        let builder = self.inner.new_file_download_group().map_err(convert_xet_error)?;
        Ok(PyXetFileDownloadGroupBuilder { inner: Some(builder) })
    }

    /// Current task state: ``"Running"``, ``"Finalizing"``, ``"Completed"``, or
    /// ``"UserCancelled"``.  Raises on error state.
    pub fn status(&self) -> PyResult<&'static str> {
        task_state_to_str(self.inner.status().map_err(convert_xet_error)?)
    }

    /// Cancel all active operations on this session.
    ///
    /// The session remains usable after abort — new commits and groups can be
    /// created.
    pub fn abort(&self) -> PyResult<()> {
        self.inner.abort().map_err(convert_xet_error)
    }

    /// SIGINT-style abort: shuts down the runtime and cancels all tasks.
    pub fn sigint_abort(&self) -> PyResult<()> {
        self.inner.sigint_abort().map_err(convert_xet_error)
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

pub(crate) fn task_state_to_str(state: XetTaskState) -> PyResult<&'static str> {
    match state {
        XetTaskState::Running => Ok("Running"),
        XetTaskState::Finalizing => Ok("Finalizing"),
        XetTaskState::Completed => Ok("Completed"),
        XetTaskState::UserCancelled => Ok("UserCancelled"),
        XetTaskState::Error(msg) => Err(convert_xet_error(xet_pkg::XetError::TaskError(msg))),
    }
}
