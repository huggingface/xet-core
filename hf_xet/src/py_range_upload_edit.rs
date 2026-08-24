//! Python bindings for XetRangeUploadEdit.

use std::sync::Arc;

use pyo3::prelude::*;
use xet_pkg::xet_session::XetRangeUploadEdit;

/// Handle for a single edit within a :class:`XetRangeUploadCommit`.
///
/// Returned by :meth:`XetRangeUploadCommit.edit`, :meth:`XetRangeUploadCommit.insert`,
/// :meth:`XetRangeUploadCommit.delete`, and :meth:`XetRangeUploadCommit.append`.
/// Feed data incrementally with :meth:`write`, then call :meth:`finish` to
/// finalise the edit before calling :meth:`XetRangeUploadCommit.commit`.
#[pyclass(name = "XetRangeUploadEdit")]
pub struct PyXetRangeUploadEdit {
    pub(crate) inner: XetRangeUploadEdit,
}

#[pymethods]
impl PyXetRangeUploadEdit {
    /// Feed data into this edit.
    ///
    /// May be called any number of times before :meth:`finish`.
    pub fn write(&self, data: Vec<u8>) {
        self.inner.write(&data);
    }

    /// Finalise the edit.
    ///
    /// Must be called before :meth:`XetRangeUploadCommit.commit`.  After a successful
    /// finish, subsequent calls return ``None``.
    pub fn finish(&self) -> PyResult<()> {
        let inner = Arc::new(self.inner.clone());
        match inner.finish() {
            Ok(_) => Ok(()),
            Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "edit was already finished",
            )),
        }
    }
}