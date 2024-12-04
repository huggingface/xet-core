use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Py, PyAny, PyErr, PyResult, Python};
use std::fmt::{Debug, Formatter};
use tracing::{error, trace};
use utils::progress::ProgressUpdater;

/// A wrapper struct of a python function to update a progress bar
pub struct WrappedProgressUpdater {
    py_func: Py<PyAny>,
    name: String,
}

impl Debug for WrappedProgressUpdater {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrappedTokenRefresher({})", self.name)
    }
}

impl WrappedProgressUpdater {
    pub fn from_func(py_func: Py<PyAny>) -> PyResult<Self> {
        let name = Self::validate_callable(&py_func)?;
        Ok(Self { py_func, name })
    }

    /// Validate that the inputted python object is callable
    fn validate_callable(py_func: &Py<PyAny>) -> Result<String, PyErr> {
        Python::with_gil(|py| {
            let f = py_func.bind(py);
            let name = f
                .repr()
                .and_then(|repr| repr.extract::<String>())
                .unwrap_or("unknown".to_string());
            if !f.is_callable() {
                error!("ProgressUpdater func: {name} is not callable");
                return Err(PyTypeError::new_err(format!("update func: {name} is not callable")));
            }
            Ok(name)
        })
    }
}

impl ProgressUpdater for WrappedProgressUpdater {
    fn update(&self, total: u64) {
        trace!("updating progress bar");
        Python::with_gil(|py| {
            let f = self.py_func.bind(py);
            if !f.is_callable() {
                error!("ProgressUpdater func: {} is not callable", self.name);
                return;
            }
            let _ = f.call1((total,))
                .inspect_err(|e| error!("python exception trying to update progress bar {e:?}"));
        });
    }
}
