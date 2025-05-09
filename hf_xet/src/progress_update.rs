use std::fmt::{Debug, Formatter};

use error_printer::ErrorPrinter;
use itertools::Itertools;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{IntoPyDict, PyString};
use pyo3::{IntoPyObject, Py, PyAny, PyResult, Python};
use tracing::error;
use utils::progress::{ProgressUpdate, TrackingProgressUpdater};

/// A wrapper over a passed-in python function to update
/// the python process of some download/upload progress
/// implements the ProgressUpdater trait and should be
/// passed around as a ProgressUpdater trait object or
/// as a template parameter
pub struct WrappedProgressUpdater {
    /// Is this enabled?
    progress_updating_enabled: bool,

    /// the function py_func is responsible for passing in the update value
    /// into the python context. Expects 1 int (uint64) parameter that
    /// is a number to increment the progress counter by.
    py_func: Py<PyAny>,
    name: String,

    /// Whether to use the simple incremental progress updating method or
    /// the more detailed
    update_with_detailed_progress: bool,
}

impl Debug for WrappedProgressUpdater {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrappedTokenRefresher({})", self.name)
    }
}

const DETAILED_PROGRESS_ARG_NAMES: [&str; 4] = ["item_id", "bytes_completed", "total_bytes", "update_increment"];

impl WrappedProgressUpdater {
    pub fn new(py_func: Py<PyAny>) -> PyResult<Self> {
        // Analyze the function to make sure it's the correct form. If it's 4 arguments with
        // the appropriate names, than we call it using the detailed progress update; if it's
        // a single function, we assume it's a global increment function and just pass in the update
        // increment.
        Python::with_gil(|py| {
            let func = py_func.bind(py);

            // Test if it's enabled first; if None is passed in, then this is disabled.
            if py_func.is_none(py) {
                return Ok(Self {
                    progress_updating_enabled: false,
                    py_func,
                    name: Default::default(),
                    update_with_detailed_progress: false,
                });
            }

            let name = func
                .repr()
                .and_then(|repr| repr.extract::<String>())
                .unwrap_or_else(|_| "unknown".to_string());

            if !func.is_callable() {
                error!("ProgressUpdater func: {name} is not callable");
                return Err(PyTypeError::new_err(format!("update func: {name} is not callable")));
            }

            let inspect = py.import("inspect")?;
            let sig = inspect.call_method1("signature", (func,))?;
            let params = sig.getattr("parameters")?;

            let param_names: Vec<Py<PyString>> = params
                .call_method0("items")?
                .try_iter()?
                .map(|item| {
                    let (k, _): (Py<PyString>, Py<PyAny>) = item?.extract()?;
                    Ok(k)
                })
                .collect::<PyResult<_>>()?;

            let update_with_detailed_progress = match param_names.len() {
                1 => false,
                4 => {
                    if param_names
                        .iter()
                        .zip(DETAILED_PROGRESS_ARG_NAMES.into_iter())
                        .all(|(v1, v2)| v1.to_string_lossy(py) == v2)
                    {
                        true
                    } else {
                        return Err(PyTypeError::new_err(format!(
                            "Function {name} must have either one argument or four named arguments ({})",
                            DETAILED_PROGRESS_ARG_NAMES.iter().join(", ")
                        )));
                    }
                },
                _ => {
                    return Err(PyTypeError::new_err(format!(
                        "Function {name} must take exactly 1 or 4 arguments, but got {}",
                        param_names.len()
                    )))
                },
            };

            Ok(Self {
                progress_updating_enabled: true,
                py_func,
                name,
                update_with_detailed_progress,
            })
        })
    }

    async fn register_updates_impl(&self, updates: &[ProgressUpdate]) -> PyResult<()> {
        Python::with_gil(|py| {
            let f = self.py_func.bind(py);

            if self.update_with_detailed_progress {
                let str2py = |c: &str| -> PyResult<Py<PyAny>> { Ok(c.into_pyobject(py)?.into()) };
                let int2py = |v: u64| -> PyResult<Py<PyAny>> { Ok(v.into_pyobject(py)?.into()) };

                let args = [
                    str2py(DETAILED_PROGRESS_ARG_NAMES[0])?,
                    str2py(DETAILED_PROGRESS_ARG_NAMES[1])?,
                    str2py(DETAILED_PROGRESS_ARG_NAMES[2])?,
                    str2py(DETAILED_PROGRESS_ARG_NAMES[3])?,
                ];

                for update in updates {
                    let kwargs = [
                        (args[0].clone_ref(py), str2py(&update.item_name)?),
                        (args[1].clone_ref(py), int2py(update.completed_count)?),
                        (args[2].clone_ref(py), int2py(update.total_count)?),
                        (args[3].clone_ref(py), int2py(update.update_increment)?),
                    ]
                    .into_py_dict(py)?;

                    f.call((), Some(&kwargs))?;
                }
            } else {
                let update_increment: u64 = updates.iter().map(|pr| pr.update_increment).sum();
                let _ = f.call1((update_increment,))?;
            }

            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl TrackingProgressUpdater for WrappedProgressUpdater {
    async fn register_updates(&self, updates: &[ProgressUpdate]) {
        if self.progress_updating_enabled {
            let _ = self
                .register_updates_impl(updates)
                .await
                .log_error("Python exception updating progress:");
        }
    }
}
