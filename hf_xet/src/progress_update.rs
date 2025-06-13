use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use error_printer::ErrorPrinter;
use itertools::Itertools;
use progress_tracking::{ProgressUpdate, TrackingProgressUpdater};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{IntoPyDict, PyList, PyString};
use pyo3::{pyclass, IntoPyObjectExt, Py, PyAny, PyResult, Python};
use tracing::error;
use xet_threadpool::exports::tokio;

use crate::runtime::convert_multithreading_error;

/// Python-exposed versions of the per-item and total progress update classes.
///
/// Both `PyTotalProgressUpdate` and `PyItemProgressUpdate` are passed
/// into a Python callback given to the wrapper class below.  For example:
///
/// ```python
/// def update_progress(self, total_update, item_updates):
///     from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
///
///     # Update overall progress (we assume this has been initialized).
///     self.progress.update(
///         self.bytes_processed_task_id,
///         advance=total_update.total_bytes_completion_increment,
///         total = total_update.total_bytes
///     )
///
///     # Update upload progress ; the total may have changed so set that too.
///     self.progress.update(
///         self.bytes_uploaded_task_id,
///         advance=total_update.total_transfer_bytes_completion_increment,
///         total = total_update.total_transfer_bytes
///     )
///
///     # Update each item:
///     for item in item_updates:
///         name = item.item_name
///         if name not in self.item_tasks:
///             self.item_tasks[name] = self.progress.add_task(
///                 name, total=item.total_bytes
///             )
///         self.progress.update(
///             self.item_tasks[name],
///             advance=item.bytes_completion_increment,
///         )
/// ```
///
/// In addition, the other possible bookkeeping values for everything are contained in this
/// as needed.
#[pyclass]
pub struct PyItemProgressUpdate {
    /// The name of the item, or a tag that is translated later.
    #[pyo3(get)]
    pub item_name: Py<PyString>,

    /// The total bytes contained in this item.   
    #[pyo3(get)]
    pub total_bytes: u64,

    /// The number of bytes completed so far, either by deduplication or transfer.
    #[pyo3(get)]
    pub bytes_completed: u64,

    /// The change in bytes completed since the last update.
    #[pyo3(get)]
    pub bytes_completion_increment: u64,
}

/// Update class for total updates
#[pyclass]
pub struct PyTotalProgressUpdate {
    /// The total bytes known for processing and possibly uploaded or downloaded.
    #[pyo3(get)]
    pub total_bytes: u64,

    /// How much total_bytes has changed from the last update..
    #[pyo3(get)]
    pub total_bytes_increment: u64,

    /// How many of the bytes queued for processing have been examined
    /// and either deduped or queued for upload or download.  
    #[pyo3(get)]
    pub total_bytes_completed: u64,

    /// The change in total_bytes_completed since the same upload.  
    #[pyo3(get)]
    pub total_bytes_completion_increment: u64,

    /// If known, the current completion speed.
    #[pyo3(get)]
    pub total_bytes_completion_rate: Option<f64>,

    /// The total bytes scheduled for transfer; also contained in total_bytes.
    #[pyo3(get)]
    pub total_transfer_bytes: u64,

    /// How much total_transfer_bytes has changed since the last update.
    #[pyo3(get)]
    pub total_transfer_bytes_increment: u64,

    /// The cumulative bytes uploaded or downloaded so far.  Also contained in total_bytes_completed.
    #[pyo3(get)]
    pub total_transfer_bytes_completed: u64,

    /// The change in total_transfer_bytes_completed since the last update.
    #[pyo3(get)]
    pub total_transfer_bytes_completion_increment: u64,

    /// If known, the current completion speed for bytes transferred.
    #[pyo3(get)]
    pub total_transfer_bytes_completion_rate: Option<f64>,
}

/// A wrapper over a passed-in python function to update
/// the python process of some download/upload progress
/// implements the ProgressUpdater trait and should be
/// passed around as a ProgressUpdater trait object or
/// as a template parameter
struct WrappedProgressUpdaterImpl {
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

impl Debug for WrappedProgressUpdaterImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrappedTokenRefresher({})", self.name)
    }
}

const DETAILED_PROGRESS_ARG_NAMES: [&str; 2] = ["total_update", "item_updates"];

impl WrappedProgressUpdaterImpl {
    pub fn new(py_func: Py<PyAny>) -> PyResult<Self> {
        // Analyze the function to make sure it's the correct form. If it's 4 arguments with
        // the appropriate names, than we call it using the detailed progress update; if it's
        // a single function, we assume it's a global increment function and just pass in the update
        // increment.
        //
        // Run on compute thread that doesn't block async workers
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
                2 => {
                    if param_names
                        .iter()
                        .zip(DETAILED_PROGRESS_ARG_NAMES.into_iter())
                        .all(|(v1, v2)| v1.to_string_lossy(py) == v2)
                    {
                        true
                    } else {
                        return Err(PyTypeError::new_err(format!(
                            "Function {name} must have either one argument or two named arguments ({})",
                            DETAILED_PROGRESS_ARG_NAMES.iter().join(", ")
                        )));
                    }
                },
                _ => {
                    return Err(PyTypeError::new_err(format!(
                        "Function {name} must take exactly 1 or 2 arguments, but got {}",
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

    async fn register_updates_impl(self: Arc<Self>, updates: ProgressUpdate) -> PyResult<()> {
        // Run on compute thread that doesn't block async workers
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let f = self.py_func.bind(py);

                if self.update_with_detailed_progress {
                    let total_update_report: Py<PyAny> = Py::new(
                        py,
                        PyTotalProgressUpdate {
                            total_bytes: updates.total_bytes,
                            total_bytes_increment: updates.total_bytes_increment,
                            total_bytes_completed: updates.total_bytes_completed,
                            total_bytes_completion_increment: updates.total_bytes_completion_increment,
                            total_bytes_completion_rate: updates.total_bytes_completion_rate,
                            total_transfer_bytes: updates.total_transfer_bytes,
                            total_transfer_bytes_increment: updates.total_transfer_bytes_increment,
                            total_transfer_bytes_completed: updates.total_transfer_bytes_completed,
                            total_transfer_bytes_completion_increment: updates
                                .total_transfer_bytes_completion_increment,
                            total_transfer_bytes_completion_rate: updates.total_transfer_bytes_completion_rate,
                        },
                    )?
                    .into_py_any(py)?;

                    let item_updates_v: Vec<Py<PyAny>> = updates
                        .item_updates
                        .into_iter()
                        .map(|u| {
                            Py::new(
                                py,
                                PyItemProgressUpdate {
                                    item_name: PyString::new(py, &u.item_name).into(),
                                    total_bytes: u.total_bytes,
                                    bytes_completed: u.bytes_completed,
                                    bytes_completion_increment: u.bytes_completion_increment,
                                },
                            )?
                            .into_py_any(py)
                        })
                        .collect::<PyResult<Vec<_>>>()?;

                    let item_updates: Py<PyAny> = PyList::new(py, item_updates_v)?.into_py_any(py)?;

                    let argname_total_update: Py<PyAny> = DETAILED_PROGRESS_ARG_NAMES[0].into_py_any(py)?;
                    let argname_item_updates: Py<PyAny> = DETAILED_PROGRESS_ARG_NAMES[1].into_py_any(py)?;

                    let kwargs = [
                        (argname_total_update, total_update_report),
                        (argname_item_updates, item_updates),
                    ]
                    .into_py_dict(py)?;

                    f.call((), Some(&kwargs))?;
                } else {
                    let update_increment: u64 =
                        updates.item_updates.iter().map(|pr| pr.bytes_completion_increment).sum();
                    let _ = f.call1((update_increment,))?;
                }

                Ok(())
            })
        })
        .await
        .map_err(convert_multithreading_error)?
    }
}

#[derive(Debug)]
pub struct WrappedProgressUpdater {
    inner: Arc<WrappedProgressUpdaterImpl>,
}

impl WrappedProgressUpdater {
    pub fn new(py_func: Py<PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(WrappedProgressUpdaterImpl::new(py_func)?),
        })
    }
}

#[async_trait::async_trait]
impl TrackingProgressUpdater for WrappedProgressUpdater {
    async fn register_updates(&self, updates: ProgressUpdate) {
        let inner = self.inner.clone();

        if inner.progress_updating_enabled {
            let _ = inner
                .register_updates_impl(updates)
                .await
                .log_error("Python exception updating progress:");
        }
    }
}
