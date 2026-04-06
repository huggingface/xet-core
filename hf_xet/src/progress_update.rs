use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{IntoPyDict, PyList, PyString};
use pyo3::{IntoPyObjectExt, Py, PyAny, PyResult, Python, pyclass};
use tracing::error;
use xet_pkg::xet_session::{GroupProgressReport, ItemProgressReport, UniqueID};
use xet_runtime::core::XetRuntime;
use xet_runtime::error_printer::ErrorPrinter;

use crate::runtime::convert_multithreading_error;

// === PyO3 progress update classes (exposed to Python) ===

#[pyclass]
pub struct PyItemProgressUpdate {
    #[pyo3(get)]
    pub item_name: Py<PyString>,
    #[pyo3(get)]
    pub total_bytes: u64,
    #[pyo3(get)]
    pub bytes_completed: u64,
    #[pyo3(get)]
    pub bytes_completion_increment: u64,
}

#[pyclass]
pub struct PyTotalProgressUpdate {
    #[pyo3(get)]
    pub total_bytes: u64,
    #[pyo3(get)]
    pub total_bytes_increment: u64,
    #[pyo3(get)]
    pub total_bytes_completed: u64,
    #[pyo3(get)]
    pub total_bytes_completion_increment: u64,
    #[pyo3(get)]
    pub total_bytes_completion_rate: Option<f64>,
    #[pyo3(get)]
    pub total_transfer_bytes: u64,
    #[pyo3(get)]
    pub total_transfer_bytes_increment: u64,
    #[pyo3(get)]
    pub total_transfer_bytes_completed: u64,
    #[pyo3(get)]
    pub total_transfer_bytes_completion_increment: u64,
    #[pyo3(get)]
    pub total_transfer_bytes_completion_rate: Option<f64>,
}

// === Progress callback (validated Python callable) ===

const DETAILED_PROGRESS_ARG_NAMES: [&str; 2] = ["total_update", "item_updates"];

/// A validated Python progress callback. Determines on construction whether
/// the callback uses the simple (1-arg increment) or detailed (2-arg total +
/// items) calling convention.
pub struct ProgressCallback {
    py_func: Py<PyAny>,
    detailed: bool,
    enabled: bool,
}

impl Debug for ProgressCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProgressCallback(detailed={}, enabled={})", self.detailed, self.enabled)
    }
}

impl ProgressCallback {
    pub fn new(py_func: Py<PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            if py_func.is_none(py) {
                return Ok(Self {
                    py_func,
                    detailed: false,
                    enabled: false,
                });
            }

            let func = py_func.bind(py);
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

            let detailed = match param_names.len() {
                1 => false,
                2 => {
                    if param_names
                        .iter()
                        .zip(DETAILED_PROGRESS_ARG_NAMES)
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
                    )));
                },
            };

            Ok(Self {
                py_func,
                detailed,
                enabled: true,
            })
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Send a progress diff to the Python callback via spawn_blocking
    /// (to avoid blocking the async runtime while holding the GIL).
    pub async fn send_update(&self, diff: ProgressDiff) -> PyResult<()> {
        if !self.enabled || diff.is_empty() {
            return Ok(());
        }

        let py_func = Python::attach(|py| self.py_func.clone_ref(py));
        let detailed = self.detailed;

        let rt = XetRuntime::current();
        rt.spawn_blocking(move || {
            Python::attach(|py| {
                let f = py_func.bind(py);

                if detailed {
                    let total_update: Py<PyAny> = Py::new(
                        py,
                        PyTotalProgressUpdate {
                            total_bytes: diff.total_bytes,
                            total_bytes_increment: diff.total_bytes_increment,
                            total_bytes_completed: diff.total_bytes_completed,
                            total_bytes_completion_increment: diff.total_bytes_completion_increment,
                            total_bytes_completion_rate: diff.total_bytes_completion_rate,
                            total_transfer_bytes: diff.total_transfer_bytes,
                            total_transfer_bytes_increment: diff.total_transfer_bytes_increment,
                            total_transfer_bytes_completed: diff.total_transfer_bytes_completed,
                            total_transfer_bytes_completion_increment: diff.total_transfer_bytes_completion_increment,
                            total_transfer_bytes_completion_rate: diff.total_transfer_bytes_completion_rate,
                        },
                    )?
                    .into_py_any(py)?;

                    let item_updates_v: Vec<Py<PyAny>> = diff
                        .item_diffs
                        .iter()
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
                        (argname_total_update, total_update),
                        (argname_item_updates, item_updates),
                    ]
                    .into_py_dict(py)?;

                    f.call((), Some(&kwargs))?;
                } else {
                    let update_increment: u64 = diff.item_diffs.iter().map(|d| d.bytes_completion_increment).sum();
                    let increment = if update_increment > 0 {
                        update_increment
                    } else {
                        diff.total_bytes_completion_increment
                    };
                    let _ = f.call1((increment,))?;
                }

                Ok(())
            })
        })
        .await
        .map_err(convert_multithreading_error)?
    }
}

// === Simple per-file progress callback for downloads ===

/// Send a simple byte-increment update to a Python callback.
pub async fn send_simple_progress(py_func: &Py<PyAny>, increment: u64) {
    if increment == 0 {
        return;
    }
    let py_func = Python::attach(|py| py_func.clone_ref(py));
    let rt = XetRuntime::current();
    let _ = rt
        .spawn_blocking(move || {
            Python::attach(|py| {
                let f = py_func.bind(py);
                let _ = f.call1((increment,));
            })
        })
        .await
        .log_error("Python exception updating download progress:");
}

// === Progress diff types ===

#[derive(Clone, Debug)]
pub struct ItemDiff {
    pub item_name: String,
    pub total_bytes: u64,
    pub bytes_completed: u64,
    pub bytes_completion_increment: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ProgressDiff {
    pub total_bytes: u64,
    pub total_bytes_increment: u64,
    pub total_bytes_completed: u64,
    pub total_bytes_completion_increment: u64,
    pub total_bytes_completion_rate: Option<f64>,
    pub total_transfer_bytes: u64,
    pub total_transfer_bytes_increment: u64,
    pub total_transfer_bytes_completed: u64,
    pub total_transfer_bytes_completion_increment: u64,
    pub total_transfer_bytes_completion_rate: Option<f64>,
    pub item_diffs: Vec<ItemDiff>,
}

impl ProgressDiff {
    pub fn is_empty(&self) -> bool {
        self.total_bytes_increment == 0
            && self.total_bytes_completion_increment == 0
            && self.total_transfer_bytes_increment == 0
            && self.total_transfer_bytes_completion_increment == 0
            && self.item_diffs.is_empty()
    }
}

// === Diff state for group-level progress polling ===

/// Tracks the previous progress snapshot so that incremental diffs can be
/// computed each time the session's `progress()` is polled.
pub struct GroupProgressDiffState {
    prev_group: GroupProgressReport,
    prev_items: HashMap<UniqueID, ItemProgressReport>,
}

impl GroupProgressDiffState {
    pub fn new() -> Self {
        Self {
            prev_group: GroupProgressReport::default(),
            prev_items: HashMap::new(),
        }
    }

    pub fn compute_diff(
        &mut self,
        group: GroupProgressReport,
        items: HashMap<UniqueID, ItemProgressReport>,
    ) -> ProgressDiff {
        let total_bytes_increment = group.total_bytes.saturating_sub(self.prev_group.total_bytes);
        let total_bytes_completion_increment = group
            .total_bytes_completed
            .saturating_sub(self.prev_group.total_bytes_completed);
        let total_transfer_bytes_increment =
            group.total_transfer_bytes.saturating_sub(self.prev_group.total_transfer_bytes);
        let total_transfer_bytes_completion_increment = group
            .total_transfer_bytes_completed
            .saturating_sub(self.prev_group.total_transfer_bytes_completed);

        let mut item_diffs = Vec::new();
        for (&id, report) in &items {
            let prev = self.prev_items.get(&id);
            let prev_completed = prev.map_or(0, |p| p.bytes_completed);
            let increment = report.bytes_completed.saturating_sub(prev_completed);

            if increment > 0 || prev.is_none() {
                item_diffs.push(ItemDiff {
                    item_name: report.item_name.clone(),
                    total_bytes: report.total_bytes,
                    bytes_completed: report.bytes_completed,
                    bytes_completion_increment: increment,
                });
            }
        }

        let diff = ProgressDiff {
            total_bytes: group.total_bytes,
            total_bytes_increment,
            total_bytes_completed: group.total_bytes_completed,
            total_bytes_completion_increment,
            total_bytes_completion_rate: group.total_bytes_completion_rate,
            total_transfer_bytes: group.total_transfer_bytes,
            total_transfer_bytes_increment,
            total_transfer_bytes_completed: group.total_transfer_bytes_completed,
            total_transfer_bytes_completion_increment,
            total_transfer_bytes_completion_rate: group.total_transfer_bytes_completion_rate,
            item_diffs,
        };

        self.prev_group = group;
        self.prev_items = items;

        diff
    }
}
