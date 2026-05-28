//! Background progress callbacks for upload commits and download groups.
//!
//! A dedicated thread polls Rust-side progress atomics and invokes a Python
//! callable via [`Python::try_attach`].  The poller sleeps on a condvar so
//! [`BackgroundProgress::stop_and_join`] can wake it immediately instead of
//! waiting for the full poll interval.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use xet_pkg::xet_session::{GroupProgressReport, ItemProgressReport, UniqueId};

fn lock_poisoned_err() -> PyErr {
    PyRuntimeError::new_err("progress poller lock poisoned")
}

fn poller_panicked_err() -> PyErr {
    PyRuntimeError::new_err("progress poller thread panicked")
}

/// Background thread that polls progress and invokes a Python callback.
pub(crate) struct BackgroundProgress {
    /// ``false`` while the poller is running; set to ``true`` in ``stop_and_join``.
    /// Paired with the condvar so setting shutdown wakes a thread blocked in
    /// ``wait_timeout`` (the mutex is released for the duration of the wait).
    wake: Arc<(Mutex<bool>, Condvar)>,
    join: Mutex<Option<JoinHandle<PyResult<()>>>>,
    callback: Py<PyAny>,
}

impl BackgroundProgress {
    /// Spawn a thread that calls ``snapshot`` every ``interval_ms`` until shutdown,
    /// the snapshot reports a terminal task state, or the callback fails.
    pub(crate) fn spawn<F>(py: Python<'_>, callback: Py<PyAny>, interval_ms: u64, mut snapshot: F) -> Self
    where
        F: FnMut() -> (GroupProgressReport, HashMap<UniqueId, ItemProgressReport>, bool) + Send + 'static,
    {
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let wake_for_thread = Arc::clone(&wake);
        let callback_for_thread = callback.clone_ref(py);
        let interval = Duration::from_millis(interval_ms);

        let join = std::thread::spawn(move || {
            progress_thread_loop(callback_for_thread, interval, &wake_for_thread, &mut snapshot)
        });

        Self {
            wake,
            join: Mutex::new(Some(join)),
            callback,
        }
    }

    /// Request shutdown, wake the poller if it is sleeping, and block until the thread exits.
    pub(crate) fn stop_and_join(&self, py: Python<'_>) -> PyResult<()> {
        {
            let mut guard = self.wake.0.lock().map_err(|_| lock_poisoned_err())?;
            *guard = true;
        }
        self.wake.1.notify_one();
        let handle = self.join.lock().map_err(|_| lock_poisoned_err())?.take();
        // Releases the GIL while joining so the poller can acquire it inside
        // [`Python::try_attach`] without deadlocking the caller.
        py.detach(|| {
            if let Some(handle) = handle {
                match handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(poller_panicked_err()),
                }
            } else {
                Ok(())
            }
        })
    }

    /// Stop the poller, then invoke the callback once on the calling thread (final snapshot).
    pub(crate) fn stop_and_emit<F>(&self, py: Python<'_>, snapshot: F) -> PyResult<()>
    where
        F: FnOnce() -> (GroupProgressReport, HashMap<UniqueId, ItemProgressReport>),
    {
        self.stop_and_join(py)?;
        let (group_report, item_reports) = snapshot();
        self.callback.call1(py, (group_report, item_reports))?;
        Ok(())
    }
}

fn progress_thread_loop<F>(
    callback: Py<PyAny>,
    interval: Duration,
    wake: &Arc<(Mutex<bool>, Condvar)>,
    snapshot: &mut F,
) -> PyResult<()>
where
    F: FnMut() -> (GroupProgressReport, HashMap<UniqueId, ItemProgressReport>, bool),
{
    loop {
        // Sleep up to `interval`, but return promptly when shutdown is already set or notified.
        let shutdown = {
            let guard = wake.0.lock().map_err(|_| lock_poisoned_err())?;
            let (guard, _) = wake
                .1
                .wait_timeout_while(guard, interval, |shutdown| !*shutdown)
                .map_err(|_| lock_poisoned_err())?;
            *guard
        };

        if shutdown {
            break;
        }

        // Mutex is not held across snapshot or the Python callback.
        let (group_report, item_reports, is_terminal) = snapshot();

        match Python::try_attach(|py| callback.call1(py, (group_report, item_reports))) {
            None => break, // interpreter shutting down
            Some(Ok(_)) => {},
            Some(Err(e)) => {
                let _ = Python::try_attach(|py| e.print(py));
                break;
            },
        }

        if is_terminal {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use pyo3::Python;
    use pyo3::ffi::c_str;
    use xet_pkg::xet_session::GroupProgressReport;

    use super::*;

    #[test]
    fn stop_and_join_returns_before_poll_interval_elapses() {
        /// Poll interval long enough that a blocking sleep would fail the test.
        const LONG_INTERVAL_MS: u64 = 60_000;
        const STOP_DEADLINE: Duration = Duration::from_secs(2);

        Python::attach(|py| -> PyResult<()> {
            py.run(c_str!("def _bg_progress_test_cb(_group, _items): pass"), None, None)?;
            let callback: Py<PyAny> = py.eval(c_str!("_bg_progress_test_cb"), None, None)?.unbind();

            let progress = BackgroundProgress::spawn(py, callback, LONG_INTERVAL_MS, || {
                (GroupProgressReport::default(), HashMap::new(), false)
            });

            let start = Instant::now();
            progress.stop_and_join(py)?;
            let elapsed = start.elapsed();

            assert!(
                elapsed < STOP_DEADLINE,
                "stop_and_join took {elapsed:?}; expected prompt return (poll interval is {LONG_INTERVAL_MS}ms)"
            );
            Ok(())
        })
        .unwrap();
    }
}
