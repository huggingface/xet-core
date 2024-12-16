use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use lazy_static::lazy_static;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use utils::threadpool::{MultithreadedRuntimeError, ThreadPool};

use crate::log;

lazy_static! {
    static ref PYTHON_SIGNAL_PROPAGATION: Mutex<Option<PyErr>> = Mutex::new(None);
    static ref MULTITHREADED_RUNTIME: RwLock<Option<Arc<ThreadPool>>> = RwLock::new(None);
}

fn signal_check_background_loop(runtime: Arc<ThreadPool>) {
    const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(250);

    'check_loop: loop {
        std::thread::sleep(SIGNAL_CHECK_INTERVAL);

        // Don't do any work if we're not actually executing something at the moment.
        if runtime.external_executor_count() == 0 {
            continue 'check_loop;
        }

        let shutdown_runtime = 'a: {
            // We're executing something, but to ensure that the signal isn't dropped if it's acquired here,
            // lock the signal propegation container while we check for a signal interrupt.   Then other threads
            // will block waiting for this check to complete on their way back into python, ensuring that
            // any signal here will propegate back through the thread execution properly.
            let mut sig_lg = PYTHON_SIGNAL_PROPAGATION.lock().unwrap();

            // If there's already a signal exception waiting to be propegated, then don't bother rechecking.
            if sig_lg.is_some() {
                continue 'check_loop;
            }

            let maybe_signal_exception = Python::with_gil(|py| {
                // Check this again; if there are no threads running currently because they
                // completed while we were waiting to acquire the signal propegation lock or
                // the gil, then we don't want to check anything.

                // The tricky part of the multithreading here is to ensure that this signal handler thread, running in
                // the background, doesn't handle a signal while there are no tasks in process to receive that signal
                // and send it back to python.  Thus only do a check if there is at least one external
                // task being run that can pick up the signal.  A lock on PYTHON_SIGNAL_PROPAGATION here
                // ensures it won't be dropped.
                if runtime.external_executor_count() == 0 {
                    return None;
                }

                match py.check_signals() {
                    Ok(_) => None,
                    Err(e) => Some((e.is_instance_of::<PyKeyboardInterrupt>(py), e)),
                }
            });

            // Is there a  to record?
            if let Some((is_keyboard_interrupt, exception_ref)) = maybe_signal_exception {
                // Always record such a signal here so the execution thread can pick it up and propegate it to python.
                *sig_lg = Some(exception_ref);

                // It's unlikely to be anything else, as another exceptions here would be one raised by a custom
                // signal handler.  Still, worth verifying.
                if is_keyboard_interrupt {
                    // Tell the runtime to shut down.
                    break 'a true;
                }
            }

            false
        };

        // The keyboard interrupt was raised, so shut down things in a reasonable amount of time and return the runtime
        // to the uninitialized state.
        if shutdown_runtime {
            // Acquire exclusive access to the runtime.  This will only be released once the runtime is shut down,
            // meaining that all the tasks have completed or been cancelled.
            let mut runtime_lg = MULTITHREADED_RUNTIME.write().unwrap();

            runtime.cancel_all_and_shutdown();

            // Drop the reference to the runtime so future executors restart it.
            *runtime_lg = None;

            // Exits this background thread.
            break;
        }
    }
}

// This function initializes the runtime if not present, otherwise returns the existing one.
fn get_threadpool() -> Result<Arc<ThreadPool>, MultithreadedRuntimeError> {
    {
        // First try a read lock to see if it's already initialized.
        let guard = MULTITHREADED_RUNTIME.read().unwrap();
        if let Some(ref existing) = *guard {
            return Ok(existing.clone());
        }
    }

    // Need to initialize. Upgrade to write lock.
    let mut guard = MULTITHREADED_RUNTIME.write().unwrap();

    // Has another thread done this already?
    if let Some(ref existing) = *guard {
        return Ok(existing.clone());
    }

    // Create a new Tokio runtime
    let runtime = Arc::new(ThreadPool::new()?);

    // Run any required async init tasks:
    let runtime_logging = runtime.clone();
    runtime.external_run_async_task(async move {
        log::initialize_logging(runtime_logging.clone()); // needs to run within an async runtime
    })?;

    // Set the runtime in the global tracker.
    *guard = Some(runtime.clone());

    // Spawn a background non-tokio thread to check for Python signals.
    let runtime_check_loop = runtime.clone();
    std::thread::spawn(move || signal_check_background_loop(runtime_check_loop));

    // Return the handle to use to run tasks.
    Ok(runtime)
}

fn convert_multithreading_error(e: MultithreadedRuntimeError) -> PyErr {
    PyRuntimeError::new_err(format!("Xet Runtime Error: {}", e))
}

pub fn async_run<Out, F>(py: Python, execution_call: impl FnOnce(Arc<ThreadPool>) -> F + Send) -> PyResult<Out>
where
    F: std::future::Future + Send + 'static,
    F::Output: Into<PyResult<Out>> + Send + Sync,
    Out: Send + Sync,
{
    // Get a handle to the current runtime.
    let runtime = get_threadpool().map_err(convert_multithreading_error)?;

    // Release the gil
    let result: PyResult<Out> = py
        .allow_threads(move || runtime.external_run_async_task(execution_call((&runtime).clone())))
        .map_err(convert_multithreading_error)?
        .into();

    // Now, check to see if there is a signal to propegate.
    let mut sig_lg = PYTHON_SIGNAL_PROPAGATION.lock().unwrap();

    // If there is an exception, take it and send it back to python.
    if let Some(py_exception) = sig_lg.take() {
        return Err(py_exception);
    }

    // Now return the result.
    result
}
