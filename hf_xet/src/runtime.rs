use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use data::{data_client, PointerFile};
use lazy_static::lazy_static;
use pyo3::exceptions::{PyException, PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::pyfunction;
use token_refresh::WrappedTokenRefresher;
use tokio::runtime::Runtime;
use utils::auth::TokenRefresher;

const RUNTIME_SHUTDOWN_WINDOW_MS: u64 = 5 * 1000; // The runtime shuts down in 5 seconds when Ctrl-C is pressed.

lazy_static! {
    static ref PYTHON_SIGNAL_PROPAGATION: Mutex<Option<PyErr>> = Mutex::new(None);
}

lazy_static! {
    static ref TOKIO_RUNTIME: RwLock<Option<Runtime>> = RwLock::new(None);
    static ref WITHIN_RUNTIME_COUNT: AtomicU64 = AtomicU64::new(0);
}

// A helper function to check Python signals within the GIL.
fn check_python_signals(py: Python) -> PyResult<()> {
    py.check_signals()?;
    Ok(())
}

fn signal_check_background_loop() {
    let sleep_duration = Duration::from_millis(250);

    'check_loop: loop {
        std::thread::sleep(sleep_duration);

        // Don't do any work if we're not actually executing something.
        if WITHIN_RUNTIME_COUNT.load(Ordering::SeqCst) == 0 {
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
                if WITHIN_RUNTIME_COUNT.load(Ordering::SeqCst) == 0 {
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
            // Shutdown with a timeout.  This waits up to 5 seconds for all the running tasks to complete or yield at an
            // await statement, then it drops the worker threads to force shutdown.  This may cause resource
            // leaks, but this is usually used in the context where the larger process needs to be shut down
            // anyway and thus that is okay.

            // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
            // variables are destroyed by running their destructor.

            // Acquire exclusive access to the runtime.  This will only be released once the runtime is shut down,
            // meaining that all the tasks have completed or been cancelled.
            let mut guard = TOKIO_RUNTIME.write().unwrap();

            // Shutdown the runtime, replacing the runtime with None.  Any future calls will spawn a new runtime.
            if let Some(runtime) = guard.take() {
                runtime.shutdown_timeout(Duration::from_millis(RUNTIME_SHUTDOWN_WINDOW_MS));
            }

            // This exits the background thread.
            break;
        }
    }
}

// This function initializes the runtime if not present, otherwise returns the existing one.
fn get_runtime_handle() -> tokio::runtime::Handle {
    {
        // First try a read lock to see if it's already initialized.
        let guard = TOKIO_RUNTIME.read().unwrap();
        if let Some(ref existing) = *guard {
            return existing.handle().clone();
        }
    }

    // Need to initialize. Upgrade to write lock.
    let mut guard = TOKIO_RUNTIME.write().unwrap();

    // Has another thread done this already?
    if let Some(ref existing) = *guard {
        return existing.handle().clone();
    }

    // Create a new Tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create multithreaded runtime.");

    // Run any required async init tasks:
    runtime.block_on(async {
        log::initialize_logging(runtime.handle()); // needs to run within an async runtime
    });

    // Get a handle to return to use to spawn tasks.
    let handle = runtime.handle().clone();

    // Set the runtime.
    *guard = Some(runtime);

    // Spawn a background non-tokio thread to check for Python signals.
    std::thread::spawn(move || signal_check_background_loop());

    // Return the handle to use to run tasks.
    handle
}

pub fn async_run<Out, E, F>(py: Python, execution_call: F) -> PyResult<Out>
where
    E: Debug + Send + Sync,
    F: std::future::Future + Send + Sync + 'static,
    F::Output: Into<Result<Out, E>> + Send + Sync,
    Out: Send + Sync,
{
    // Get a handle to the current runtime.
    let runtime_handle: tokio::runtime::Handle = get_runtime_handle();

    // First note that we're in here
    WITHIN_RUNTIME_COUNT.fetch_add(1, Ordering::SeqCst);

    // Release the gil
    let result = py.allow_threads(move || {
        let result: PyResult<_> = runtime_handle.block_on(async move {
            // Run the actual task on the task worker thread so it can be aborted when
            // it yields by the shutdown process.  Processes on this thread will not abort.
            let jh = tokio::spawn(execution_call);

            match jh.await {
                Err(e) => {
                    if e.is_panic() {
                        // The task paniced.  Report with backtrace.
                        Err(PyRuntimeError::new_err(format!("task panic: {e:?}")))
                    } else if e.is_cancelled() {
                        // Likely caused by a keyboard interrupt, but this will get checked a level above if so.
                        Err(PyRuntimeError::new_err(format!("task cancelled: {e}")))
                    } else {
                        Err(PyRuntimeError::new_err(format!("task join error: {e}")))
                    }
                },
                Ok(r) => {
                    let ret: Result<Out, E> = r.into();
                    ret.map_err(|e| PyException::new_err(format!("{e:?}")))
                },
            }
        });

        // The tricky part of the multithreading here is to ensure that the signal handler thread in
        // the background doesn't handle a signal while there are no tasks in process to receive that signal and
        // send it back to python.  When WITHIN_RUNTIME_COUNT is zero, then no checks are done, and this check is done
        // in the signal handling loop while holding a lock on PYTHON_SIGNAL_PROPAGATION.  Thus decrementing this here
        // and then acquiring a lock on PYTHON_SIGNAL_PROPAGATION ensures that the loop will only check for a signal
        // when there is still a thread here that will check PYTHON_SIGNAL_PROPAGATION for an exception and propegate
        // it.
        WITHIN_RUNTIME_COUNT.fetch_sub(1, Ordering::SeqCst);

        // Now, check to see if there is a signal to propegate.
        let mut sig_lg = PYTHON_SIGNAL_PROPAGATION.lock().unwrap();

        // If there is an exception, take it so that other threads don't.
        if let Some(py_exception) = sig_lg.take() {
            return Err(py_exception);
        }

        // Now return the result.
        result
    });

    result
}
