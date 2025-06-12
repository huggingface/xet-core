use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use lazy_static::lazy_static;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use xet_threadpool::errors::MultithreadedRuntimeError;
use xet_threadpool::ThreadPool;

use crate::log;

lazy_static! {
    static ref SIGINT_DETECTED: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    static ref SIGINT_HANDLER_INSTALLED: (AtomicBool, Mutex<()>) = (AtomicBool::new(false), Mutex::new(()));
    static ref MULTITHREADED_RUNTIME: RwLock<Option<Arc<ThreadPool>>> = RwLock::new(None);
}

#[cfg(unix)]
fn install_sigint_handler() -> PyResult<()> {
    use signal_hook::consts::SIGINT;
    use signal_hook::flag;

    // Register the SIGINT handler to set our atomic flag.
    // Using `signal_hook::flag::register` allows us to set the atomic flag when SIGINT is received.
    flag::register(SIGINT, SIGINT_DETECTED.clone()).map_err(|e| {
        PyRuntimeError::new_err(format!("Initialization Error: Unable to register SIGINT handler {e:?}"))
    })?;

    Ok(())
}

#[cfg(windows)]
fn install_sigint_handler() -> PyResult<()> {
    // On Windows, use ctrlc crate.
    // This sets a callback to run on Ctrl-C:
    let sigint_detected_flag = SIGINT_DETECTED.clone();
    ctrlc::set_handler(move || {
        sigint_detected_flag.store(true, Ordering::SeqCst);
    })
    .map_err(|e| PyRuntimeError::new_err(format!("Initialization Error: Unable to register SIGINT handler {e:?}")))?;
    Ok(())
}

fn check_sigint_handler() -> PyResult<()> {
    // Clear the sigint flag.  It is possible but unlikely that there will be a race condition here
    // that will cause a CTRL-C to be temporarily ignored by us.  In such a case, the user
    // will have to press it again.

    SIGINT_DETECTED.store(false, Ordering::SeqCst);

    if SIGINT_HANDLER_INSTALLED.0.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Need to install it; acquire a lock to do so.
    let _install_lg = SIGINT_HANDLER_INSTALLED.1.lock().unwrap();

    // If another thread beat us to it while we're waiting for the lock.
    if SIGINT_HANDLER_INSTALLED.0.load(Ordering::SeqCst) {
        return Ok(());
    }

    install_sigint_handler()?;

    // Finally, store that we have installed it successfully.
    SIGINT_HANDLER_INSTALLED.0.store(true, Ordering::SeqCst);

    Ok(())
}

fn signal_check_background_loop() {
    const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(250);

    loop {
        std::thread::sleep(SIGNAL_CHECK_INTERVAL);

        let shutdown_runtime = SIGINT_DETECTED.load(Ordering::SeqCst);

        // The keyboard interrupt was raised, so shut down things in a reasonable amount of time and return the runtime
        // to the uninitialized state.
        if shutdown_runtime {
            // Acquire exclusive access to the runtime.  This will only be released once the runtime is shut down,
            // meaining that all the tasks have completed or been cancelled.
            let maybe_runtime = MULTITHREADED_RUNTIME.write().unwrap().take();

            if let Some(ref runtime) = maybe_runtime {
                // See if we have anything going on that needs to be shut down.
                if runtime.external_executor_count() != 0 {
                    eprintln!("Cancellation requested; stopping current tasks.");
                    runtime.perform_sigint_shutdown();
                }
            }

            // Clear the flag; we're good to go now.
            SIGINT_DETECTED.store(false, Ordering::SeqCst);

            // Exits this background thread.
            break;
        }
    }
}

pub fn init_threadpool(py: Python) -> PyResult<Arc<ThreadPool>> {
    // Need to initialize. Upgrade to write lock.
    let mut guard = MULTITHREADED_RUNTIME.write().unwrap();

    // Has another thread done this already?
    if let Some(ref existing) = *guard {
        return Ok(existing.clone());
    }

    // Create a new Tokio runtime.
    let runtime = ThreadPool::new().map_err(convert_multithreading_error)?;

    // Check the signal handler
    check_sigint_handler()?;

    // Set the runtime in the global tracker.
    *guard = Some(runtime.clone());

    // Spawn a background non-tokio thread to check the sigint flag.
    std::thread::spawn(signal_check_background_loop);

    // Drop the guard and initialize the logging.
    //
    // We want to drop this first is that multiple threads entering this runtime
    // may cause a deadlock if the thread that has the GIL tries to acquire the runtime,
    // but then the logging expects the GIL in order to initialize it properly.
    //
    // In most cases, this will done on module initialization; however, after CTRL-C, the runtime is
    // initialized lazily and so putting this here avoids the deadlock (and possibly some info! or other
    // error statements may not be sent to python if the other thread continues ahead of the logging
    // being initialized.)
    drop(guard);

    // Initialize the logging
    log::initialize_runtime_logging(py, runtime.clone());

    // Return the runtime
    Ok(runtime)
}

// This function initializes the runtime if not present, otherwise returns the existing one.
fn get_threadpool(py: Python) -> PyResult<Arc<ThreadPool>> {
    // First try a read lock to see if it's already initialized.
    {
        let guard = MULTITHREADED_RUNTIME.read().unwrap();
        if let Some(ref existing) = *guard {
            return Ok(existing.clone());
        }
    }
    // Init and return
    init_threadpool(py)
}

pub fn convert_multithreading_error(e: impl Into<MultithreadedRuntimeError> + std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Xet Runtime Error: {e}"))
}

pub fn async_run<Out, F>(py: Python, execution_call: F) -> PyResult<Out>
where
    F: std::future::Future + Send + 'static,
    F::Output: Into<PyResult<Out>> + Send + Sync,
    Out: Send + Sync,
{
    // Get a handle to the current runtime.
    let runtime = get_threadpool(py)?;

    // Release the gil
    let runtime_internal = runtime.clone();
    let result: PyResult<Out> = py
        .allow_threads(move || runtime_internal.external_run_async_task(execution_call))
        .map_err(convert_multithreading_error)?
        .into();

    // Now, if we're in the middle of a shutdown, and this is an error, then
    // just translate that error to a KeyboardInterrupt (or we get a lot of
    if let Err(ref e) = &result {
        if runtime.in_sigint_shutdown() {
            if cfg!(debug_assertions) {
                eprintln!("[debug] ignored error reported during shutdown: {e:?}");
            }
            return Err(PyKeyboardInterrupt::new_err(()));
        }
    }

    // Now return the result.
    result
}
