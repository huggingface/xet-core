use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use lazy_static::lazy_static;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use tracing::info;
use xet_threadpool::errors::MultithreadedRuntimeError;
use xet_threadpool::sync_primatives::spawn_os_thread;
use xet_threadpool::ThreadPool;

lazy_static! {
    static ref SIGINT_DETECTED: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    static ref SIGINT_HANDLER_INSTALL_PID: (AtomicU32, Mutex<()>) = (AtomicU32::new(0), Mutex::new(()));
    static ref MULTITHREADED_RUNTIME: RwLock<Option<(u32, Arc<ThreadPool>)>> = RwLock::new(None);
}

#[cfg(unix)]
fn install_sigint_handler() -> Result<(), MultithreadedRuntimeError> {
    use signal_hook::consts::SIGINT;
    use signal_hook::flag;

    // Register the SIGINT handler to set our atomic flag.
    // Using `signal_hook::flag::register` allows us to set the atomic flag when SIGINT is received.
    flag::register(SIGINT, SIGINT_DETECTED.clone()).map_err(|e| {
        MultithreadedRuntimeError::Other(format!("Initialization Error: Unable to register SIGINT handler {e:?}"))
    })?;

    Ok(())
}

#[cfg(windows)]
fn install_sigint_handler() -> Result<(), MultithreadedRuntimeError> {
    // On Windows, use ctrlc crate.
    // This sets a callback to run on Ctrl-C:
    let sigint_detected_flag = SIGINT_DETECTED.clone();
    ctrlc::set_handler(move || {
        sigint_detected_flag.store(true, Ordering::SeqCst);
    })
    .map_err(|e| {
        MultithreadedRuntimeError::Other(format!("Initialization Error: Unable to register SIGINT handler {e:?}"))
    })?;
    Ok(())
}

fn check_sigint_handler() -> Result<(), MultithreadedRuntimeError> {
    // Clear the sigint flag.  It is possible but unlikely that there will be a race condition here
    // that will cause a CTRL-C to be temporarily ignored by us.  In such a case, the user
    // will have to press it again.
    SIGINT_DETECTED.store(false, Ordering::SeqCst);

    let stored_pid = SIGINT_HANDLER_INSTALL_PID.0.load(Ordering::SeqCst);
    let pid = std::process::id();

    if stored_pid == pid {
        return Ok(());
    }

    // Need to install it; acquire a lock to do so.
    let _install_lg = SIGINT_HANDLER_INSTALL_PID.1.lock().unwrap();

    // If another thread beat us to it while we're waiting for the lock.
    let stored_pid = SIGINT_HANDLER_INSTALL_PID.0.load(Ordering::SeqCst);
    if stored_pid == pid {
        return Ok(());
    }

    install_sigint_handler()?;

    // Finally, store that we have installed it successfully.
    SIGINT_HANDLER_INSTALL_PID.0.store(pid, Ordering::SeqCst);

    Ok(())
}

pub(crate) fn perform_sigint_shutdown() {
    // Acquire exclusive access to the runtime.  This will only be released once the runtime is shut down,
    // meaning that all the tasks have completed or been cancelled.
    let maybe_runtime = MULTITHREADED_RUNTIME.write().unwrap().take();

    // Shut it down gracefully if we own it in this process.
    if let Some((runtime_pid, ref runtime)) = maybe_runtime {
        // Only do anything with the runtime if we're on the right process.
        // Otherwise, it's none of our business.
        if runtime_pid == std::process::id() && runtime.external_executor_count() != 0 {
            eprintln!("Cancellation requested; stopping current tasks.");
            runtime.perform_sigint_shutdown();
        }
    }
}

fn in_sigint_shutdown() -> bool {
    SIGINT_DETECTED.load(Ordering::Relaxed)
}

fn signal_check_background_loop() {
    const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(250);

    loop {
        std::thread::sleep(SIGNAL_CHECK_INTERVAL);

        let shutdown_runtime = SIGINT_DETECTED.load(Ordering::SeqCst);

        // The keyboard interrupt was raised, so shut down things in a reasonable amount of time and return the runtime
        // to the uninitialized state.
        if shutdown_runtime {
            // Shut this down.
            perform_sigint_shutdown();

            // Clear the flag; we're good to go now.
            SIGINT_DETECTED.store(false, Ordering::SeqCst);

            // Exits this background thread.
            break;
        }
    }
}

// This should be called once on library load.
pub fn init_threadpool() -> Result<Arc<ThreadPool>, MultithreadedRuntimeError> {
    // Need to initialize. Upgrade to write lock.
    let mut guard = MULTITHREADED_RUNTIME.write().unwrap();

    // Has another thread done this already?
    let pid = std::process::id();

    if let Some((runtime_pid, existing)) = guard.take() {
        if runtime_pid == pid {
            // We're OK, so reset it here.
            *guard = Some((pid, existing.clone()));
            return Ok(existing);
        } else {
            // Ok, discard the previous runtime, as it's effectively poisoned by the
            // fork-exec, and we simply need to leak it and restart from scratch.  The memory and
            // resources will be freed up when the child exits.
            existing.discard_runtime();

            info!("Runtime restarted due to detected process ID change, likely due to running inside a fork call.");
        }
    }

    // Create a new Tokio runtime.
    let runtime = ThreadPool::new()?;

    // Check the signal handler.  This must be reinstalled on new or after a spawn
    check_sigint_handler()?;

    // Set the runtime in the global tracker.
    *guard = Some((pid, runtime.clone()));

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

    // Return the runtime
    Ok(runtime)
}

// This function initializes the runtime if not present, otherwise returns the existing one.
fn get_threadpool() -> Result<Arc<ThreadPool>, MultithreadedRuntimeError> {
    // First try a read lock to see if it's already initialized.
    {
        let guard = MULTITHREADED_RUNTIME.read().unwrap();
        if let Some((runtime_pid, ref existing)) = *guard {
            let pid = std::process::id();

            if runtime_pid == pid {
                return Ok(existing.clone());
            }
        }
    }

    // Init and return

    init_threadpool()
}

pub fn convert_multithreading_error(e: impl Into<MultithreadedRuntimeError> + std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Xet Runtime Error: {e}"))
}

pub fn async_run<Out, F>(py: Python, execution_call: F) -> PyResult<Out>
where
    F: std::future::Future + Send + 'static,
    F::Output: Into<PyResult<Out>> + Send + Sync,
    Out: Send + Sync + 'static,
{
    let result: PyResult<Out> = py.allow_threads(move || {
        // Now, without the GIL, spawn the task on a new OS thread.  This avoids having tokio cache stuff in
        // thread-local storage that is invalidated after a fork-exec.
        spawn_os_thread(move || {
            let runtime = get_threadpool().map_err(convert_multithreading_error)?;

            runtime
                .external_run_async_task(execution_call)
                .map_err(convert_multithreading_error)?
                .into()
        })
        .join()
        .map_err(convert_multithreading_error)?
    });

    // Now, if we're in the middle of a shutdown, and this is an error, then
    // just translate that error to a KeyboardInterrupt (or we get a lot of
    if let Err(ref e) = &result {
        if in_sigint_shutdown() {
            if cfg!(debug_assertions) {
                eprintln!("[debug] ignored error reported during shutdown: {e:?}");
            }
            return Err(PyKeyboardInterrupt::new_err(()));
        }
    }

    // Now return the result.
    result
}
