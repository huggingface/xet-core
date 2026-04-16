use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use lazy_static::lazy_static;
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use tracing::info;
use xet_pkg::XetError;
use xet_runtime::RuntimeError;
use xet_runtime::core::XetRuntime;
use xet_runtime::core::sync_primatives::spawn_os_thread;

lazy_static! {
    static ref SIGINT_DETECTED: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    static ref SIGINT_HANDLER_INSTALL_PID: (AtomicU32, Mutex<()>) = (AtomicU32::new(0), Mutex::new(()));
    static ref MULTITHREADED_RUNTIME: RwLock<Option<(u32, Arc<XetRuntime>)>> = RwLock::new(None);
}

#[cfg(unix)]
fn install_sigint_handler() -> Result<(), RuntimeError> {
    use signal_hook::consts::SIGINT;
    use signal_hook::flag;

    flag::register(SIGINT, SIGINT_DETECTED.clone())
        .map_err(|e| RuntimeError::Other(format!("Initialization Error: Unable to register SIGINT handler {e:?}")))?;

    Ok(())
}

#[cfg(windows)]
extern "system" fn console_ctrl_handler(
    ctrl_type: winapi::shared::minwindef::DWORD,
) -> winapi::shared::minwindef::BOOL {
    use winapi::um::wincon;

    if ctrl_type == wincon::CTRL_C_EVENT {
        SIGINT_DETECTED.store(true, Ordering::SeqCst);
    }

    winapi::shared::minwindef::FALSE
}

#[cfg(windows)]
fn install_sigint_handler() -> Result<(), RuntimeError> {
    use winapi::um::consoleapi::SetConsoleCtrlHandler;

    unsafe {
        if SetConsoleCtrlHandler(Some(console_ctrl_handler), winapi::shared::minwindef::TRUE) == 0 {
            let error = winapi::um::errhandlingapi::GetLastError();
            return Err(RuntimeError::Other(format!(
                "Initialization Error: Unable to register SIGINT handler. Windows error: {error}"
            )));
        }
    }
    Ok(())
}

fn check_sigint_handler() -> Result<(), RuntimeError> {
    SIGINT_DETECTED.store(false, Ordering::SeqCst);

    let stored_pid = SIGINT_HANDLER_INSTALL_PID.0.load(Ordering::SeqCst);
    let pid = std::process::id();

    if stored_pid == pid {
        return Ok(());
    }

    let _install_lg = SIGINT_HANDLER_INSTALL_PID.1.lock().unwrap();

    let stored_pid = SIGINT_HANDLER_INSTALL_PID.0.load(Ordering::SeqCst);
    if stored_pid == pid {
        return Ok(());
    }

    install_sigint_handler()?;
    SIGINT_HANDLER_INSTALL_PID.0.store(pid, Ordering::SeqCst);

    Ok(())
}

pub(crate) fn perform_sigint_shutdown() {
    let maybe_runtime = MULTITHREADED_RUNTIME.write().unwrap().take();

    if let Some((runtime_pid, ref runtime)) = maybe_runtime
        && runtime_pid == std::process::id()
        && runtime.external_executor_count() != 0
    {
        eprintln!("Cancellation requested; stopping current tasks.");
        runtime.perform_sigint_shutdown();
    }
}

fn in_sigint_shutdown() -> bool {
    SIGINT_DETECTED.load(Ordering::Relaxed)
}

fn signal_check_background_loop() {
    const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(250);

    loop {
        std::thread::sleep(SIGNAL_CHECK_INTERVAL);

        if SIGINT_DETECTED.load(Ordering::SeqCst) {
            perform_sigint_shutdown();
            SIGINT_DETECTED.store(false, Ordering::SeqCst);
            break;
        }
    }
}

pub fn init_threadpool() -> Result<Arc<XetRuntime>, RuntimeError> {
    let mut guard = MULTITHREADED_RUNTIME.write().unwrap();
    let pid = std::process::id();

    if let Some((runtime_pid, existing)) = guard.take() {
        if runtime_pid == pid {
            *guard = Some((pid, existing.clone()));
            return Ok(existing);
        } else {
            existing.discard_runtime();
            info!("Runtime restarted due to detected process ID change, likely due to running inside a fork call.");
        }
    }

    let runtime = XetRuntime::new()?;
    check_sigint_handler()?;
    *guard = Some((pid, runtime.clone()));
    std::thread::spawn(signal_check_background_loop);
    drop(guard);

    Ok(runtime)
}

fn get_threadpool() -> Result<Arc<XetRuntime>, RuntimeError> {
    {
        let guard = MULTITHREADED_RUNTIME.read().unwrap();
        if let Some((runtime_pid, ref existing)) = *guard
            && runtime_pid == std::process::id()
        {
            return Ok(existing.clone());
        }
    }

    init_threadpool()
}

pub fn convert_multithreading_error(e: impl Into<RuntimeError>) -> PyErr {
    PyErr::from(XetError::from(e.into()))
}

pub fn async_run<Out, F>(py: Python, execution_call: F) -> PyResult<Out>
where
    F: std::future::Future + Send + 'static,
    F::Output: Into<PyResult<Out>> + Send + Sync,
    Out: Send + Sync + 'static,
{
    let result: PyResult<Out> = py.detach(move || {
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

    if let Err(e) = &result
        && in_sigint_shutdown()
    {
        if cfg!(debug_assertions) {
            eprintln!("[debug] ignored error reported during shutdown: {e:?}");
        }
        return Err(PyKeyboardInterrupt::new_err(()));
    }

    result
}
