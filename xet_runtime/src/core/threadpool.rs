use std::fmt::Display;
use std::future::Future;
use std::panic::AssertUnwindSafe;
#[cfg(not(target_family = "wasm"))]
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
#[cfg(not(target_family = "wasm"))]
use std::task::{Context, Waker};

use futures::FutureExt;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioRuntimeHandle, Runtime as TokioRuntime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::debug;
#[cfg(not(target_family = "wasm"))]
use tracing::info;

use crate::config::XetConfig;
use crate::error::RuntimeError;
#[cfg(not(target_family = "wasm"))]
use crate::logging::SystemMonitor;
#[cfg(not(target_family = "wasm"))]
use crate::utils::ClosureGuard as CallbackGuard;

const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet"; // thread names will be hf-xet-0, hf-xet-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size

/// Cap the number of tokio threads to 32 to avoid massive expansion on huge CPUs; can be overridden with
/// TOKIO_WORKER_THREADS.
///
/// Note that the compute intensive parts of the code get offloaded to blocking threads, which don't count against this
/// limit.
#[cfg(not(target_family = "wasm"))]
const THREADPOOL_MAX_ASYNC_THREADS: usize = 32;

/// Returns the number of Tokio worker threads to use:
/// 1) If `TOKIO_WORKER_THREADS` is set to a positive integer, use that.
/// 2) Otherwise, use `min(available_parallelism, THREADPOOL_MAX_ASYNC_THREADS)`, with a floor of 2.
#[cfg(not(target_family = "wasm"))]
fn get_num_tokio_worker_threads() -> usize {
    use std::num::NonZeroUsize;

    // Allow TOKIO_WORKER_THREADS to override this value.
    if let Ok(val) = std::env::var("TOKIO_WORKER_THREADS") {
        match val.parse::<usize>() {
            Ok(n) if n > 0 => {
                info!("Using {n} async threads from TOKIO_WORKER_THREADS");
                return n;
            },
            _ => {
                use tracing::warn;

                warn!(
                    value = %val,
                    "Invalid TOKIO_WORKER_THREADS; must be a positive integer. Falling back to auto."
                );
            },
        }
    }

    let cores = std::thread::available_parallelism().map(NonZeroUsize::get).unwrap_or(1);

    // Minimum 2 threads needed to run everything
    let n = cores.clamp(2, THREADPOOL_MAX_ASYNC_THREADS);
    info!("Using {n} async threads for tokio runtime");
    n
}

/// Whether the runtime owns its tokio thread pool or wraps an external handle.
///
/// - **`Owned`**: runtime created its own thread pool. Both async bridging ([`XetThreadpool::bridge_async`]) and sync
///   bridging ([`XetThreadpool::bridge_sync`]) are supported.
///
/// - **`External`**: runtime wraps a caller-provided tokio handle. Async bridging polls the future directly on the
///   caller's executor. Sync bridging ([`XetThreadpool::bridge_sync`]) is rejected with
///   [`RuntimeError::InvalidRuntime`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuntimeMode {
    Owned,
    External,
}

type OwnedRuntimeCell = Arc<std::sync::RwLock<Option<Arc<TokioRuntime>>>>;

#[derive(Debug)]
#[cfg_attr(target_family = "wasm", allow(dead_code))]
enum RuntimeBackend {
    External,
    OwnedThreadPool { runtime: OwnedRuntimeCell },
}

#[cfg(target_family = "wasm")]
struct CallbackGuard<F: FnOnce()> {
    callback: Option<F>,
}

#[cfg(target_family = "wasm")]
impl<F: FnOnce()> CallbackGuard<F> {
    fn new(callback: F) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

#[cfg(target_family = "wasm")]
impl<F: FnOnce()> Drop for CallbackGuard<F> {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

/// This module provides a simple wrapper around Tokio's runtime to create a thread pool
/// with some default settings. It is intended to be used as a singleton thread pool for
/// the entire application.
///
/// The `ThreadPool` struct encapsulates a Tokio runtime and provides methods to run
/// futures to completion, spawn new tasks, and get a handle to the runtime.
///
/// # Example
///
/// ```rust
/// use xet_runtime::core::XetThreadpool;
///
/// let pool = XetThreadpool::new().expect("Error initializing runtime.");
///
/// let result = pool
///     .bridge_sync(async {
///         // Your async code here
///         42
///     })
///     .expect("Task Error.");
///
/// assert_eq!(result, 42);
/// ```
///
/// # Panics
///
/// The `new_threadpool` function will intentionally panic if the Tokio runtime cannot be
/// created. This is because the application should not continue running without a
/// functioning thread pool.
///
/// # Settings
///
/// The thread pool is configured with the following settings:
/// - 4 worker threads
/// - Thread names prefixed with "hf-xet-"
/// - 8MB stack size per thread (default is 2MB)
/// - Maximum of 100 blocking threads
/// - All Tokio features enabled (IO, Timer, Signal, Reactor)
///
/// # Structs
///
/// - `ThreadPool`: The main struct that encapsulates the Tokio runtime.
#[derive(Debug)]
pub struct XetThreadpool {
    // Runtime backend and its owned state (if any).
    backend: RuntimeBackend,

    // We use this handle when we actually enter the runtime to avoid the lock.  It is
    // the same as using the runtime, with the exception that it does not block a shutdown
    // while holding a reference to the runtime does.
    handle_ref: OnceLock<TokioRuntimeHandle>,

    // The number of external threads calling into this threadpool.
    external_executor_count: AtomicUsize,

    // Are we in the middle of a sigint shutdown?
    sigint_shutdown: AtomicBool,

    //  System monitor instance if enabled, monitor starts on initiation
    #[cfg(not(target_family = "wasm"))]
    system_monitor: Option<SystemMonitor>,
}

impl XetThreadpool {
    /// Creates a new owned tokio thread pool with the given configuration.
    pub fn new(config: &XetConfig) -> Result<Arc<Self>, RuntimeError> {
        let runtime = Arc::new(std::sync::RwLock::new(None));

        let rt = Arc::new(Self {
            backend: RuntimeBackend::OwnedThreadPool {
                runtime: runtime.clone(),
            },
            handle_ref: OnceLock::new(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
            #[cfg(not(target_family = "wasm"))]
            system_monitor: config
                .system_monitor
                .enabled
                .then(|| {
                    SystemMonitor::follow_process(
                        config.system_monitor.sample_interval,
                        config.system_monitor.log_path.clone(),
                    )
                    .ok()
                })
                .flatten(),
        });

        let thread_id = AtomicUsize::new(0);
        let get_thread_name = move || {
            let id = thread_id.fetch_add(1, Ordering::Relaxed);
            format!("{THREADPOOL_THREAD_ID_PREFIX}-{id}")
        };

        let mut tokio_rt_builder = {
            #[cfg(not(target_family = "wasm"))]
            {
                TokioRuntimeBuilder::new_multi_thread()
            }

            #[cfg(target_family = "wasm")]
            {
                TokioRuntimeBuilder::new_current_thread()
            }
        };
        #[cfg(not(target_family = "wasm"))]
        {
            tokio_rt_builder.worker_threads(get_num_tokio_worker_threads());
        }

        let tokio_rt = tokio_rt_builder
            .thread_name_fn(get_thread_name)
            .thread_stack_size(THREADPOOL_STACK_SIZE)
            .enable_all()
            .build()
            .map_err(RuntimeError::RuntimeInit)?;

        let handle = tokio_rt.handle().clone();
        let tokio_rt = Arc::new(tokio_rt);
        *runtime.write().unwrap() = Some(tokio_rt);
        rt.handle_ref.set(handle).unwrap();

        Ok(rt)
    }

    /// Wraps an existing tokio handle with a new `XetThreadpool`, using the config for
    /// system monitor setup.
    pub fn from_external_with_config(
        rt_handle: TokioRuntimeHandle,
        config: &XetConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        Ok(Arc::new(Self {
            backend: RuntimeBackend::External,
            handle_ref: rt_handle.into(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
            #[cfg(not(target_family = "wasm"))]
            system_monitor: config
                .system_monitor
                .enabled
                .then(|| {
                    SystemMonitor::follow_process(
                        config.system_monitor.sample_interval,
                        config.system_monitor.log_path.clone(),
                    )
                    .ok()
                })
                .flatten(),
        }))
    }

    /// Wraps an existing tokio handle without system monitoring.
    pub fn from_external(rt_handle: TokioRuntimeHandle) -> Arc<Self> {
        Arc::new(Self {
            backend: RuntimeBackend::External,
            handle_ref: rt_handle.into(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
            #[cfg(not(target_family = "wasm"))]
            system_monitor: None,
        })
    }

    #[inline]
    pub fn handle(&self) -> TokioRuntimeHandle {
        self.handle_ref.get().expect("Not initialized with handle set.").clone()
    }

    #[inline]
    pub fn num_worker_threads(&self) -> usize {
        self.handle().metrics().num_workers()
    }

    /// Gives the number of concurrent sync bridge callers (`external_run_async_task` and `bridge_sync`).
    #[inline]
    pub fn external_executor_count(&self) -> usize {
        self.external_executor_count.load(Ordering::SeqCst)
    }

    /// Cancels and shuts down the runtime.  All tasks currently running will be aborted.
    ///
    /// A concurrent [`bridge_sync`](Self::bridge_sync) or in-flight
    /// [`bridge_async`](Self::bridge_async) may still hold a cloned `Arc` to the tokio runtime
    /// until that call returns, so teardown of the reactor may complete only after those finish.
    pub fn perform_sigint_shutdown(&self) {
        // Shut down the tokio
        self.sigint_shutdown.store(true, Ordering::SeqCst);

        if cfg!(debug_assertions) {
            eprintln!("SIGINT detected, shutting down.");
        }

        // External mode wraps a caller-owned runtime and has no owned runtime to tear down.
        let Some(runtime_cell) = self.runtime_cell_if_owned() else {
            #[cfg(not(target_family = "wasm"))]
            if let Some(monitor) = &self.system_monitor {
                let _ = monitor.stop();
            }
            return;
        };

        // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
        // variables are destroyed by running their destructor.
        let maybe_runtime = runtime_cell.write().expect("cancel_all called recursively.").take();

        let Some(runtime) = maybe_runtime else {
            eprintln!("WARNING: perform_sigint_shutdown called on runtime that has already been shut down.");
            #[cfg(not(target_family = "wasm"))]
            if let Some(monitor) = &self.system_monitor {
                let _ = monitor.stop();
            }
            return;
        };

        // Dropping the runtime will cancel all the tasks; shutdown occurs when the next async call
        // is encountered.  Ideally, all async code should be cancellation safe.
        drop(runtime);

        // Stops the system monitor loop if there is one running.
        #[cfg(not(target_family = "wasm"))]
        if let Some(monitor) = &self.system_monitor {
            let _ = monitor.stop();
        }
    }

    /// Discards the runtime without shutdown; to be used after fork-exec or spawn.
    pub fn discard_runtime(&self) {
        // This function makes a best effort attempt to clean everything up.

        let Some(runtime_cell) = self.runtime_cell_if_owned() else {
            return;
        };

        // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
        // variables are destroyed by running their destructor.
        //
        // If this call fails, then it means that there is a recursive call to this runtime, or that
        // this process is in the middle of a shutdown, so we can ignore it silently.
        let Ok(mut rt_lock) = runtime_cell.write() else {
            return;
        };

        let Some(runtime) = rt_lock.take() else {
            return;
        };

        // In this context, we actually want to simply leak the runtime, as doing anything with it will
        // likely cause a deadlock.  The memory will be reaped when the child process returns, and it's
        // likely in the copy-on-write state anyway.
        std::mem::forget(runtime);
    }

    /// Returns true if we're in the middle of a sigint shutdown,
    /// and false otherwise.
    pub fn in_sigint_shutdown(&self) -> bool {
        self.sigint_shutdown.load(Ordering::SeqCst)
    }

    fn check_sigint(&self) -> Result<(), RuntimeError> {
        if self.in_sigint_shutdown() {
            Err(RuntimeError::KeyboardInterrupt)
        } else {
            Ok(())
        }
    }

    /// This function should ONLY be used by threads outside of tokio; it should not be called
    /// from within a task running on the runtime worker pool.  Doing so can lead to deadlocking.
    pub fn external_run_async_task<F>(&self, future: F) -> Result<F::Output, RuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.external_executor_count.fetch_add(1, Ordering::SeqCst);
        let _executor_count_guard = CallbackGuard::new(|| {
            self.external_executor_count.fetch_sub(1, Ordering::SeqCst);
        });

        self.handle().block_on(async move {
            // Run the actual task on a task worker thread so we can get back information
            // on issues, including reporting panics as runtime errors.
            self.handle().spawn(future).await.map_err(RuntimeError::from)
        })
    }

    /// Spawn an async task to run in the background on the current pool of worker threads.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // If the runtime has been shut down, this will immediately abort.
        debug!("threadpool: spawn called, {}", self);
        self.handle().spawn(future)
    }

    /// Run a future on the appropriate runtime for this `XetThreadpool`.
    ///
    /// - **External mode**: the future is awaited directly on the caller's executor.
    /// - **Owned mode**: the future is spawned onto the owned thread pool and the result is delivered via a `oneshot`
    ///   channel (compatible with any executor).
    ///
    /// This is the primary async entry point. Session-level async methods should call
    /// `self.runtime.bridge_async(...)`.
    pub async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, RuntimeError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.check_sigint()?;
        match &self.backend {
            RuntimeBackend::External => Ok(fut.await),
            RuntimeBackend::OwnedThreadPool { .. } => self.bridge_to_owned(task_name, fut).await,
        }
    }

    /// Run an async future synchronously, blocking the calling thread until completion.
    ///
    /// Only supported on **Owned** runtimes. Returns
    /// [`RuntimeError::InvalidRuntime`] when called on an External-mode runtime.
    ///
    /// The caller must **not** be on a tokio worker thread (calling from
    /// `spawn_blocking` threads, OS threads, or the main thread is fine).
    ///
    /// This is the primary sync entry point. Session-level `_blocking` methods
    /// should simply call `self.runtime.bridge_sync(...)`.
    pub fn bridge_sync<F>(&self, future: F) -> Result<F::Output, RuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.check_sigint()?;
        if matches!(self.backend, RuntimeBackend::External) {
            return Err(RuntimeError::InvalidRuntime(
                "bridge_sync() cannot be called on an External-mode runtime; \
                 use the async API instead"
                    .into(),
            ));
        }

        self.external_executor_count.fetch_add(1, Ordering::SeqCst);
        let _executor_count_guard = CallbackGuard::new(|| {
            self.external_executor_count.fetch_sub(1, Ordering::SeqCst);
        });

        let spawn_handle = self.handle();
        self.handle()
            .block_on(async move { spawn_handle.spawn(future).await.map_err(RuntimeError::from) })
    }

    /// Bridge a future onto this runtime's `hf-xet-*` thread pool from any async context,
    /// including non-tokio executors (smol, async-std, `futures::executor::block_on`).
    ///
    /// Unlike [`bridge_sync`](Self::bridge_sync) which **blocks** the calling thread,
    /// this method returns a future that any executor can poll.
    /// The result is delivered via a `tokio::sync::oneshot` channel whose receiver only
    /// requires a `std::task::Waker`, making it compatible with every standard executor.
    ///
    /// Returns `Err(RuntimeError::TaskPanic)` if the spawned future panics, or
    /// `Err(RuntimeError::TaskCanceled)` if the runtime shuts down before the result
    /// can be delivered.
    async fn bridge_to_owned<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, RuntimeError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.spawn(async move {
            let result = AssertUnwindSafe(fut).catch_unwind().await;
            let _ = tx.send(result);
        });
        match rx.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(panic_payload)) => {
                let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    format!("{task_name}: {s}")
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    format!("{task_name}: {s}")
                } else {
                    format!("{task_name}: <unknown panic>")
                };
                Err(RuntimeError::TaskPanic(msg))
            },
            Err(_) => Err(RuntimeError::TaskCanceled(task_name.to_string())),
        }
    }

    #[inline]
    fn runtime_cell_if_owned(&self) -> Option<&OwnedRuntimeCell> {
        match &self.backend {
            RuntimeBackend::OwnedThreadPool { runtime } => Some(runtime),
            RuntimeBackend::External => None,
        }
    }

    /// Spawn a blocking task on the runtime's blocking thread pool.
    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle().spawn_blocking(f)
    }

    /// Returns the runtime mode (Owned or External).
    #[inline]
    pub fn mode(&self) -> RuntimeMode {
        match &self.backend {
            RuntimeBackend::External => RuntimeMode::External,
            RuntimeBackend::OwnedThreadPool { .. } => RuntimeMode::Owned,
        }
    }

    /// Wraps a caller-provided tokio handle after validating that it meets requirements.
    ///
    /// Not available on WASM targets.
    #[cfg(not(target_family = "wasm"))]
    pub fn from_validated_external(
        rt_handle: TokioRuntimeHandle,
        config: &XetConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        if !Self::handle_meets_requirements(&rt_handle) {
            return Err(RuntimeError::InvalidRuntime(
                "supplied tokio handle does not meet requirements \
                 (missing drivers or wrong flavor)"
                    .into(),
            ));
        }
        Self::from_external_with_config(rt_handle, config)
    }

    /// Probe whether a tokio runtime handle meets the requirements for use as an
    /// External-mode runtime.
    ///
    /// Checks:
    /// 1. **Multi-threaded flavor**.
    /// 2. **Time driver** -- required for timeouts, retry backoff, and progress intervals.
    /// 3. **IO driver** -- required for all network I/O via `reqwest`/`hyper`.
    ///
    /// Driver availability is probed by entering the handle's context and polling a
    /// driver-dependent future once inside `catch_unwind`. Tokio panics synchronously
    /// on the first poll when a driver is absent, so the result is immediate.
    ///
    /// **Fragility note:** this probing technique relies on tokio panicking
    /// synchronously on the first poll of `tokio::time::sleep` /
    /// `tokio::net::TcpListener::bind` when the corresponding driver is absent.
    /// This is undocumented internal behavior validated against tokio 1.x.
    ///
    /// On WASM targets the only available flavor is `current_thread` and
    /// there are no separate IO/time drivers to probe, so any handle is accepted.
    #[cfg(target_family = "wasm")]
    pub fn handle_meets_requirements(_handle: &TokioRuntimeHandle) -> bool {
        true
    }

    /// Not available on WASM targets (WASM always uses `current_thread`).
    #[cfg(not(target_family = "wasm"))]
    pub fn handle_meets_requirements(handle: &TokioRuntimeHandle) -> bool {
        if matches!(handle.runtime_flavor(), tokio::runtime::RuntimeFlavor::CurrentThread) {
            return false;
        }

        let _guard = handle.enter();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let has_time = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut sleep = pin!(tokio::time::sleep(std::time::Duration::ZERO));
            let _ = sleep.as_mut().poll(&mut cx);
        }))
        .is_ok();

        let has_io = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut bind = pin!(tokio::net::TcpListener::bind("127.0.0.1:0"));
            let _ = bind.as_mut().poll(&mut cx);
        }))
        .is_ok();

        has_time && has_io
    }
}

impl Display for XetThreadpool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = match &self.backend {
            RuntimeBackend::External => self.handle().metrics(),
            RuntimeBackend::OwnedThreadPool { runtime } => {
                // Need to be careful that this doesn't acquire locks eagerly, as this function can be called
                // from some weird places like displaying the backtrace of a panic or exception.
                let Ok(runtime_rlg) = runtime.try_read() else {
                    return write!(f, "Locked Tokio Runtime.");
                };

                let Some(ref runtime) = *runtime_rlg else {
                    return write!(f, "Terminated Tokio Runtime Handle; cancel_all_and_shutdown called.");
                };
                runtime.metrics()
            },
        };

        write!(
            f,
            "pool: num_workers: {:?}, num_alive_tasks: {:?}, global_queue_depth: {:?}",
            metrics.num_workers(),
            metrics.num_alive_tasks(),
            metrics.global_queue_depth()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::XetRuntime;

    #[test]
    fn test_get_or_create_reqwest_client_returns_client() {
        let ctx = XetRuntime::default().unwrap();
        let result = ctx.get_or_create_reqwest_client("test".to_string(), || reqwest::Client::builder().build());
        assert!(result.is_ok());
    }

    #[test]
    fn test_bridge_async_owned_mode_runs_on_pool() {
        let ctx = XetRuntime::default().unwrap();
        assert_eq!(ctx.threadpool.mode(), RuntimeMode::Owned);
        let rt = ctx.threadpool.clone();
        let result = ctx
            .threadpool
            .bridge_sync(async move { rt.bridge_async("test", async { 42 }).await.unwrap() });
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_bridge_async_external_mode_runs_directly() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let config = XetConfig::new();
        let rt = XetThreadpool::from_external_with_config(tokio_rt.handle().clone(), &config).unwrap();
        let ctx = XetRuntime::new(rt, config);
        assert_eq!(ctx.threadpool.mode(), RuntimeMode::External);

        let result = tokio_rt.block_on(async { ctx.threadpool.bridge_async("test", async { 99 }).await.unwrap() });
        assert_eq!(result, 99);
    }

    #[test]
    fn test_bridge_sync_owned_mode() {
        let ctx = XetRuntime::default().unwrap();
        assert_eq!(ctx.threadpool.mode(), RuntimeMode::Owned);
        let result = ctx.threadpool.bridge_sync(async { 123 }).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn test_bridge_sync_from_spawn_blocking_owned_mode() {
        let ctx = XetRuntime::default().unwrap();
        let rt = ctx.threadpool.clone();
        let rt2 = ctx.threadpool.clone();
        let jh = rt.spawn_blocking(move || rt2.bridge_sync(async { 456 }).unwrap());
        let result = ctx.threadpool.bridge_sync(async { jh.await.unwrap() }).unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn test_bridge_sync_external_mode_returns_error() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let config = XetConfig::new();
        let rt = XetThreadpool::from_external_with_config(tokio_rt.handle().clone(), &config).unwrap();
        let ctx = XetRuntime::new(rt, config);
        assert_eq!(ctx.threadpool.mode(), RuntimeMode::External);

        let result = ctx.threadpool.bridge_sync(async { 789 });
        assert!(matches!(result, Err(RuntimeError::InvalidRuntime(_))));
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_handle_meets_requirements_multi_thread_all() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        assert!(XetThreadpool::handle_meets_requirements(rt.handle()));
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_handle_meets_requirements_current_thread_rejected() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        assert!(!XetThreadpool::handle_meets_requirements(rt.handle()));
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_handle_meets_requirements_no_drivers_rejected() {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        assert!(!XetThreadpool::handle_meets_requirements(rt.handle()));
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_from_validated_external_accepts_valid_handle() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let config = XetConfig::new();
        let rt = XetThreadpool::from_validated_external(tokio_rt.handle().clone(), &config).unwrap();
        assert_eq!(rt.mode(), RuntimeMode::External);
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_from_validated_external_rejects_current_thread_runtime() {
        let tokio_rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let config = XetConfig::new();
        let result = XetThreadpool::from_validated_external(tokio_rt.handle().clone(), &config);
        assert!(matches!(result, Err(RuntimeError::InvalidRuntime(_))));
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn test_from_validated_external_rejects_runtime_without_drivers() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let config = XetConfig::new();
        let result = XetThreadpool::from_validated_external(tokio_rt.handle().clone(), &config);
        assert!(matches!(result, Err(RuntimeError::InvalidRuntime(_))));
    }

    #[test]
    fn test_bridge_async_owned_mode_catches_panic() {
        let ctx = XetRuntime::default().unwrap();
        let rt = ctx.threadpool.clone();
        let result = ctx.threadpool.bridge_sync(async move {
            rt.bridge_async("panic_test", async {
                panic!("intentional test panic");
            })
            .await
        });
        let err = result.unwrap().unwrap_err();
        assert!(matches!(err, RuntimeError::TaskPanic(_)));
    }

    #[test]
    fn test_context_config_preserved_through_external() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let mut config = XetConfig::new();
        config.data.default_cas_endpoint = "https://test-endpoint.example.com".into();
        let rt = XetThreadpool::from_external(tokio_rt.handle().clone());
        let ctx = XetRuntime::new(rt, config);
        assert_eq!(ctx.config.data.default_cas_endpoint, "https://test-endpoint.example.com");
    }

    #[test]
    fn test_check_sigint_shutdown_not_triggered() {
        let ctx = XetRuntime::default().unwrap();
        assert!(ctx.check_sigint_shutdown().is_ok());
    }
}
