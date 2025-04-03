use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioRuntimeHandle, Runtime as TokioRuntime};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::errors::MultithreadedRuntimeError;

const THREADPOOL_NUM_WORKER_THREADS: usize = 4; // 4 active threads
const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet"; // thread names will be hf-xet-0, hf-xet-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size
const THREADPOOL_MAX_BLOCKING_THREADS: usize = 100; // max 100 threads can block IO

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
/// use xet_threadpool::ThreadPool;
///
/// let pool = ThreadPool::new().expect("Error initializing runtime.");
///
/// let result = pool
///     .external_run_async_task(async {
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
///
/// # Functions
///
/// - `new_threadpool`: Creates a new Tokio runtime with the specified settings.

#[derive(Debug)]
pub struct ThreadPool {
    // This has to allow for exclusive access to enable shutdown when
    runtime: std::sync::RwLock<Option<TokioRuntime>>,

    // We use this handle when we actually enter the runtime to avoid the lock.  It is
    // the same as using the runtime, with the exception that it does not block a shutdown
    // while holding a reference to the runtime does.
    handle: TokioRuntimeHandle,

    // The number of external threads calling into this threadpool
    external_executor_count: AtomicUsize,

    // Are we in the middle of a sigint shutdown?
    sigint_shutdown: AtomicBool,
}

impl ThreadPool {
    pub fn new() -> Result<Self, MultithreadedRuntimeError> {
        let runtime = new_threadpool(false)?;
        Ok(Self {
            handle: runtime.handle().clone(),
            runtime: std::sync::RwLock::new(Some(runtime)),
            external_executor_count: AtomicUsize::new(0),
            sigint_shutdown: AtomicBool::new(false),
        })
    }

    /// Use this when already running within a tokio runtime; this simply
    /// pulls in the handle from the current runtime.  Can be used in testing functions
    /// when the runtime needs to be passed in as an argument to current functions.
    pub fn from_current_runtime() -> Arc<Self> {
        Arc::new(Self::from_external(TokioRuntimeHandle::current()))
    }

    pub fn new_with_hardware_parallelism_limit() -> Result<Self, MultithreadedRuntimeError> {
        let runtime = new_threadpool(true)?;
        Ok(Self {
            handle: runtime.handle().clone(),
            runtime: std::sync::RwLock::new(Some(runtime)),
            external_executor_count: AtomicUsize::new(0),
            sigint_shutdown: AtomicBool::new(false),
        })
    }

    pub fn from_external(handle: tokio::runtime::Handle) -> Self {
        Self {
            runtime: std::sync::RwLock::new(None),
            handle,
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
        }
    }

    pub fn num_worker_threads(&self) -> usize {
        self.handle.metrics().num_workers()
    }

    /// Gives the number of concurrent calls to external_run_async_task.
    #[inline]
    pub fn external_executor_count(&self) -> usize {
        self.external_executor_count.load(Ordering::SeqCst)
    }

    /// Cancels and shuts down the runtime.  All tasks currently running will be aborted.
    pub fn perform_sigint_shutdown(&self) {
        // Shut down the tokio
        self.sigint_shutdown.store(true, Ordering::SeqCst);

        if cfg!(debug_assertions) {
            eprintln!("SIGINT detected, shutting down.");
        }

        // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
        // variables are destroyed by running their destructor.
        let maybe_runtime = self.runtime.write().expect("cancel_all called recursively.").take();

        let Some(runtime) = maybe_runtime else {
            eprintln!("WARNING: perform_sigint_shutdown called on runtime that has already been shut down.");
            return;
        };

        // Dropping the runtime will cancel all the tasks; shutdown occurs when the next async call
        // is encountered.  Ideally, all async code should be cancelation safe.
        drop(runtime);
    }

    /// Returns true if we're in the middle of a sigint shutdown,
    /// and false otherwise.
    pub fn in_sigint_shutdown(&self) -> bool {
        self.sigint_shutdown.load(Ordering::SeqCst)
    }

    /// This function should ONLY be used by threads outside of tokio; it should not be called
    /// from within a task running on the runtime worker pool.  Doing so can lead to deadlocking.
    pub fn external_run_async_task<F>(&self, future: F) -> Result<F::Output, MultithreadedRuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + Sync,
    {
        self.external_executor_count.fetch_add(1, Ordering::SeqCst);

        let ret = self.handle.block_on(async move {
            // Run the actual task on a task worker thread so we can get back information
            // on issues, including reporting panics as runtime errors.
            self.handle.spawn(future).await.map_err(MultithreadedRuntimeError::from)
        });

        self.external_executor_count.fetch_sub(1, Ordering::SeqCst);
        ret
    }

    /// Spawn an async task to run in the background on the current pool of worker threads.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // If the runtime has been shut down, this will immediately abort.
        debug!("threadpool: spawn called, {}", self);
        self.handle.spawn(future)
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

impl Display for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Need to be careful that this doesn't acquire locks eagerly, as this function can be called
        // from some weird places like displaying the backtrace of a panic or exception.
        let Ok(runtime_rlg) = self.runtime.try_read() else {
            return write!(f, "Locked Tokio Runtime.");
        };

        let Some(ref runtime) = *runtime_rlg else {
            return write!(f, "Terminated Tokio Runtime Handle; cancel_all_and_shutdown called.");
        };

        let metrics = runtime.metrics();
        write!(
            f,
            "pool: num_workers: {:?}, num_alive_tasks: {:?}, global_queue_depth: {:?}",
            metrics.num_workers(),
            metrics.num_alive_tasks(),
            metrics.global_queue_depth()
        )
    }
}

/// Intended to be used as a singleton threadpool for the entire application.
/// This is a simple wrapper around tokio's runtime, with some default settings.
/// Intentionally unwrap this because if it fails, the application should not continue.
fn new_threadpool(maximum_worker_threads: bool) -> Result<TokioRuntime, MultithreadedRuntimeError> {
    #[cfg(not(target_family = "wasm"))]
    let mut builder = TokioRuntimeBuilder::new_multi_thread();
    #[cfg(target_family = "wasm")]
    let mut builder = TokioRuntimeBuilder::new_current_thread();
    if !maximum_worker_threads {
        builder.worker_threads(THREADPOOL_NUM_WORKER_THREADS); // 4 active threads
    }
    builder
        .thread_name_fn(get_thread_name) // thread names will be hf-xet-0, hf-xet-1, etc.
        .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
        .max_blocking_threads(THREADPOOL_MAX_BLOCKING_THREADS) // max 100 threads can block IO
        .enable_all() // enable all features, including IO/Timer/Signal/Reactor
        .build()
        .map_err(MultithreadedRuntimeError::RuntimeInitializationError)
}

/// gets the name of a new thread for the threadpool. Names are prefixed with
/// `THREADPOOL_THREAD_ID_PREFIX` and suffixed with a global counter:
/// e.g. hf-xet-0, hf-xet-1, hf-xet-2, ...
fn get_thread_name() -> String {
    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
    format!("{THREADPOOL_THREAD_ID_PREFIX}-{id}")
}
