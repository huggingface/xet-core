use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
/// use utils::ThreadPool;
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
use tokio;
use tokio::task::{JoinError, JoinHandle};
use tracing::debug;
use xet_error::Error;

const THREADPOOL_NUM_WORKER_THREADS: usize = 4; // 4 active threads
const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet"; // thread names will be hf-xet-0, hf-xet-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size
const THREADPOOL_MAX_BLOCKING_THREADS: usize = 100; // max 100 threads can block IO
const RUNTIME_SHUTDOWN_WINDOW_MS: u64 = 5 * 1000; // The runtime shuts down in 5 seconds when Ctrl-C is pressed.

/// Define an error time for spawning external threads.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MultithreadedRuntimeError {
    #[error("Error Initializing Multithreaded Runtime: {0:?}")]
    RuntimeInitializationError(std::io::Error),

    #[error("Task Panic: {0:?}.")]
    TaskPanic(JoinError),

    #[error("Task cancelled; possible runtime shutdown in progress {0}.")]
    TaskCanceled(String),

    #[error("Unknown task runtime error: {0}")]
    Other(String),
}

#[derive(Debug)]
pub struct ThreadPool {
    // This has to allow for exclusive access to enable shutdown when
    runtime: std::sync::RwLock<Option<tokio::runtime::Runtime>>,

    // We should use this handle when we actually enter the runtime, which means that during shutdown, spawned tasks
    // and the like are actually
    handle: tokio::runtime::Handle,

    // The number of external threads calling into this threadpool
    external_executor_count: AtomicUsize,
}

impl ThreadPool {
    pub fn new() -> Result<Self, MultithreadedRuntimeError> {
        let runtime = new_threadpool()?;
        Ok(Self {
            handle: runtime.handle().clone(),
            runtime: std::sync::RwLock::new(Some(runtime)),
            external_executor_count: AtomicUsize::new(0),
        })
    }

    /// Gives the number of concurrent calls to external_run_async_task.
    #[inline]
    pub fn external_executor_count(&self) -> usize {
        self.external_executor_count.load(Ordering::SeqCst)
    }

    /// Cancels and shuts down the runtime.  All tasks currently running will be aborted.
    pub fn cancel_all_and_shutdown(&self) {
        // Shutdown with a timeout.  This waits up to 5 seconds for all the running tasks to complete or yield at an
        // await statement, then it drops the worker threads to force shutdown.  This may cause resource
        // leaks, but this is usually used in the context where the larger process needs to be shut down
        // anyway and thus that is okay.

        // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
        // variables are destroyed by running their destructor.
        let maybe_runtime = self.runtime.write().expect("cancel_all called recursively.").take();

        let Some(runtime) = maybe_runtime else {
            eprintln!("WARNING: cancel_all_and_shutdown called on runtime that has already been shut down.");
            return;
        };

        runtime.shutdown_timeout(Duration::from_millis(RUNTIME_SHUTDOWN_WINDOW_MS));
    }

    /// This function should ONLY be used by threads outside of tokio; it should not be called
    /// from within a task running on the runtime worker pool.  Doing so can lead to deadlocking.
    pub fn external_run_async_task<F>(&self, future: F) -> Result<F::Output, MultithreadedRuntimeError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + Sync,
    {
        self.external_executor_count.fetch_add(1, Ordering::SeqCst);

        let ret = self.handle.block_on(async move {
            // Run the actual task on a task worker thread so it can be aborted when
            // it yields by the shutdown process, or that panics can be reported as errors.
            // Processes run on this thread, through block_on, will not be shutdown when the runtime is
            // shut down.

            tokio::spawn(future).await.map_err(|e| {
                if e.is_panic() {
                    // The task panic'd.  Pass this exception on.
                    MultithreadedRuntimeError::TaskPanic(e)
                } else if e.is_cancelled() {
                    // Likely caused by the runtime shutting down (e.g. with a keyboard CTRL-C).
                    MultithreadedRuntimeError::TaskCanceled(format!("{e}"))
                } else {
                    MultithreadedRuntimeError::Other(format!("task join error: {e}"))
                }
            })
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
fn new_threadpool() -> Result<tokio::runtime::Runtime, MultithreadedRuntimeError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(THREADPOOL_NUM_WORKER_THREADS) // 4 active threads
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
    let id = ATOMIC_ID.fetch_add(1, SeqCst);
    format!("{THREADPOOL_THREAD_ID_PREFIX}-{id}")
}
