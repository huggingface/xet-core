use std::fmt::Display;
use std::future::Future;

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
/// let pool = ThreadPool::new();
///
/// pool.spawn(async {
///     // Your async code here
/// });
///
/// let result = pool.block_on(async {
///     // Your async code here
///     42
/// });
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
/// - Thread names prefixed with "hf_xet-"
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
use tokio::{self, task::JoinHandle};
use tracing::info;

const THREADPOOL_NUM_WORKER_THREADS: usize = 4; // 4 active threads
const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet-"; // thread names will be hf_xet-1, hf_xet-2, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size
const THREADPOOL_MAX_BLOCKING_THREADS: usize = 100; // max 100 threads can block IO

#[derive(Debug)]
pub struct ThreadPool {
    inner: tokio::runtime::Runtime,
}

impl Default for ThreadPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPool {
    pub fn new() -> Self {
        Self {
            inner: new_threadpool(),
        }
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        info!("threadpool: block_on called, {}", self);
        self.inner.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        info!("threadpool: spawn called, {}", self);
        self.inner.spawn(future)
    }

    pub fn get_handle(&self) -> tokio::runtime::Handle {
        self.inner.handle().clone()
    }
}

impl Display for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = self.inner.metrics();
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
fn new_threadpool() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(THREADPOOL_NUM_WORKER_THREADS) // 4 active threads
        .thread_name(THREADPOOL_THREAD_ID_PREFIX) // thread names will be hf_xet-1, hf_xet-2, etc.
        .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
        .max_blocking_threads(THREADPOOL_MAX_BLOCKING_THREADS) // max 100 threads can block IO
        .enable_all() // enable all features, including IO/Timer/Signal/Reactor
        .build()
        .unwrap()
}
