use std::cell::RefCell;
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioRuntimeHandle, Runtime as TokioRuntime};

use crate::errors::MultithreadedRuntimeError;

const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet"; // thread names will be hf-xet-0, hf-xet-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size
const THREADPOOL_MAX_BLOCKING_THREADS: usize = 100; // max 100 threads can block IO

const THREADPOOL_NUM_IO_WORKERS: usize = 2; // Number of worker threads used to handle the IO.

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
#[derive(Debug)]
pub struct ThreadPool {
    // The runtime used when
    runtime: std::sync::RwLock<Option<TPRuntimes>>,

    // We use this handle when we actually enter the runtime to avoid the lock.  It is
    // the same as using the runtime, with the exception that it does not block a shutdown
    // while holding a reference to the runtime does.
    handle_ref: OnceLock<TPHandles>,

    // The number of external threads calling into this threadpool
    external_executor_count: AtomicUsize,

    // Are we in the middle of a sigint shutdown?
    sigint_shutdown: AtomicBool,
}

#[derive(Debug)]
#[cfg(not(target_family = "wasm"))]
struct TPRuntimes {
    io_runtime: TokioRuntime,
    compute_runtime: TokioRuntime,
}

#[derive(Debug)]
#[cfg(target_family = "wasm")]
struct TPRuntimes {
    runtime: TokioRuntime,
}

#[derive(Debug)]
struct TPHandles {
    io_handle: TokioRuntimeHandle,
    compute_handle: TokioRuntimeHandle,
}

// Use thread-local references to the runtime that are set on initilization among all
// the worker threads in the runtime.  This way, XetRuntime::current() will always refer to
// the runtime active with that worker thread.
thread_local! {
    static THREAD_RUNTIME_REF: RefCell<Option<Arc<ThreadPool>>> = const { RefCell::new(None) };
}

impl ThreadPool {
    /// Return the current threadpool that the current worker thread uses.  Will fail if  
    /// called from a thread that is not spawned from the current runtime.  
    #[inline]
    pub fn current() -> Arc<Self> {
        let maybe_rt = THREAD_RUNTIME_REF.with_borrow(|rt| rt.clone());

        if let Some(rt) = maybe_rt {
            rt
        } else {
            let Ok(tokio_rt) = TokioRuntimeHandle::try_current() else {
                panic!(
                    "ThreadPool::current() called before ThreadPool::new() or on thread outside of current runtime."
                );
            };

            Self::from_external(tokio_rt)
        }
    }

    pub fn new() -> Result<Arc<Self>, MultithreadedRuntimeError> {
        // First, make sure that this is not being run from a currently active tokio runtime.
        if TokioRuntimeHandle::try_current().is_ok() {
            return Err(MultithreadedRuntimeError::Other(
                "Tokio runtime already started; use from_external instead.".to_owned(),
            ));
        }

        // First, get an Arc value holding the runtime that we can initialize the
        // thread-local THREAD_RUNTIME_REF with
        let rt = Arc::new(Self {
            runtime: std::sync::RwLock::new(None),
            handle_ref: OnceLock::new(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
        });

        // Each thread in each of the tokio worker threads holds a reference to the runtime handling
        // that thread.  If there are multiple runtimes -- as could exist if CTRL-C is hit, then a process
        // calls into xet immediately afterwards -- the references are still correct due to using
        // thread-local storage.
        let rt_c = rt.clone();
        let set_threadlocal_reference = move || {
            THREAD_RUNTIME_REF.set(Some(rt_c.clone()));
        };

        #[cfg(not(target_family = "wasm"))]
        let (runtimes, handles) = {
            // Set the name of a new thread for the threadpool. Names are prefixed with
            // `THREADPOOL_THREAD_ID_PREFIX` and suffixed with a counter:
            // e.g. for io: hf-xet-io-0, hf-xet-io-1, hf-xet-io-2, ...
            // or for compute: hf-xet-compute-0, ...
            let io_thread_id = AtomicUsize::new(0);
            let get_io_thread_name = move || {
                let id = io_thread_id.fetch_add(1, Ordering::Relaxed);
                format!("{THREADPOOL_THREAD_ID_PREFIX}-io-{id}")
            };

            let compute_thread_id = AtomicUsize::new(0);
            let get_compute_thread_name = move || {
                let id = compute_thread_id.fetch_add(1, Ordering::Relaxed);
                format!("{THREADPOOL_THREAD_ID_PREFIX}-compute-{id}")
            };

            let runtimes = TPRuntimes {
                io_runtime: TokioRuntimeBuilder::new_multi_thread()
                    .thread_name_fn(get_io_thread_name) // thread names will be hf-xet-0, hf-xet-1, etc.
                    .worker_threads(THREADPOOL_NUM_IO_WORKERS)
                    .on_thread_start(set_threadlocal_reference.clone()) // Set the local runtime reference.
                    .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
                    .enable_all() // enable all features, including IO/Timer/Signal/Reactor
                    .build()
                    .map_err(MultithreadedRuntimeError::RuntimeInitializationError)?,

                compute_runtime: TokioRuntimeBuilder::new_multi_thread()
                    .thread_name_fn(get_compute_thread_name) // thread names will be hf-xet-0, hf-xet-1, etc.
                    .on_thread_start(set_threadlocal_reference) // Set the local runtime reference.
                    .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
                    .max_blocking_threads(THREADPOOL_MAX_BLOCKING_THREADS) // max 100 threads can block IO
                    .enable_time() // Enable timers; but io should be relegated to the io runtime
                    .build()
                    .map_err(MultithreadedRuntimeError::RuntimeInitializationError)?,
            };

            let handles = TPHandles {
                io_handle: runtimes.io_runtime.handle().clone(),
                compute_handle: runtimes.compute_runtime.handle().clone(),
            };

            (runtimes, handles)
        };

        #[cfg(target_family = "wasm")]
        let (runtimes, handles) = {
            let runtime = TokioRuntimeBuilder::new_current_thread()
                .on_thread_start(set_threadlocal_reference) // Set the local runtime reference.
                .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
                .enable_all() // enable all features, including IO/Timer/Signal/Reactor
                .build()
                .map_err(MultithreadedRuntimeError::RuntimeInitializationError)?;

            // Here, it's just one
            let handles = TPHandles {
                io_handle: runtime.handle().clone(),
                compute_handle: runtime.handle().clone(),
            };

            (TPRuntimes { runtime }, handles)
        };

        // Now that the runtime is created, fill out the original struct.
        *rt.runtime.write().unwrap() = Some(runtimes); // Only fails if other thread destroyed mutex; unwrap ok.
        rt.handle_ref.set(handles).unwrap(); // Only fails if set called twice; unwrap ok.

        Ok(rt)
    }

    pub fn from_external(rt_handle: TokioRuntimeHandle) -> Arc<Self> {
        Arc::new(Self {
            runtime: std::sync::RwLock::new(None),
            handle_ref: TPHandles {
                // From the external environment, use the same handle for both
                io_handle: rt_handle.clone(),
                compute_handle: rt_handle,
            }
            .into(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
        })
    }

    #[inline]
    pub fn compute_handle(&self) -> TokioRuntimeHandle {
        self.handle_ref
            .get()
            .expect("Not initialized with handle set.")
            .compute_handle
            .clone()
    }

    #[inline]
    pub fn io_handle(&self) -> TokioRuntimeHandle {
        self.handle_ref
            .get()
            .expect("Not initialized with handle set.")
            .io_handle
            .clone()
    }

    pub fn num_worker_threads(&self) -> usize {
        self.compute_handle().metrics().num_workers()
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

        let ret = self.compute_handle().block_on(async move {
            // Run the actual task on a task worker thread so we can get back information
            // on issues, including reporting panics as runtime errors.
            self.compute_handle()
                .spawn(future)
                .await
                .map_err(MultithreadedRuntimeError::from)
        });

        self.external_executor_count.fetch_sub(1, Ordering::SeqCst);
        ret
    }

    pub async fn execute_io_task<F>(future: F) -> Result<F::Output, MultithreadedRuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        ThreadPool::current()
            .io_handle()
            .spawn(future)
            .await
            .map_err(MultithreadedRuntimeError::from)
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

        let compute_metrics = runtime.compute_runtime.metrics();
        write!(
            f,
            "compute pool: num_workers: {:?}, num_alive_tasks: {:?}, global_queue_depth: {:?}",
            compute_metrics.num_workers(),
            compute_metrics.num_alive_tasks(),
            compute_metrics.global_queue_depth()
        )
    }
}
