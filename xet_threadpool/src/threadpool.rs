use std::cell::RefCell;
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioRuntimeHandle, Runtime as TokioRuntime};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::errors::MultithreadedRuntimeError;
use crate::global_semaphores::{GlobalSemaphoreHandle, GlobalSemaphoreLookup};

const THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet"; // thread names will be hf-xet-0, hf-xet-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size

/// Cap the number of tokio threads to 32 to avoid massive expansion on huge CPUs; can be overridden with
/// TOKIO_WORKER_THREADS.
///
/// Note that the compute intensive parts of the code get offloaded to blocking threads, which don't count against this
/// limit.
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
    runtime: std::sync::RwLock<Option<TokioRuntime>>,

    // We use this handle when we actually enter the runtime to avoid the lock.  It is
    // the same as using the runtime, with the exception that it does not block a shutdown
    // while holding a reference to the runtime does.
    handle_ref: OnceLock<TokioRuntimeHandle>,

    // The number of external threads calling into this threadpool
    external_executor_count: AtomicUsize,

    // Are we in the middle of a sigint shutdown?
    sigint_shutdown: AtomicBool,

    // Semaphores.  We use an initialization function as the handle here.
    global_semaphore_table: GlobalSemaphoreLookup,
}

// Use thread-local references to the runtime that are set on initilization among all
// the worker threads in the runtime.  This way, XetRuntime::current() will always refer to
// the runtime active with that worker thread.
thread_local! {
    static THREAD_RUNTIME_REF: RefCell<Option<(u32, Arc<ThreadPool>)>> = const { RefCell::new(None) };
}

impl ThreadPool {
    /// Return the current threadpool that the current worker thread uses.  Will fail if  
    /// called from a thread that is not spawned from the current runtime.  
    #[inline]
    pub fn current() -> Arc<Self> {
        let maybe_rt = THREAD_RUNTIME_REF.with_borrow(|rt| rt.clone());

        if let Some((pid, rt)) = maybe_rt {
            if pid == std::process::id() {
                return rt;
            }
        }

        let Ok(tokio_rt) = TokioRuntimeHandle::try_current() else {
            panic!("ThreadPool::current() called before ThreadPool::new() or on thread outside of current runtime.");
        };

        Self::from_external(tokio_rt)
    }

    pub fn new() -> Result<Arc<Self>, MultithreadedRuntimeError> {
        // First, get an Arc value holding the runtime that we can initialize the
        // thread-local THREAD_RUNTIME_REF with
        let rt = Arc::new(Self {
            runtime: std::sync::RwLock::new(None),
            handle_ref: OnceLock::new(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
            global_semaphore_table: GlobalSemaphoreLookup::default(),
        });

        // Each thread in each of the tokio worker threads holds a reference to the runtime handling
        // that thread.  If there are multiple runtimes -- as could exist if CTRL-C is hit, then a process
        // calls into xet immediately afterwards -- the references are still correct due to using
        // thread-local storage.
        let rt_c = rt.clone();
        let pid = std::process::id();
        let set_threadlocal_reference = move || {
            THREAD_RUNTIME_REF.set(Some((pid, rt_c.clone())));
        };

        // Set the name of a new thread for the threadpool. Names are prefixed with
        // `THREADPOOL_THREAD_ID_PREFIX` and suffixed with a counter:
        // e.g. hf-xet-0, hf-xet-1, hf-xet-2, ...
        let thread_id = AtomicUsize::new(0);
        let get_thread_name = move || {
            let id = thread_id.fetch_add(1, Ordering::Relaxed);
            format!("{THREADPOOL_THREAD_ID_PREFIX}-{id}")
        };

        let tokio_rt = {
            #[cfg(not(target_family = "wasm"))]
            {
                // A new multithreaded runtime with a capped number of threads
                TokioRuntimeBuilder::new_multi_thread().worker_threads(get_num_tokio_worker_threads())
            }

            #[cfg(target_family = "wasm")]
            {
                TokioRuntimeBuilder::new_current_thread()
            }
        }
        .thread_name_fn(get_thread_name) // thread names will be hf-xet-0, hf-xet-1, etc.
        .on_thread_start(set_threadlocal_reference) // Set the local runtime reference.
        .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
        .enable_all() // enable all features, including IO/Timer/Signal/Reactor
        .build()
        .map_err(MultithreadedRuntimeError::RuntimeInitializationError)?;

        // Now that the runtime is created, fill out the original struct.
        let handle = tokio_rt.handle().clone();
        *rt.runtime.write().unwrap() = Some(tokio_rt); // Only fails if other thread destroyed mutex; unwrap ok.
        rt.handle_ref.set(handle).unwrap(); // Only fails if set called twice; unwrap ok.

        Ok(rt)
    }

    pub fn from_external(rt_handle: TokioRuntimeHandle) -> Arc<Self> {
        Arc::new(Self {
            runtime: std::sync::RwLock::new(None),
            handle_ref: rt_handle.into(),
            external_executor_count: 0.into(),
            sigint_shutdown: false.into(),
            global_semaphore_table: GlobalSemaphoreLookup::default(),
        })
    }

    #[inline]
    pub fn handle(&self) -> TokioRuntimeHandle {
        self.handle_ref.get().expect("Not initialized with handle set.").clone()
    }

    pub fn num_worker_threads(&self) -> usize {
        self.handle().metrics().num_workers()
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

    /// Discards the runtime without shutdown; to be used after fork-exec or spawn.
    pub fn discard_runtime(&self) {
        // This function makes a best effort attempt to clean everything up.

        // When a task is shut down, it will stop running at whichever .await it has yielded at.  All local
        // variables are destroyed by running their destructor.
        //
        // If this call fails, then it means that there is a recursive call to this runtime, or that
        // this process is in the middle of a shutdown, so we can ignore it silently.
        let Ok(mut rt_lock) = self.runtime.write() else {
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

    /// This function should ONLY be used by threads outside of tokio; it should not be called
    /// from within a task running on the runtime worker pool.  Doing so can lead to deadlocking.
    pub fn external_run_async_task<F>(&self, future: F) -> Result<F::Output, MultithreadedRuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + Sync,
    {
        self.external_executor_count.fetch_add(1, Ordering::SeqCst);

        let ret = self.handle().block_on(async move {
            // Run the actual task on a task worker thread so we can get back information
            // on issues, including reporting panics as runtime errors.
            self.handle().spawn(future).await.map_err(MultithreadedRuntimeError::from)
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
        self.handle().spawn(future)
    }

    /// Allows a user to access a global semaphore that is associated with the runtime.
    ///
    /// The key here is a function handle that, when called, returns the number of permits
    /// to use in the semaphore.  It's reset on new runtimes.
    pub fn global_semaphore(&self, handle: impl Into<GlobalSemaphoreHandle>) -> Arc<Semaphore> {
        self.global_semaphore_table.get(handle)
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
