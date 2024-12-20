use std::future::Future;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use lazy_static::lazy_static;
use tracing::debug;

use crate::errors::{map_join_error, Result, RuntimeCancellation, XetRuntimeError};
use crate::primatives::{AsyncJoinSet, ComputeJoinHandle, ComputeJoinSet};
use crate::AsyncJoinHandle;

const ASYNC_THREADPOOL_NUM_WORKER_THREADS: usize = 4; // 4 active threads
const ASYNC_THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet-async"; // thread names will be hf-xet-async-0, hf-xet-async-1, etc.
const THREADPOOL_STACK_SIZE: usize = 8_000_000; // 8MB stack size

const COMPUTE_THREADPOOL_NUM_WORKER_THREADS: usize = 4;

const COMPUTE_THREADPOOL_THREAD_ID_PREFIX: &str = "hf-xet-comp"; // thread names will be hf-xet-comp-0, hf-xet-comp-1, etc.

lazy_static! {
    static ref XET_RUNTIME: XetRuntime = XetRuntime::new().expect("Error Starting Xet Runtime");
}

pub fn xet_runtime() -> &'static XetRuntime {
    &XET_RUNTIME
}

#[inline]
pub fn cancellation_requested() -> bool {
    xet_runtime().cancelation_requested()
}

/// Call this function within long-running loops or tasks to check whether a cancellation request
/// (e.g. a CTRL-C press) has happened.  If so, it returns an error that can be handled outside.  
#[inline]
pub fn check_runtime_cancellation() -> std::result::Result<(), RuntimeCancellation> {
    xet_runtime().check_for_cancellation()
}

#[derive(Debug)]
pub struct XetRuntime {
    //
    async_runtime: tokio::runtime::Runtime,

    // The number of external threads calling into this threadpool
    external_executor_count: AtomicUsize,

    // Are we in the middle of a sigint shutdown?
    cancelation_requested: AtomicBool,

    // The rayon threadpool for tasks?
    compute_threadpool: Arc<rayon::ThreadPool>,
}

impl XetRuntime {
    /// Constructs the runtime.  Should typically be used from runtime() as the lazily initialized
    /// static global above
    pub fn new() -> Result<Self> {
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ASYNC_THREADPOOL_NUM_WORKER_THREADS) // 4 active threads
            .thread_name_fn(get_async_thread_name) // thread names will be hf-xet-0, hf-xet-1, etc.
            .thread_stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
            .enable_all() // enable all features, including IO/Timer/Signal/Reactor
            .build()
            .map_err(XetRuntimeError::AsyncRuntimeInitializationError)?;

        let compute_threadpool = rayon::ThreadPoolBuilder::new()
            .num_threads(COMPUTE_THREADPOOL_NUM_WORKER_THREADS.min(num_cpus::get() - 2).max(1)) // Use the number of CPUs by default
            .stack_size(THREADPOOL_STACK_SIZE) // 8MB stack size, default is 2MB
            .thread_name(|idx| format!("{COMPUTE_THREADPOOL_THREAD_ID_PREFIX}-{idx}"))
            .build()
            .map_err(|e| {
                XetRuntimeError::ComputeRuntimeInitializationError(format!("Failed to initialize thread pool: {:?}", e))
            })?;

        Ok(Self {
            async_runtime,
            external_executor_count: AtomicUsize::new(0),
            cancelation_requested: AtomicBool::new(false),
            compute_threadpool: Arc::new(compute_threadpool),
        })
    }

    /// Spawns a compute task to be run on the current compute thread pool,
    /// returning a join handle to the result.  This task may be run in any
    /// order relative to the other tasks in the worker pool.
    pub fn spawn_compute_task<T: Send + Sync + 'static>(
        &self,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> Result<ComputeJoinHandle<T>> {
        self.check_for_cancellation()?;

        let (jh, tx) = ComputeJoinHandle::create();

        self.compute_threadpool.spawn(move || {
            let result = task();
            let _ = tx.send(result).map_err(|e| {
                debug!("Return result on join handle encountered error: {e:?}");
                e
            });
        });

        Ok(jh)
    }

    /// Spawns a compute task to be run on the current compute thread pool with FIFO
    /// priority relative to other fifo threads.  All other FIFO spawned tasks will be started before
    /// this one is started.  This does not mean that this task will *finish* after
    /// the other tasks.
    pub fn spawn_compute_task_fifo<T: Send + Sync + 'static>(
        &self,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> Result<ComputeJoinHandle<T>> {
        self.check_for_cancellation()?;

        let (jh, tx) = ComputeJoinHandle::create();

        self.compute_threadpool.spawn_fifo(move || {
            let result = task();
            let _ = tx.send(result).map_err(|e| {
                debug!("Return result on join handle encountered error: {e:?}");
                e
            });
        });

        Ok(jh)
    }

    /// From an async context, run a compute task, yielding while that task finishes to avoid tying up an
    /// async worker.
    pub async fn run_compute_task_from_async<T: Send + Sync + 'static>(
        &self,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T> {
        self.spawn_compute_task(task)?.await
    }

    /// Run an async task within the current runtime.  Should not be called from within a
    /// an async worker thread; this may lead to deadlock as it queues a task but then blocks
    /// and ties up the current worker while waiting for that task to complete.
    pub fn run_async_task<F>(&self, future: F) -> Result<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + Sync + 'static,
    {
        self.check_for_cancellation()?;

        self.async_runtime.block_on(async move {
            // Run the actual task on a task worker thread so we can get back information
            // on issues, including reporting panics as runtime errors.
            tokio::spawn(future).await.map_err(map_join_error)
        })
    }

    /// Spawn an async task to run in the background on the current pool of async worker threads.
    pub fn spawn_async_task<F>(&self, future: F) -> Result<AsyncJoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + Sync + 'static,
    {
        self.check_for_cancellation()?;

        // If the runtime has been shut down, this will immediately abort.
        Ok(AsyncJoinHandle::new(self.async_runtime.handle().clone(), self.async_runtime.spawn(future)))
    }

    /// Runs a set of compute functions in parallel on the input, returning a vector of results.
    pub fn par_for<F, T, I, E>(&self, inputs: I, func: F) -> std::result::Result<Vec<T>, E>
    where
        I: rayon::iter::IntoParallelIterator + Send,
        F: Fn(I::Item) -> std::result::Result<T, E> + Send + Sync,
        T: Send,
        I::Item: Send,
        E: Send + std::fmt::Debug,
    {
        let error = std::sync::Mutex::new(None); // To capture the first error safely across threads.
        let error_occured = AtomicBool::new(false);

        use rayon::iter::ParallelIterator;

        let results: Vec<Option<T>> = self.compute_threadpool.install(|| {
            inputs
                .into_par_iter()
                .map(|item| {
                    if error_occured.load(Ordering::Acquire) {
                        // If an error has already been captured, skip further processing.
                        return None;
                    }

                    match func(item) {
                        Ok(result) => Some(result),
                        Err(e) => {
                            // Capture the first error in the Mutex.
                            error_occured.store(true, Ordering::SeqCst);
                            *error.lock().unwrap() = Some(e);
                            None
                        },
                    }
                })
                .collect()
        });

        // If an error occurred, return it; otherwise, unwrap the results.
        if let Some(err) = error.into_inner().unwrap() {
            Err(err)
        } else {
            Ok(results.into_iter().map(|r| r.unwrap()).collect())
        }
    }

    /// Runs a set of compute functions in parallel on the input.
    pub fn par_for_each<F, I, E>(&self, inputs: I, func: F) -> std::result::Result<(), E>
    where
        I: rayon::iter::IntoParallelIterator + Send,
        F: Fn(I::Item) -> std::result::Result<(), E> + Send + Sync,
        I::Item: Send,
        E: Send + std::fmt::Debug,
    {
        let error = std::sync::Mutex::new(None); // To capture the first error safely across threads.
        let error_occured = AtomicBool::new(false);

        use rayon::iter::ParallelIterator;

        self.compute_threadpool.install(|| {
            inputs.into_par_iter().for_each(|item| {
                if error_occured.load(Ordering::Acquire) {
                    // If an error has already been captured, skip further processing.
                    return;
                }

                if let Err(e) = func(item) {
                    // Capture the first error in the Mutex.
                    error_occured.store(true, Ordering::SeqCst);
                    *error.lock().unwrap() = Some(e);
                }
            });
        });

        // If an error occurred, return it; otherwise, unwrap the results.
        if let Some(err) = error.into_inner().unwrap() {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Enter the runtime from an external call, running the given task as the entry point
    /// and returning the result.
    pub fn enter_runtime<Out: Send + Sync>(&self, task: impl FnOnce() -> Out) -> Result<Out> {
        self.check_for_cancellation()?;

        self.increment_external_executor_count();

        let ret = if self.cancelation_requested() {
            Err(XetRuntimeError::RuntimeCancellation(RuntimeCancellation {}))
        } else {
            Ok(task())
        };

        self.decrement_external_executor_count();
        ret
    }

    /// Enter the runtime from an external call, running the given async task as the entry point
    /// and returning the result.
    ///
    /// This function should ONLY be used by threads outside of tokio; it should not be called
    /// from within a task running on the runtime worker pool.  Doing so can lead to deadlocking.
    pub fn enter_runtime_async<F>(&self, future: F) -> Result<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + Sync,
    {
        self.check_for_cancellation()?;

        self.increment_external_executor_count();
        let ret = self.run_async_task(future);
        self.decrement_external_executor_count();
        ret
    }

    /// Create a JoinSet for use on the compute runtime.  A JoinSet allows a place for spawning a set of tasks
    /// that can complete out of order.  Results are available as soon as any job completes.
    pub fn compute_joinset<ResultType: Send + Sync + 'static>(&self) -> ComputeJoinSet<ResultType> {
        ComputeJoinSet::new(self.compute_threadpool.clone())
    }

    /// Create an async JoinSet for use on the async runtime.  A JoinSet allows a place for spawning a set of tasks
    /// that can complete out of order.  Results are available as soon as any job completes.
    pub fn async_joinset<ResultType: Send + Sync + 'static>(&self) -> AsyncJoinSet<ResultType> {
        AsyncJoinSet::new(self.async_runtime.handle().clone())
    }

    /// Gives the number of current calls in the enter_runtime_* methods.
    #[inline]
    pub fn external_executor_count(&self) -> usize {
        self.external_executor_count.load(Ordering::SeqCst)
    }

    /// Sets a global flag to signal that all tasks should be canceled.  
    /// It is up to individual tasks to call check_for_cancellation when appropriate,
    /// preferably as frequently as possible.  
    pub fn request_task_cancellation(&self) {
        // Issue the flag to cause all the tasks to cancel.
        self.cancelation_requested.store(true, Ordering::SeqCst);
    }

    /// Returns true if we're in the middle of a cancellation request (E.g. CTRL-C).
    /// and false otherwise.
    #[inline]
    pub fn cancelation_requested(&self) -> bool {
        self.cancelation_requested.load(Ordering::SeqCst)
    }

    /// If a cancellation has been requested, returns a RuntimeCancellation error.  Otherwise
    /// does nothing.  
    #[inline]
    pub fn check_for_cancellation(&self) -> std::result::Result<(), RuntimeCancellation> {
        if self.cancelation_requested() {
            Err(RuntimeCancellation {})
        } else {
            Ok(())
        }
    }

    /// Increments the count of other threads entering the runtime.
    #[inline]
    fn increment_external_executor_count(&self) {
        self.external_executor_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrements the count of other threads entering the runtime.
    /// Increments the count of other threads entering the runtime.
    #[inline]
    fn decrement_external_executor_count(&self) {
        let prev_count = self.external_executor_count.fetch_sub(1, Ordering::SeqCst);
        debug_assert_ne!(prev_count, 0);

        if prev_count == 1 {
            // This is the last thread leaving the runtime, so make sure the cancellation flag is cleared.
            self.cancelation_requested.store(false, Ordering::Relaxed);
        }
    }
}

/// gets the name of a new thread for the threadpool. Names are prefixed with
/// `ASYNC_THREADPOOL_THREAD_ID_PREFIX` and suffixed with a global counter:
/// e.g. hf-xet-0, hf-xet-1, hf-xet-2, ...
fn get_async_thread_name() -> String {
    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
    let id = ATOMIC_ID.fetch_add(1, SeqCst);
    format!("{ASYNC_THREADPOOL_THREAD_ID_PREFIX}-{id}")
}

mod tests {
    use super::*;

    #[test]
    fn test_runtime_initialization() {
        let runtime = XetRuntime::new();
        assert!(runtime.is_ok(), "Failed to initialize XetRuntime: {:?}", runtime.err());
    }

    #[test]
    fn test_spawn_compute_task() {
        let runtime = XetRuntime::new().unwrap();
        let handle = runtime.spawn_compute_task(|| 42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_spawn_async_background_task() {
        let runtime = XetRuntime::new().unwrap();
        let handle = runtime.spawn_async_task(async { 42 }).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_spawn_compute_task_fifo() {
        let runtime = XetRuntime::new().unwrap();
        let handle = runtime.spawn_compute_task_fifo(|| 42).unwrap();
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_run_compute_task_from_async() {
        let runtime = Arc::new(XetRuntime::new().unwrap());
        let runtime_ = runtime.clone();
        let result = runtime
            .enter_runtime_async(async move { runtime_.run_compute_task_from_async(|| 42).await.unwrap() })
            .unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_run_async_task() {
        let runtime = XetRuntime::new().unwrap();
        let result = runtime.run_async_task(async { 42 });
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_spawn_async_task() {
        let runtime = Arc::new(XetRuntime::new().unwrap());
        let runtime_ = runtime.clone();

        runtime
            .enter_runtime_async(async move {
                let handle = runtime_.spawn_async_task(async { 42 }).unwrap();

                let result = handle.await.unwrap();
                assert_eq!(result, 42);
            })
            .unwrap();

        // Now, make sure things can be passed around.
        let handle = runtime.spawn_async_task(async { 42 }).unwrap();

        runtime
            .run_async_task(async move {
                let result = handle.await.unwrap();
                assert_eq!(result, 42);
            })
            .unwrap();

        // Now, make sure things can be passed around.
        let handle = runtime.spawn_async_task(async { 42 }).unwrap();

        // Retrieve from the sync context
        let result = handle.join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_par_for() {
        let runtime = XetRuntime::new().unwrap();
        let data = vec![1, 2, 3];
        let result = runtime.par_for(data, |x| Result::Ok(x + 1)).unwrap();
        assert_eq!(result, vec![2, 3, 4]);
    }

    #[test]
    fn test_par_for_each() {
        let runtime = XetRuntime::new().unwrap();
        let data: Vec<_> = (0..500).collect();
        let result = runtime.par_for_each(&data, |_| Result::Ok(()));
        assert!(result.is_ok());

        let result = runtime.par_for_each(&data, |x| {
            if *x == 4 {
                Err(XetRuntimeError::Other("I don't like the number 4".to_owned()))
            } else {
                Ok(())
            }
        });
        assert!(!result.is_ok());

        let data: Vec<_> = (0..500).map(AtomicUsize::new).collect();

        runtime
            .par_for_each(&data, |x| {
                x.fetch_add(1, Ordering::Relaxed);
                Result::Ok(())
            })
            .unwrap();

        for (i, r) in data.iter().enumerate() {
            assert_eq!(i + 1, r.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn test_cancellation_request() {
        let runtime = XetRuntime::new().unwrap();
        runtime.request_task_cancellation();
        assert!(runtime.cancelation_requested());
    }

    #[test]
    fn test_check_for_cancellation() {
        let runtime = XetRuntime::new().unwrap();
        runtime.request_task_cancellation();
        let result = runtime.check_for_cancellation();
        assert!(result.is_err());
    }

    #[test]
    fn test_external_executor_count() {
        let runtime = Arc::new(XetRuntime::new().unwrap());

        assert_eq!(runtime.external_executor_count(), 0);

        let r1 = runtime.clone();
        runtime
            .enter_runtime(move || {
                assert_eq!(r1.external_executor_count(), 1);
            })
            .unwrap();

        assert_eq!(runtime.external_executor_count(), 0);

        let r2 = runtime.clone();
        runtime
            .enter_runtime_async(async move {
                assert_eq!(r2.external_executor_count(), 1);
            })
            .unwrap();

        assert_eq!(runtime.external_executor_count(), 0);
    }

    #[test]
    fn test_compute_joinset() {
        let runtime = XetRuntime::new().unwrap();
        let mut joinset = runtime.compute_joinset();

        // Test that it's repeatable too
        for _iter in [0, 1] {
            joinset.spawn(|| 42);
            assert!(!joinset.is_empty());
            assert_eq!(joinset.join_next().unwrap(), Some(42));
            assert!(joinset.is_empty());
            assert_eq!(joinset.join_next().unwrap(), None);
            assert!(joinset.is_empty());
            assert_eq!(joinset.join_next().unwrap(), None);
        }
    }

    #[test]
    fn test_async_joinset() {
        let runtime = XetRuntime::new().unwrap();
        let mut joinset = runtime.async_joinset();
        joinset.spawn(async { 42 });
        let result = runtime
            .run_async_task(async move { (joinset.join_next().await.unwrap(), joinset.join_next().await.unwrap()) })
            .unwrap();
        assert_eq!(result, (Some(42), None));
    }

    #[test]
    fn test_async_joinset_blocking() {
        let runtime = XetRuntime::new().unwrap();
        let mut joinset = runtime.async_joinset();
        joinset.spawn(async { 42 });
        let result = joinset.join_next_blocking().unwrap();
        assert_eq!(result, Some(42));
        let result = joinset.join_next_blocking().unwrap();
        assert_eq!(result, None);
    }
}
