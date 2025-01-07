use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::FutureExt;
use rayon::ThreadPool;

use crate::errors::{map_join_error, Result, XetRuntimeError};

/// Join handle for a task on the compute runtime.  
///
/// This struct should only be instantiated through xet_runtime().spawn_compute_task(...).
///
/// # Example:
///
/// ```
/// use xet_runtime::{xet_runtime, ComputeJoinHandle};
/// let handle: ComputeJoinHandle<_> = xet_runtime().spawn_compute_task(|| 99).unwrap();
///
/// // From synchronous code:
/// assert_eq!(handle.join().unwrap(), 99);
///
/// // From async code:
/// // assert_eq!(handle.await?, 99);
/// ```
pub struct ComputeJoinHandle<T: Send + Sync> {
    task_result: oneshot::Receiver<T>, /* Use the other join handle to figure out when the previous job is
                                        * done. */
}

impl<T: Send + Sync> ComputeJoinHandle<T> {
    pub(crate) fn create() -> (Self, oneshot::Sender<T>) {
        let (sender, task_result) = oneshot::channel();
        (Self { task_result }, sender)
    }

    /// Blocks the current thread until the CPU-bound task has finished.  
    /// Use this only in synchronous code; in async code, just use `.await`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying task panicked.  
    ///
    /// # Examples
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinHandle};
    /// let handle: ComputeJoinHandle<_> = xet_runtime().spawn_compute_task(|| 42).unwrap();
    /// let result = handle.join().unwrap();
    /// assert_eq!(result, 42);
    /// ```
    pub fn join(self) -> Result<T> {
        self.task_result
            .recv()
            .map_err(|e| XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}")))
    }

    /// Attempts to retrieve the result without blocking.  
    ///
    /// - Returns `Ok(Some(value))` if the task is complete.
    /// - Returns `Ok(None)` if the task is still running.
    /// - Returns an `Err(...)` variant if
    ///
    /// # Examples
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinHandle};
    /// let handle: ComputeJoinHandle<_> = xet_runtime().spawn_compute_task(|| 42).unwrap();
    ///
    /// // Possibly do some work here...
    /// match handle.try_join() {
    ///     Ok(Some(value)) => println!("Value is ready: {}", value),
    ///     Ok(None) => println!("Still running"),
    ///     Err(e) => eprintln!("Error: {:?}", e),
    /// }
    /// ```    
    pub fn try_join(&self) -> Result<Option<T>> {
        match self.task_result.try_recv() {
            Err(oneshot::TryRecvError::Empty) => Ok(None),
            Err(e) => Err(XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}"))),
            Ok(r) => Ok(Some(r)),
        }
    }
}

/// Implements `.await` for [`ComputeJoinHandle`], allowing async code to
/// wait on it directly:
///
/// ```ignore
/// async fn async_code() -> Result<()> {
///     let handle = xet_runtime().spawn_compute_task(|| 42)?;
///     let result = handle.await?;
///     assert_eq!(result, 42);
///     Ok(())
/// }
impl<T: Send + Sync> Future for ComputeJoinHandle<T> {
    type Output = Result<T>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.task_result
            .poll_unpin(cx)
            .map_err(|e| XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}")))
    }
}

/// A handle for an async task spawned via
/// [`XetRuntime::spawn_async_task`](crate::XetRuntime::spawn_async_task).
///
/// # Overview
///
/// `AsyncJoinHandle` allows you to retrieve the result of an async function
/// running on the Tokio runtime. You can:
///
/// - **Block in synchronous code** with [`AsyncJoinHandle::join`], or
/// - **Await in async code** by using `.await` directly on the handle.
///
/// # Examples
///
/// ```
/// # use xet_runtime::{xet_runtime, AsyncJoinHandle};
/// let handle: AsyncJoinHandle<_> = xet_runtime().spawn_async_task(async { 99 }).unwrap();
///
/// // From synchronous code, block until the async task completes:
/// let val = handle.join().unwrap();
/// assert_eq!(val, 99);
///
/// // Or from async code, you could do:
/// // let val = handle.await?;
/// ```
pub struct AsyncJoinHandle<T: Send + Sync> {
    // Just wrap the inner struct and translates any errors to XetRuntimeErrors.
    inner: tokio::task::JoinHandle<T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<T: Send + Sync> AsyncJoinHandle<T> {
    /// Internal constructor; use XetRuntime::spawn_async_task
    pub(crate) fn new(runtime_handle: tokio::runtime::Handle, inner: tokio::task::JoinHandle<T>) -> Self {
        Self { inner, runtime_handle }
    }

    /// Blocks until the async task completes, returning the task's output.
    ///
    /// Use this only in non-async code. In async code, you can
    /// simply do `let result = handle.await?`.
    ///
    /// # Errors
    ///
    /// Returns an error if the async task panics or if joining fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, AsyncJoinHandle};
    /// let handle = xet_runtime().spawn_async_task(async { 42 }).unwrap();
    /// assert_eq!(handle.join().unwrap(), 42);
    /// ```
    pub fn join(self) -> Result<T> {
        self.runtime_handle.block_on(self.inner).map_err(map_join_error)
    }
}

/// Implements `.await` for [`AsyncJoinHandle`], allowing async code to
/// wait on it directly:
///
/// ```ignore
/// async fn async_code() -> Result<()> {
///     let handle = xet_runtime().spawn_async_task(async { 42 })?;
///     let result = handle.await?;
///     assert_eq!(result, 42);
///     Ok(())
/// }
/// ```
impl<T: Send + Sync> Future for AsyncJoinHandle<T> {
    type Output = Result<T>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.inner.poll_unpin(cx).map_err(map_join_error)
    }
}

/// A set for managing multiple compute tasks whose results can be retrieved as they complete.
/// You can process tasks as they finish without waiting for them all to
/// complete (unlike a simple `par_for` call, which collects all results).
///
/// # Overview
///
/// `ComputeJoinSet` maintains a collection of tasks. You can:
///
/// - [`spawn`](Self::spawn) tasks (unordered)
/// - [`spawn_fifo`](Self::spawn_fifo) tasks (FIFO-ordered)
/// - retrieve each completed result via [`join_next`](Self::join_next) or [`try_join_next`](Self::try_join_next)
///
/// Create a `ComputeJoinSet` via
/// [`XetRuntime::compute_joinset`](crate::XetRuntime::compute_joinset):
///
/// Example:
///
/// ```
/// use xet_runtime::xet_runtime;
/// let mut join_set = xet_runtime().compute_joinset::<i32>();
///
/// join_set.spawn(|| 42);
/// join_set.spawn(|| 99);
///
/// let mut results = vec![];
///
/// while let Some(res) = join_set.join_next().unwrap() {
///     results.push(res);
/// }
///
/// results.sort();
///
/// assert_eq!(results, vec![42, 99]);
/// ```
pub struct ComputeJoinSet<T: Send + Sync + 'static> {
    thread_pool: Arc<ThreadPool>,

    receiver: crossbeam::channel::Receiver<T>,

    shared_data: Arc<ComputeJoinSetSharedData<T>>,
}

/// Internal helper struct for the join set.
struct ComputeJoinSetSharedData<T: Send + Sync + 'static> {
    // The channel on which to send new results.
    result_sender: crossbeam::channel::Sender<T>,

    // The total number of results to expect, either in the channel or coming from
    // still-running tasks
    incoming_result_count: AtomicUsize,
}

impl<T: Send + Sync + 'static> ComputeJoinSet<T> {
    /// Creates a new `ComputeJoinSet` with the provided Rayon `ThreadPool`.
    pub(crate) fn new(thread_pool: Arc<ThreadPool>) -> Self {
        let (result_sender, receiver) = crossbeam::channel::unbounded();
        Self {
            thread_pool,
            receiver,
            shared_data: Arc::new(ComputeJoinSetSharedData {
                result_sender,
                incoming_result_count: AtomicUsize::new(0),
            }),
        }
    }

    /// Spawns a new *FIFO* task onto the Rayon threadpool. Tasks spawned with
    /// `spawn_fifo` will wait for previously spawned FIFO tasks to *start*
    /// before they themselves begin.
    ///
    /// This does not imply they will *finish* in FIFO order, only *start*.
    ///
    /// # Example:
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinSet};
    /// # let rt = xet_runtime();
    /// # let mut join_set = rt.compute_joinset::<i32>();
    /// join_set.spawn_fifo(|| 42);
    /// join_set.spawn_fifo(|| 99);
    ///
    /// while let Some(val) = join_set.join_next().unwrap() {
    ///     println!("Got val: {}", val);
    /// }
    /// ```
    pub fn spawn_fifo<F>(&self, task: F)
    where
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_impl(task, true);
    }

    /// Spawns a new *unordered* task on the compute threadpool.
    /// This may run in parallel with other tasks in any order.
    ///
    /// # Example:
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinSet};
    /// # let rt = xet_runtime();
    /// # let mut join_set = rt.compute_joinset::<i32>();
    /// join_set.spawn(|| 42);
    /// join_set.spawn(|| 99);
    ///
    /// // collect results
    /// while let Some(val) = join_set.join_next().unwrap() {
    ///     println!("Got val: {}", val);
    /// }
    /// ```
    pub fn spawn<F>(&self, task: F)
    where
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_impl(task, false);
    }

    fn spawn_impl<F>(&self, task: F, fifo: bool)
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let shared_data = self.shared_data.clone();

        // One more task to expect incoming.
        shared_data.incoming_result_count.fetch_add(1, Ordering::SeqCst);

        let exec = move || {
            let result = task();
            // Send the result back to the ComputeJoinSet.
            if shared_data.result_sender.send(result).is_err() {
                // Receiver has been dropped, ignore the result but this is a programming bug; likely
                // going to occur only under a panic rewind threading situation.
                if cfg!(debug_assertions) {
                    eprintln!("Error in ComputeJoinSet: Send failed as receiver dropped.");
                }
                // This one errored out, so nothing more to be done.
                shared_data.incoming_result_count.fetch_sub(1, Ordering::SeqCst);
            }
        };
        if fifo {
            self.thread_pool.spawn_fifo(exec);
        } else {
            self.thread_pool.spawn(exec);
        }
    }

    /// Attempts to retrieve a completed task result without blocking.
    ///
    /// - Returns `Ok(Some(value))` if a task has completed.
    /// - Returns `Ok(None)` if no tasks have completed yet.
    /// - Returns `Err(...)` if there’s a channel or internal error.
    ///
    /// # Example:
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinSet};
    /// # let rt = xet_runtime();
    /// # let mut join_set = rt.compute_joinset::<i32>();
    /// join_set.spawn(|| 42);
    /// match join_set.try_join_next() {
    ///     Ok(Some(val)) => println!("Value: {}", val),
    ///     Ok(None) => println!("Not ready yet"),
    ///     Err(e) => eprintln!("Error: {:?}", e),
    /// }
    /// ```    
    pub fn try_join_next(&mut self) -> Result<Option<T>> {
        let ret = match self.receiver.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(crossbeam::channel::TryRecvError::Empty) => {
                return Ok(None);
            },
            Err(e) => Err(XetRuntimeError::Other(format!("JoinSet: Channel discarded/send closed ({e})."))),
        };

        self.shared_data.incoming_result_count.fetch_sub(1, Ordering::SeqCst);

        ret
    }

    /// Waits until at least one of the running tasks completes, returning its result.
    /// Returns `Ok(None)` if there are no more tasks left.
    ///
    /// # Example:
    ///
    /// ```
    /// # use xet_runtime::{xet_runtime, ComputeJoinSet};
    /// # let rt = xet_runtime();
    /// # let mut join_set = rt.compute_joinset::<i32>();
    /// join_set.spawn(|| 42);
    /// join_set.spawn(|| 99);
    ///
    /// while let Some(val) = join_set.join_next().unwrap() {
    ///     println!("Got val: {}", val);
    /// }
    /// ```
    pub fn join_next(&mut self) -> Result<Option<T>> {
        // Note: the &mut self here forces us to have exclusive access; otherwise
        // there is a race condition in which another join_next steals the result between
        // testing the sender count and receiving the result, causing this join_next to
        // block forever.  Exclusive access prevents this.
        if self.shared_data.incoming_result_count.load(Ordering::SeqCst) == 0 {
            // None left to account for.
            return Ok(None);
        }

        let ret = match self.receiver.recv() {
            Ok(result) => Ok(Some(result)),
            Err(e) => Err(XetRuntimeError::Other(format!("JoinSet: Channel discarded/send closed ({e})."))),
        };
        self.shared_data.incoming_result_count.fetch_sub(1, Ordering::SeqCst);
        ret
    }

    /// Checks if no tasks are present or all tasks have completed.
    pub fn is_empty(&self) -> bool {
        self.shared_data.incoming_result_count.load(Ordering::Acquire) == 0
    }

    /// Returns the number of tasks currently running or completed with results waiting.
    pub fn len(&self) -> usize {
        self.shared_data.incoming_result_count.load(Ordering::Acquire)
    }
}

/// A set for managing multiple async tasks whose results can be retrieved as they complete.
///
/// # Overview
///
/// `AsyncJoinSet` maintains a collection of async tasks. You can:
///
/// - [`spawn`](Self::spawn) new futures, all running in parallel,
/// - retrieve each completed result via [`join_next`](Self::join_next) (async) or
///   [`join_next_blocking`](Self::join_next_blocking) (sync),
/// - [`abort_all`](Self::abort_all) if needed.
///
/// Typically, you create an `AsyncJoinSet` via
/// [`XetRuntime::async_joinset`](crate::XetRuntime::async_joinset):
///
/// ```ignore
/// let mut join_set = xet_runtime().async_joinset();
/// join_set.spawn(async { 42 });
/// join_set.spawn(async { 99 });
///
/// // In async code:
/// while let Some(val) = join_set.join_next().await.unwrap() {
///     println!("Got: {}", val);
/// }
/// ```
///
/// This is similar to [`ComputeJoinSet`], but for async tasks.
pub struct AsyncJoinSet<T> {
    join_set: tokio::task::JoinSet<T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<T> AsyncJoinSet<T>
where
    T: Send + 'static,
{
    /// Internal constructor; this should be used through xet_runtime::async_joinset.
    pub(crate) fn new(runtime_handle: tokio::runtime::Handle) -> Self {
        Self {
            join_set: tokio::task::JoinSet::new(),
            runtime_handle,
        }
    }

    /// Spawns an async future into this join set.
    ///
    /// # Example:
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use xet_runtime::{xet_runtime, AsyncJoinSet};
    /// let mut join_set = xet_runtime().async_joinset::<u64>();
    ///
    /// // Spawn an async sleep:
    /// join_set.spawn(async {
    ///     tokio::time::sleep(Duration::from_millis(10)).await;
    ///     42
    /// });
    /// ```
    pub fn spawn<F>(&mut self, task: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let _ = self.join_set.spawn_on(task, &self.runtime_handle);
    }

    /// Returns the next completed task’s result in an async manner.
    /// If there are no tasks left, returns `Ok(None)`.
    ///
    /// # Example:
    ///
    /// ```ignore
    /// use xet_runtime::{xet_runtime, AsyncJoinSet};
    ///
    /// let mut join_set = xet_runtime().async_joinset();
    ///
    /// while let Some(val) = join_set.join_next().await? {
    ///     println!("Task returned: {}", val);
    /// }
    /// ```  
    pub async fn join_next(&mut self) -> Result<Option<T>> {
        self.join_set.join_next().await.transpose().map_err(map_join_error)
    }

    /// Removes and returns the next completed task’s result, blocking
    /// until one is ready. Similar to [`join_next`](Self::join_next), but
    /// for sync code.
    ///
    /// If no tasks remain, returns `Ok(None)`.
    ///
    /// # Example:
    ///
    /// ```ignore
    /// use xet_runtime::{xet_runtime, AsyncJoinSet};
    /// let mut join_set = xet_runtime().async_joinset();
    /// // ...
    /// let next = join_set.join_next_blocking().unwrap();
    /// match next {
    ///     Some(val) => println!("Got {}", val),
    ///     None => println!("No tasks remain"),
    /// }
    /// ```
    pub fn join_next_blocking(&mut self) -> Result<Option<T>> {
        let jnh = self.join_set.join_next();
        self.runtime_handle.block_on(jnh).transpose().map_err(map_join_error)
    }

    /// Returns whether the join set is empty
    pub fn is_empty(&self) -> bool {
        self.join_set.is_empty()
    }

    /// Returns the number of tasks currently in the join set, including completed tasks with
    /// results that have not yet been retrieved.
    pub fn len(&self) -> usize {
        self.join_set.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rayon::ThreadPoolBuilder;

    use super::*;

    #[tokio::test]
    async fn test_async_join_set() {
        let handle = tokio::runtime::Handle::try_current().unwrap().clone();

        let mut join_set = AsyncJoinSet::new(handle);

        for i in 0..5 {
            join_set.spawn(async move { i * 2 });
        }

        let mut results = vec![];

        while let Some(result) = join_set.join_next().await.unwrap() {
            results.push(result);
        }

        results.sort();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_spawn_and_join_next() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(1).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        // Spawn some tasks
        for i in 0..5 {
            join_set.spawn(move || i * 2);
        }

        // Collect results
        let mut results = vec![];
        while let Ok(Some(result)) = join_set.join_next() {
            results.push(result);
        }

        // Ensure all tasks completed
        results.sort();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_spawn_and_join_next_fifo() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(1).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        // Spawn some tasks
        for i in 0..5 {
            join_set.spawn_fifo(move || i * 2);
        }

        // Collect results
        let mut results = vec![];
        while let Ok(Some(result)) = join_set.join_next() {
            results.push(result);
        }

        // Ensure all tasks completed
        results.sort();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_try_join_next() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(2).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        // Spawn a task
        join_set.spawn(|| 42);

        assert!(!join_set.is_empty());
        assert_eq!(join_set.len(), 1);

        // Allow some time for the task to complete
        std::thread::sleep(Duration::from_millis(100));

        assert!(!join_set.is_empty());
        assert_eq!(join_set.len(), 1);

        // Try to join next result
        let result = join_set.try_join_next();
        assert_eq!(result.unwrap(), Some(42));
        assert!(join_set.is_empty());
        assert_eq!(join_set.len(), 0);

        // No more tasks left
        let result = join_set.try_join_next();
        assert_eq!(result.unwrap(), None);
        assert!(join_set.is_empty());
        assert_eq!(join_set.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(2).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        assert!(join_set.is_empty());

        // Spawn a task
        join_set.spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            1
        });

        assert!(!join_set.is_empty());

        // Wait for the task to complete
        let r = join_set.join_next().unwrap();

        assert_eq!(r, Some(1));

        assert!(join_set.is_empty());

        let r = join_set.join_next().unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn test_no_block_on_empty_join_next() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(2).build().unwrap());
        let mut join_set = ComputeJoinSet::<bool>::new(thread_pool);

        // No tasks spawned
        assert_eq!(join_set.join_next().unwrap(), None);
    }

    #[test]
    fn test_multiple_tasks_1() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(4).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        // Spawn multiple tasks
        let mut reference = Vec::new();
        for i in 0..100 {
            join_set.spawn(move || i * i);
            reference.push(i * i);
        }

        let mut results = vec![];
        while let Ok(Some(result)) = join_set.join_next() {
            results.push(result);
        }

        results.sort();
        reference.sort();
        assert_eq!(results, reference);
    }
    #[test]
    fn test_multiple_tasks_2() {
        let thread_pool = Arc::new(ThreadPoolBuilder::new().num_threads(4).build().unwrap());
        let mut join_set = ComputeJoinSet::new(thread_pool);

        // Spawn multiple tasks
        let mut reference = Vec::new();
        let mut results = vec![];

        for i in 0..200 {
            join_set.spawn(move || {
                std::thread::sleep(Duration::from_millis(i % 10));
                i * i
            });

            reference.push(i * i);

            while let Some(v) = join_set.try_join_next().unwrap() {
                results.push(v);
            }
        }

        while let Ok(Some(result)) = join_set.join_next() {
            results.push(result);
        }

        results.sort();
        reference.sort();
        assert_eq!(results, reference);
    }
}
