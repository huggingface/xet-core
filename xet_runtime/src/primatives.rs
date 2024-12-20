use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::FutureExt;
use rayon::ThreadPool;

use crate::errors::{map_join_error, Result, XetRuntimeError};

pub struct ComputeJoinHandle<T: Send + Sync> {
    task_result: oneshot::Receiver<T>, /* Use the other join handle to figure out when the previous job is
                                        * done. */
}

impl<T: Send + Sync> ComputeJoinHandle<T> {
    pub(crate) fn create() -> (Self, oneshot::Sender<T>) {
        let (sender, task_result) = oneshot::channel();
        (Self { task_result }, sender)
    }

    /// Blocks the current thread until the task has finished.  Do not use in async code; in async code,
    /// use .await on this to await the result.
    pub fn join(self) -> Result<T> {
        self.task_result
            .recv()
            .map_err(|e| XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}")))
    }

    pub fn try_join(&self) -> Result<Option<T>> {
        match self.task_result.try_recv() {
            Err(oneshot::TryRecvError::Empty) => Ok(None),
            Err(e) => Err(XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}"))),
            Ok(r) => Ok(Some(r)),
        }
    }
}

// Implement .await as a trait
impl<T: Send + Sync> Future for ComputeJoinHandle<T> {
    type Output = Result<T>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.task_result
            .poll_unpin(cx)
            .map_err(|e| XetRuntimeError::Other(format!("ComputeJoinHandle: {e:?}")))
    }
}

/// Join handle for a task on the async runtime.  
pub struct AsyncJoinHandle<T: Send + Sync> {
    // Just wrap the inner struct and translates any errors to XetRuntimeErrors.
    inner: tokio::task::JoinHandle<T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<T: Send + Sync> AsyncJoinHandle<T> {
    pub(crate) fn new(runtime_handle: tokio::runtime::Handle, inner: tokio::task::JoinHandle<T>) -> Self {
        Self { inner, runtime_handle }
    }

    /// Join on an async task from the sync runtime.  Use join_handle.await it the
    /// async context.  
    pub fn join(self) -> Result<T> {
        self.runtime_handle.block_on(self.inner).map_err(map_join_error)
    }
}

// Implements .await on the join handle.
impl<T: Send + Sync> Future for AsyncJoinHandle<T> {
    type Output = Result<T>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.inner.poll_unpin(cx).map_err(map_join_error)
    }
}

// Join Sets

struct _ComputeJoinSetSharedData<T: Send + Sync + 'static> {
    // The channel on which to send new results.
    result_sender: crossbeam::channel::Sender<T>,

    // The total number of results to expect, either in the channel or coming from
    // still-running tasks
    incoming_result_count: AtomicUsize,
}

pub struct ComputeJoinSet<T: Send + Sync + 'static> {
    thread_pool: Arc<ThreadPool>,

    receiver: crossbeam::channel::Receiver<T>,

    shared_data: Arc<_ComputeJoinSetSharedData<T>>,
}

impl<T: Send + Sync + 'static> ComputeJoinSet<T> {
    /// Creates a new `ComputeJoinSet` with the provided Rayon `ThreadPool`.
    pub(crate) fn new(thread_pool: Arc<ThreadPool>) -> Self {
        let (result_sender, receiver) = crossbeam::channel::unbounded();
        Self {
            thread_pool,
            receiver,
            shared_data: Arc::new(_ComputeJoinSetSharedData {
                result_sender,
                incoming_result_count: AtomicUsize::new(0),
            }),
        }
    }

    /// Spawns a new task onto the Rayon threadpool.
    pub fn spawn<F>(&self, task: F)
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let shared_data = self.shared_data.clone();

        // One more task to expect incoming.
        shared_data.incoming_result_count.fetch_add(1, Ordering::SeqCst);

        self.thread_pool.spawn(move || {
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
        });
    }

    /// Tries to get the next completed task result if available without blocking;
    /// If not present, then
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

    /// Waits for the next completed task result, blocking if necessary.
    /// Returns `None` if all tasks are complete.
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

/// An async wrapper class around the tokio JoinSet that provides two additional things:
/// - tasks are spawned explicitly on the runtime handle.
/// - This will allow us to swap out tokio perhaps down the road if we want to...
pub struct AsyncJoinSet<T> {
    join_set: tokio::task::JoinSet<T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<T> AsyncJoinSet<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(runtime_handle: tokio::runtime::Handle) -> Self {
        Self {
            join_set: tokio::task::JoinSet::new(),
            runtime_handle,
        }
    }

    /// Spawns a task on the runtime associated with this wrapper
    pub fn spawn<F>(&mut self, task: F) -> tokio::task::AbortHandle
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        self.join_set.spawn_on(task, &self.runtime_handle)
    }

    /// Aborts all tasks in the join set
    pub fn abort_all(&mut self) {
        self.join_set.abort_all();
    }

    /// Removes and returns the next task result from the join set
    pub async fn join_next(&mut self) -> Result<Option<T>> {
        self.join_set.join_next().await.transpose().map_err(map_join_error)
    }

    /// Removes and returns the next task result from the join set; to be called
    /// only from outside the async runtime.
    pub fn join_next_blocking(&mut self) -> Result<Option<T>> {
        let jnh = self.join_set.join_next();
        self.runtime_handle.block_on(jnh).transpose().map_err(map_join_error)
    }

    /// Returns whether the join set is empty
    pub fn is_empty(&self) -> bool {
        self.join_set.is_empty()
    }

    /// Returns the number of tasks currently in the join set
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
