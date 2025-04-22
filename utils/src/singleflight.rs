//! A singleflight implementation for tokio.
//!
//! Inspired by [async_singleflight](https://crates.io/crates/async_singleflight).
//!
//! # Examples
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use futures::future::join_all;
//! use utils::singleflight::Group;
//! use xet_threadpool;
//!
//! const RES: usize = 7;
//!
//! async fn expensive_fn() -> Result<usize, ()> {
//!     tokio::time::sleep(Duration::new(1, 500)).await;
//!     Ok(RES)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let g = Arc::new(Group::<_, ()>::new());
//!     let mut handlers = Vec::new();
//!     for _ in 0..10 {
//!         let g = g.clone();
//!         handlers.push(tokio::spawn(async move {
//!             let res = g.work("key", expensive_fn()).await.0;
//!             let r = res.unwrap();
//!             println!("{}", r);
//!         }));
//!     }
//!
//!     join_all(handlers).await;
//! }
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures::future::Either;
use parking_lot::RwLock;
use pin_project::{pin_project, pinned_drop};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, Notify};
use tracing::debug;

pub use crate::errors::SingleflightError;

type SingleflightResult<T, E> = Result<T, SingleflightError<E>>;
type CallMap<T, E> = HashMap<String, Arc<Call<T, E>>>;

// Marker Traits to help make the code a bit cleaner.

/// ResultType indicates the success type of a singleflight [Group].
/// Since the actual processing might occur on a separate thread,
/// we need to type to be [Send] + [Sync]. It also needs to be [Clone]
/// so that we can clone the response across many tasks
pub trait ResultType: Send + Clone + Sync + Debug {}
impl<T: Send + Clone + Sync + Debug> ResultType for T {}

/// Indicates the Error type of a singleflight [Group].
/// The response might have been generated on a separate
/// thread, thus, we need this type to be [Send] + [Sync].
pub trait ResultError: Send + Debug + Sync {}
impl<E: Send + Debug + Sync> ResultError for E {}

/// Futures provided to a singleflight Group must produce a [Result<T, E>]
/// for some T, E. This future must also be [Send]
/// as it could be spawned as a tokio task.
pub trait TaskFuture<T, E>: Future<Output = Result<T, E>> + Send {}
impl<T, E, F: Future<Output = Result<T, E>> + Send> TaskFuture<T, E> for F {}

/// Call represents the (eventual) results of running some Future.
///
/// It consists of a condition variable that can be waited upon until the
/// owner task [completes](Call::complete) it.
///
/// Tasks can get the Call's result using [get_future](Call::get_future)
/// to get a Future to await. Or they can call [get](Call::get)
/// to try and get the result synchronously if the Call is already complete.
#[derive(Debug, Clone)]
struct Call<T, E>
where
    T: ResultType,
    E: ResultError,
{
    // The condition variable
    nt: Arc<Notify>,

    // The result of the operation. Kept under a RWLock that is expected
    // to be write-once, read-many.
    // We use a lock instead of an AtomicPtr since updating the result and
    // notifying the waiters needs to be atomic to avoid tasks missing the
    // notification or to avoid tasks reading an empty value.
    //
    // Also important to note is that this lock is synchronous as we need
    // to be able to store the value in the [OwnerTask::drop] function if
    // the underlying future panics. Thus, complete() must be synchronous.
    // This is ok since we are never holding the mutex across an await
    // boundary (all functions are synchronous), and the critical section
    // is fast.
    res: Arc<RwLock<Option<SingleflightResult<T, E>>>>,

    // Number of tasks that were waiting
    num_waiters: Arc<AtomicU16>,
}

impl<T, E> Call<T, E>
where
    T: ResultType,
    E: ResultError,
{
    fn new() -> Self {
        Self {
            nt: Arc::new(Notify::new()),
            res: Arc::new(RwLock::new(None)),
            num_waiters: Arc::new(AtomicU16::new(0)),
        }
    }

    /// Completes the Call. This involves storing the provided result into the Call
    /// and notifying all waiters that there is a value.
    fn complete(&self, res: SingleflightResult<T, E>) {
        // write-lock
        let mut val = self.res.write();
        *val = Some(res);
        self.nt.notify_waiters();
        let num_waiters = self.num_waiters.load(Ordering::SeqCst);
        debug!("Completed Call with: {} waiters", num_waiters);
    }

    /// Gets a Future that can be awaited to get the singleflight results, whenever that
    /// might occur.
    fn get_future(&self) -> impl Future<Output = SingleflightResult<T, E>> + '_ {
        // read-lock
        let res = self.res.read();
        if let Some(result) = res.clone() {
            // we already have the result, provide it back to the caller.
            debug!("Call already completed");
            Either::Left(async move { result })
        } else {
            // no result yet, we are a waiter task.
            self.num_waiters.fetch_add(1, Ordering::SeqCst);
            debug!("Adding to Call's Notify");

            // Note that the `notified()` needs to be performed outside of the async
            // block since we need to register our waiting within this read-lock
            // or else, we might miss the owner task's notification.
            let notified = self.nt.notified();
            Either::Right(async move {
                notified.await;
                self.get()
            })
        }
    }

    /// Gets the result for the Call if set.
    /// If not set, then [SingleflightError::NoResult] is returned
    fn get(&self) -> SingleflightResult<T, E> {
        let res = self.res.read();
        res.clone().unwrap_or(Err(SingleflightError::NoResult))
    }
}

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Debug)]
pub struct Group<T, E>
where
    T: ResultType + 'static,
    E: ResultError,
{
    call_map: Arc<Mutex<CallMap<T, E>>>,
    _marker: PhantomData<fn(E)>,
}

impl<T, E: 'static> Default for Group<T, E>
where
    T: ResultType + 'static,
    E: ResultError,
{
    fn default() -> Self {
        Self {
            call_map: Arc::new(Default::default()),
            _marker: Default::default(),
        }
    }
}

impl<T, E: 'static> Group<T, E>
where
    T: ResultType + 'static,
    E: ResultError,
{
    /// Create a new Group to do work with.
    pub fn new() -> Group<T, E> {
        Self::default()
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    /// The second return value indicates whether the call is the owner.
    ///
    /// On error, the owner will receive the original error returned from the function
    /// as a SingleflightError::InternalError, all waiters will receive a copy of the
    /// error message wrapped in a SingleflightError::WaiterInternalError.
    /// This is due to the fact that most error types don't implement Clone (e.g. anyhow::Error)
    /// and thus we can't clone the original error for all of the waiters.
    pub async fn work(
        &self,
        key: &str,
        fut: impl TaskFuture<T, E> + 'static,
    ) -> (Result<T, SingleflightError<E>>, bool) {
        // Get the call to use and a handle for retrieving the results
        let (call, created) = self.get_call_or_create(key).await;
        let results_future = call.get_future();

        if created {
            // spawn the owner task and wait
            let owner_task = OwnerTask::new(fut, call.clone());
            let owner_handle = Handle::current().spawn(owner_task);

            // wait for the owner task and results to come back
            let (handle_result, future_result) = tokio::join!(owner_handle, results_future);
            let result = handle_result
                .map_err(|e| SingleflightError::JoinError(e.to_string()))
                .and(future_result);

            // since we created the call, remove it from the map
            if let Err(e) = self.remove_call(key).await {
                return (Err(e), true);
            }
            (result, true)
        } else {
            (results_future.await, false)
        }
    }

    /// Like work but only returns the result, dumps the bool result value
    pub async fn work_dump_caller_info(
        &self,
        key: &str,
        fut: impl TaskFuture<T, E> + 'static,
    ) -> Result<T, SingleflightError<E>> {
        let (result, _) = self.work(key, fut).await;
        result
    }

    /// Gets the [Call] to use from the call_map or else inserts a new Call
    /// into the map.  
    ///
    /// Returns the [Call] that should be used and whether it was created or
    /// not.   
    async fn get_call_or_create(&self, key: &str) -> (Arc<Call<T, E>>, bool) {
        let mut m = self.call_map.lock().await;
        if let Some(c) = m.get(key).cloned() {
            (c, false)
        } else {
            let c = Arc::new(Call::new());
            let our_call = c.clone();
            m.insert(key.to_owned(), c);
            (our_call, true)
        }
    }

    /// Removes the [Call] associated with the Key. If there is no such [Call],
    /// then an error is returned.
    async fn remove_call(&self, key: &str) -> SingleflightResult<(), E> {
        let mut m = self.call_map.lock().await;
        m.remove(key).ok_or(SingleflightError::CallMissing)?;
        Ok(())
    }
}

/// Defines a task to own the poll'ing of the Future and ensure that the
/// call is updated (i.e. result stored and waiters notified) when the
/// Future completes (even if the future panics).
///
/// We can guarantee that the [Call] gets notified even during a Panic
/// since tokio tasks will catch panics and call the `drop()` function.
///
/// For more info, see: https://github.com/tokio-rs/tokio/blob/4eed411519783ef6f58cbf74f886f91142b5cfa6/tokio/src/runtime/task/harness.rs#L453-L459
/// and the discussion on: https://users.rust-lang.org/t/how-panic-calls-drop-functions/53663/8
///
/// Pin'ed since it is a Future implementation.
#[pin_project(PinnedDrop)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct OwnerTask<T, E, F>
where
    T: ResultType,
    E: ResultError,
    F: TaskFuture<T, E>,
{
    #[pin]
    fut: F,
    got_response: AtomicBool,
    call: Arc<Call<T, E>>,
}

impl<T, E, F> OwnerTask<T, E, F>
where
    T: ResultType,
    E: ResultError,
    F: TaskFuture<T, E>,
{
    fn new(fut: F, call: Arc<Call<T, E>>) -> Self {
        Self {
            fut,
            got_response: AtomicBool::new(false),
            call,
        }
    }
}

impl<T, E, F> Future for OwnerTask<T, E, F>
where
    T: ResultType,
    E: ResultError,
    F: TaskFuture<T, E>,
{
    type Output = Result<T, SingleflightError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res: Result<T, E> = ready!(this.fut.poll(cx));
        let res = res.map_err(|e| SingleflightError::InternalError(e));
        // we have a result, so store it into our call and notify all waiters.
        let call = this.call;
        this.got_response.store(true, Ordering::SeqCst);
        call.complete(res.clone());
        Poll::Ready(res)
    }
}

#[pinned_drop]
impl<T, E, F> PinnedDrop for OwnerTask<T, E, F>
where
    T: ResultType,
    E: ResultError,
    F: TaskFuture<T, E>,
{
    fn drop(self: Pin<&mut Self>) {
        // If we don't have a result stored in the call, then we panicked and
        // should store an error, notifying all waiters of the panic.
        let this = self.project();
        if !this.got_response.load(Ordering::SeqCst) {
            let call = this.call;
            call.complete(Err(SingleflightError::OwnerPanicked))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use futures::stream::iter;
    use futures::StreamExt;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc::error::SendError;
    use tokio::sync::mpsc::{channel, Sender};
    use tokio::sync::{Mutex, Notify};
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use xet_threadpool::ThreadPool;

    use super::Group;
    use crate::errors::SingleflightError;
    use crate::singleflight::{Call, OwnerTask};

    /// A period of time for waiters to wait for a notification from the owner
    /// task. This is expected to be sufficient time for the test futures to
    /// complete. Thus, if we hit this timeout, then likely, there is something
    /// wrong with the [Call] notifications.
    const WAITER_TIMEOUT: Duration = Duration::from_millis(100);

    const RES: usize = 7;

    async fn return_res() -> Result<usize, ()> {
        Ok(RES)
    }

    async fn expensive_fn(x: Arc<AtomicU32>, resp: usize) -> Result<usize, ()> {
        tokio::time::sleep(Duration::new(1, 0)).await;
        x.fetch_add(1, Ordering::SeqCst);
        Ok(resp)
    }

    #[test]
    fn test_simple_with_threadpool() {
        let threadpool = Arc::new(ThreadPool::new().unwrap());
        let g = Group::new();
        let res = threadpool
            .external_run_async_task(async move { g.work("key", return_res()).await })
            .unwrap()
            .0;
        let r = res.unwrap();
        assert_eq!(r, RES);
    }

    #[tokio::test]
    async fn test_simple() {
        let g = Group::new();
        let res = g.work("key", return_res()).await.0;
        let r = res.unwrap();
        assert_eq!(r, RES);
    }

    #[test]
    fn test_multiple_threads_with_threadpool() {
        let times_called = Arc::new(AtomicU32::new(0));
        let threadpool = Arc::new(ThreadPool::new().unwrap());
        let g: Arc<Group<usize, ()>> = Arc::new(Group::new());
        let mut handlers: Vec<JoinHandle<(usize, bool)>> = Vec::new();
        let threadpool_ = threadpool.clone();
        let tasks = async move {
            for _ in 0..10 {
                let g = g.clone();
                let counter = times_called.clone();
                handlers.push(threadpool_.spawn(async move {
                    let tup = g.work("key", expensive_fn(counter, RES)).await;
                    let res = tup.0;
                    let fn_response = res.unwrap();
                    (fn_response, tup.1)
                }));
            }

            let num_callers = join_all(handlers)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .filter(|(val, is_caller)| {
                    assert_eq!(*val, RES);
                    *is_caller
                })
                .count();
            assert_eq!(1, num_callers);
            assert_eq!(1, times_called.load(Ordering::SeqCst));
        };
        let _ = threadpool.external_run_async_task(tasks).unwrap();
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        let times_called = Arc::new(AtomicU32::new(0));
        let g: Arc<Group<usize, ()>> = Arc::new(Group::new());
        let mut handlers: Vec<JoinHandle<(usize, bool)>> = Vec::new();
        for _ in 0..10 {
            let g = g.clone();
            let counter = times_called.clone();
            handlers.push(Handle::current().spawn(async move {
                let tup = g.work("key", expensive_fn(counter, RES)).await;
                let res = tup.0;
                let fn_response = res.unwrap();
                (fn_response, tup.1)
            }));
        }

        let num_callers = join_all(handlers)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .filter(|(val, is_caller)| {
                assert_eq!(*val, RES);
                *is_caller
            })
            .count();
        assert_eq!(1, num_callers);
        assert_eq!(1, times_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_error() {
        let times_called = Arc::new(AtomicU32::new(0));

        async fn expensive_error_fn(x: Arc<AtomicU32>) -> Result<usize, &'static str> {
            tokio::time::sleep(Duration::new(1, 500)).await;
            x.fetch_add(1, Ordering::SeqCst);
            Err("Error")
        }

        let g: Arc<Group<usize, &'static str>> = Arc::new(Group::new());
        let mut handlers = Vec::new();

        for _ in 0..10 {
            let g = g.clone();
            let counter = times_called.clone();
            handlers.push(Handle::current().spawn(async move {
                let tup = g.work("key", expensive_error_fn(counter)).await;
                let res = tup.0;
                assert!(res.is_err());
                tup.1
            }));
        }

        let num_callers = join_all(handlers).await.into_iter().map(|r| r.unwrap()).filter(|b| *b).count();
        assert_eq!(1, num_callers);
        assert_eq!(1, times_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let times_called_x = Arc::new(AtomicU32::new(0));
        let times_called_y = Arc::new(AtomicU32::new(0));

        let mut handlers1 = call_success_n_times(5, "key", times_called_x.clone(), 7);
        let mut handlers2 = call_success_n_times(5, "key2", times_called_y.clone(), 13);
        handlers1.append(&mut handlers2);
        let count_x = AtomicU32::new(0);
        let count_y = AtomicU32::new(0);

        let num_callers = join_all(handlers1)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .filter(|(val, is_caller)| {
                if *val == 7 {
                    count_x.fetch_add(1, Ordering::SeqCst);
                } else if *val == 13 {
                    count_y.fetch_add(1, Ordering::SeqCst);
                } else {
                    panic!("joined a number not expected: {}", *val);
                }
                *is_caller
            })
            .count();
        assert_eq!(2, num_callers);
        assert_eq!(5, count_x.load(Ordering::SeqCst));
        assert_eq!(5, count_y.load(Ordering::SeqCst));
        assert_eq!(1, times_called_x.load(Ordering::SeqCst));
        assert_eq!(1, times_called_y.load(Ordering::SeqCst));
    }

    // must be run in a #[tokio::test]
    fn call_success_n_times(times: usize, key: &str, c: Arc<AtomicU32>, val: usize) -> Vec<JoinHandle<(usize, bool)>> {
        let g: Arc<Group<usize, ()>> = Arc::new(Group::new());
        let mut handlers = Vec::new();
        for _ in 0..times {
            let g = g.clone();
            let counter = c.clone();
            let k = key.to_owned();
            handlers.push(Handle::current().spawn(async move {
                let tup = g.work(k.as_str(), expensive_fn(counter, val)).await;
                let res = tup.0;
                let fn_response = res.unwrap();
                (fn_response, tup.1)
            }));
        }
        handlers
    }

    #[tokio::test]
    async fn test_owner_task_future_impl() {
        const VAL: i32 = 10;
        let future = async { Ok::<i32, String>(VAL) };
        let call = Arc::new(Call::new());
        let owner_task = OwnerTask::new(future, call.clone());
        let result = tokio::spawn(owner_task).await;
        assert_eq!(VAL, result.unwrap().unwrap());
        assert_eq!(VAL, call.get().unwrap());
    }

    #[tokio::test]
    async fn test_owner_task_future_notify() {
        const VAL: i32 = 10;
        let future = async { Ok::<i32, String>(VAL) };
        let call = Arc::new(Call::new());
        let call_waiter = call.clone();
        let waiter_task = async move {
            let waiter_future = call_waiter.get_future();
            assert_eq!(VAL, waiter_future.await.unwrap());
        };
        let waiter_handle = tokio::spawn(waiter_task);
        let owner_task = OwnerTask::new(future, call.clone());
        let result = tokio::spawn(owner_task).await;
        timeout(WAITER_TIMEOUT, waiter_handle).await.unwrap().unwrap();
        assert_eq!(VAL, result.unwrap().unwrap());
        assert_eq!(VAL, call.get().unwrap());
        assert_eq!(1, call.num_waiters.load(Ordering::SeqCst)) // we should have had 1 waiter
    }

    #[tokio::test]
    async fn test_owner_task_future_panic() {
        let future = async { panic!("failing task") };
        let call = Arc::new(Call::<i32, String>::new());
        let call_waiter = call.clone();
        let waiter_task = async move {
            let waiter_future = call_waiter.get_future();
            let result = waiter_future.await;
            assert!(matches!(result, Err(SingleflightError::OwnerPanicked)));
        };
        let waiter_handle = tokio::spawn(waiter_task);

        let owner_task = OwnerTask::new(future, call.clone());
        let result = tokio::spawn(owner_task).await;
        assert!(result.is_err());
        timeout(WAITER_TIMEOUT, waiter_handle).await.unwrap().unwrap();
        assert_eq!(1, call.num_waiters.load(Ordering::SeqCst)) // we should have had 1 waiter
    }

    #[tokio::test]
    async fn test_deadlock() {
        /*
        Each spawned tokio task is expected to send some ints to the main task via a bounded buffer.
        The ints are fetched using a futures::Buffered stream over some future. These futures will
        call into singleflight to fetch an int.

        To setup the deadlock, we have 3 tasks: main, t1, and t2 with the following dependency:
        main is waiting to read from t1, t1 is a waiter on some element that t2 is working on,
        t2 is blocked writing to the buffer (i.e. waiting for main to read).

        to accomplish this, we spawn t1, t2. Each will start up their sub-tasks (3 at a time).
        However, there is a dependency where task2[2] runs for some int x and task1[4] needs
        that value, thus triggering a dependency within singleflight.
         */

        let group: Arc<Group<usize, ()>> = Arc::new(Group::new());
        // communication channels
        let (send1, mut recv1) = channel::<usize>(1);
        let (send2, mut recv2) = channel::<usize>(1);
        // Items to return on the channels from the tasks.
        let vals1: Vec<usize> = vec![1, 2, 3, 4, SHARED_ITEM];
        let vals2: Vec<usize> = vec![6, 7, SHARED_ITEM, 8, 9];

        // waiters allows us to define the order that sub-tasks run in the underlying tasks.
        // We need this for 2 reasons:
        // 1. SHARED_ITEM sub-task in t2 needs to block until we can ensure that it has a waiter
        // 2. vals2[1] needs to block to ensure that t2's SHARED_ITEM starts.
        let waiters: Arc<Mutex<HashMap<usize, Arc<Notify>>>> = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut guard = waiters.lock().await;
            guard.insert(vals2[1], Arc::new(Notify::new()));
            guard.insert(SHARED_ITEM, Arc::new(Notify::new()));
        }

        // spawn tasks
        let t1 = Handle::current().spawn(run_task(1, group.clone(), waiters.clone(), send1, false, vals1.clone()));
        let t2 = Handle::current().spawn(run_task(2, group.clone(), waiters.clone(), send2, true, vals2.clone()));

        // try to receive all the values from task1 without getting stuck.
        for (i, expected_val) in vals1.into_iter().enumerate() {
            if i == 3 {
                // resume vals2[1] to allow task2 to get "stuck" waiting on send2.send()
                println!("[main] notifying val: {}", vals2[1]);
                let guard = waiters.lock().await;
                guard.get(&vals2[1]).unwrap().notify_one();
                println!("[main] notified val: {}", vals2[1])
            }
            if i == 4 {
                // resume task2's SHARED_ITEM sub-task since we now have a waiter (i.e. vals1[4]).
                println!("[main] notifying val: {}", SHARED_ITEM);
                let guard = waiters.lock().await;
                guard.get(&SHARED_ITEM).unwrap().notify_one();
                println!("[main] notified val: {}", SHARED_ITEM);
            }
            println!("[main] getting t1[{}]", i);
            let res = timeout(WAITER_TIMEOUT, recv1.recv())
                .await
                .map_err(|_| format!("Timed out on task1 waiting for val: {}. Likely deadlock.", expected_val));
            let val = res.unwrap().unwrap();
            println!("[main] got val: {} from t1[{}]", val, i);
            assert_eq!(expected_val, val);
        }

        // try to receive all the values from task2 without getting stuck.
        for expected_val in vals2 {
            let res = timeout(WAITER_TIMEOUT, recv2.recv())
                .await
                .map_err(|_| format!("Timed out on task2 waiting for val: {}. Likely deadlock.", expected_val));
            let val = res.unwrap().unwrap();
            assert_eq!(expected_val, val);
        }

        // make sure t1,t2 completed successfully.
        t1.await.unwrap().unwrap();
        t2.await.unwrap().unwrap();
    }

    const SHARED_ITEM: usize = 5;

    async fn run_task(
        id: i32,
        g: Arc<Group<usize, ()>>,
        waiters: Arc<Mutex<HashMap<usize, Arc<Notify>>>>,
        send_chan: Sender<usize>,
        should_own: bool,
        vals: Vec<usize>,
    ) -> Result<(), SendError<usize>> {
        // create a buffered stream that will run at most 3 sub-tasks concurrently.
        let mut strm = iter(vals.into_iter().map(|v| {
            let g = g.clone();
            let waiters = waiters.clone();
            // get the sub-task for the given item.
            async move {
                println!("[task: {}] running task for: {}", id, v);
                let (res, is_owner) = g.work(format!("{}", v).as_str(), run_fut(v, waiters)).await;
                println!("[task: {}] completed task for: {}, is_owner: {}", id, v, is_owner);
                if v == SHARED_ITEM {
                    assert_eq!(should_own, is_owner);
                }
                res.unwrap()
            }
        }))
        .buffered(3);

        while let Some(val) = strm.next().await {
            println!("[task: {}] sending next element: {}", id, val);
            send_chan.send(val).await?;
            println!("[task: {}] sent next element: {}", id, val);
        }
        println!("[task: {}] done executing", id);
        Ok(())
    }

    async fn run_fut(v: usize, waiters: Arc<Mutex<HashMap<usize, Arc<Notify>>>>) -> Result<usize, ()> {
        let waiter = {
            let x = waiters.lock().await;
            x.get(&v).cloned()
        };
        // wait for the main task to tell us to proceed.
        if let Some(waiter) = waiter {
            println!("val: {}, waiting for signal", v);
            waiter.notified().await;
            println!("val: {}, woke up from signal", v);
        }
        Ok(v)
    }
}
