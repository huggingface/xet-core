use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};
use tracing::info;

use crate::errors::MultithreadedRuntimeError;

/// A simple single-flight group: concurrent calls to `run` will coalesce so that
/// only one invocation of the retrieval function executes; all other callers
/// await the same result.
///
/// - The first caller becomes the "leader", executes the retrieval, and then broadcasts the value to all "followers".
/// - Followers wait on a oneshot to receive a clone of the produced value.
///
/// Notes:
/// - If the retrieval future panics, followers will receive a canceled oneshot and this function will panic in
///   followers with a clear message.
/// - If you want error handling instead of panics, change `T` to `Result<U, E>` and broadcast the result.
pub struct SingleFlightGroup<T: Clone + Send + Sync + 'static> {
    state: Arc<Mutex<Option<Vec<oneshot::Sender<T>>>>>,
}

impl<T: Clone + Send + Sync + 'static> SingleFlightGroup<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }

    /// Coalesce concurrent calls: only one invocation of `retrieval_function` runs.
    ///
    /// `retrieval_function` is only executed by the first concurrent caller. All
    /// other callers receive the produced `T` via a oneshot channel.
    pub async fn run<F, Fut>(&self, retrieval_function: F) -> std::result::Result<T, MultithreadedRuntimeError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T> + Send,
    {
        // Attempt to become the leader or register as a follower.
        let rx_opt = {
            let mut guard = self.state.lock().await;
            match *guard {
                // Someone is already computing: become a follower.
                Some(ref mut waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Some(rx)
                },
                // No one computing yet: become the leader.
                None => {
                    *guard = Some(Vec::new());
                    None
                },
            }
        };

        // If we got a receiver, we're a follower, so just await the result.
        if let Some(rx) = rx_opt {
            match rx.await {
                Ok(v) => Ok(v),
                Err(e) => {
                    let msg = format!(
                        "Singleflight leader failed before broadcasing result, possible shutdown in progress. {e:?}"
                    );
                    info!("{msg}");
                    Err(MultithreadedRuntimeError::Other(msg))
                },
            }
        } else {
            // We're the leader: get and run the retrieval *without* holding the lock.
            let value = retrieval_function().await;

            // Reacquire the lock, take waiters, and broadcast the result.
            let waiters = self.state.lock().await.take().unwrap_or_default();

            for tx in waiters {
                // Ignore send errors (receivers may have been dropped).
                let _ = tx.send(value.clone());
            }

            Ok(value)
        }
    }
}

/// Central registry of single-flight groups, keyed only by string `key`.
pub struct SingleFlightManager {
    groups: Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>,
}

impl SingleFlightManager {
    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
        }
    }

    /// Coalesces concurrent calls by `key`. A key must always be used with the same `T`
    /// unless you `clear(key)` first. If a different `T` is used for an existing key,
    /// this returns an error prompting a clear.
    pub async fn run<F, Fut, T>(&self, key: &str, retrieval_function: F) -> Result<T, MultithreadedRuntimeError>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T> + Send,
    {
        let group: Arc<SingleFlightGroup<T>> = {
            let mut map = self.groups.lock().await;
            if let Some(existing) = map.get(key) {
                let existing = existing.clone();
                match Arc::downcast::<SingleFlightGroup<T>>(existing) {
                    Ok(g) => g,
                    Err(_e) => {
                        return Err(MultithreadedRuntimeError::Other(format!(
                            "singleflight key '{key}' already registered with a different result type; \
                             call clear(\"{key}\") before reusing this key with another type"
                        )));
                    },
                }
            } else {
                let g: Arc<SingleFlightGroup<T>> = Arc::new(SingleFlightGroup::new());
                map.insert(key.to_owned(), g.clone() as Arc<dyn Any + Send + Sync>);
                g
            }
        };
        // lock released; delegate
        group.run(retrieval_function).await
    }

    /// Remove any group stored under `key`. Returns true if something was removed.
    pub async fn clear(&self, key: &str) -> bool {
        let mut map = self.groups.lock().await;
        map.remove(key).is_some()
    }
}

#[cfg(test)]
mod singleflight_tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio::time::sleep;

    use super::*; // brings SingleFlightGroup into scope
    use crate::errors::MultithreadedRuntimeError;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesces_concurrent_calls_and_returns_result() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        CALLS.store(0, Ordering::SeqCst);

        let g = Arc::new(SingleFlightGroup::<usize>::new());

        // Fire a bunch of concurrent calls; only one should execute the retrieval.
        let mut handles: Vec<JoinHandle<Result<usize, MultithreadedRuntimeError>>> = Vec::new();
        for _ in 0..32 {
            let g = g.clone();
            handles.push(tokio::spawn(async move {
                g.run(|| async {
                    // Only the leader executes this.
                    CALLS.fetch_add(1, Ordering::SeqCst);
                    // Give followers time to enqueue before we finish.
                    sleep(Duration::from_millis(50)).await;
                    42usize
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            let v = h.await.expect("task join ok").expect("singleflight Ok");
            results.push(v);
        }

        // Exactly one retrieval should have executed.
        assert_eq!(CALLS.load(Ordering::SeqCst), 1, "only one retrieval runs");

        // Everyone got the same value.
        let set: HashSet<_> = results.into_iter().collect();
        assert_eq!(set.len(), 1);
        assert_eq!(*set.iter().next().unwrap(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resets_between_sequential_runs() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        CALLS.store(0, Ordering::SeqCst);

        let g = SingleFlightGroup::<usize>::new();

        // First run.
        let v1 = g
            .run(|| async {
                CALLS.fetch_add(1, Ordering::SeqCst);
                7usize
            })
            .await
            .expect("first Ok");
        assert_eq!(v1, 7);

        // Second run should start a brand-new flight (so CALLS increments again).
        let v2 = g
            .run(|| async {
                CALLS.fetch_add(1, Ordering::SeqCst);
                9usize
            })
            .await
            .expect("second Ok");
        assert_eq!(v2, 9);

        assert_eq!(CALLS.load(Ordering::SeqCst), 2, "sequential runs start separate flights");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesces_by_key() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        CALLS.store(0, Ordering::SeqCst);

        let mgr = Arc::new(SingleFlightManager::new());
        let key = "users:123";

        // Many concurrent calls with same key -> one retrieval
        let mut handles: Vec<JoinHandle<Result<usize, MultithreadedRuntimeError>>> = Vec::new();
        for _ in 0..24 {
            let mgr = mgr.clone();
            handles.push(tokio::spawn(async move {
                mgr.run(key, || async {
                    if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                        sleep(Duration::from_millis(40)).await;
                    }
                    42usize
                })
                .await
            }));
        }

        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), 42);
        }
        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn type_mismatch_requires_clear_then_reuse_works() {
        let mgr = SingleFlightManager::new();
        let key = "same-key";

        // First use key with u32
        let v1 = mgr.run(key, || async { 7u32 }).await.unwrap();
        assert_eq!(v1, 7);

        // Reuse same key with a *different* T (String) -> expect error
        match mgr.run::<_, _, String>(key, || async { "ok".to_string() }).await {
            Err(MultithreadedRuntimeError::Other(msg)) => {
                assert!(msg.contains("already registered with a different result type"), "unexpected error: {msg}");
            },
            other => panic!("expected type-mismatch error, got {other:?}"),
        }

        // Clear and reuse the key with the new type
        assert!(mgr.clear(key).await, "clear should remove the entry");
        let s = mgr.run(key, || async { "ok".to_string() }).await.unwrap();
        assert_eq!(s, "ok".to_string());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clear_is_idempotent() {
        let mgr = SingleFlightManager::new();
        let key = "idempotent";

        // Clear when absent -> false
        assert!(!mgr.clear(key).await);

        // Populate, then clear -> true, then false again
        assert_eq!(mgr.run(key, || async { 1u64 }).await.unwrap(), 1u64);
        assert!(mgr.clear(key).await);
        assert!(!mgr.clear(key).await);
    }
}
