use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;

type CachedValueContainer = Arc<Mutex<Option<Box<dyn Any + Send + Sync>>>>;

/// A utility class to hold a cached value in a way that can be associated with the runtime.
#[derive(Default, Debug)]
pub struct AnyCache {
    value_cache: Mutex<HashMap<String, CachedValueContainer>>,
}

impl AnyCache {
    pub fn new() -> Self {
        Self {
            value_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Returns a type- and key-specific string for use in the map.
    fn make_typed_key<T: 'static>(base_key: &str) -> String {
        format!("{}::{:?}", base_key, TypeId::of::<T>())
    }

    /// Get or create a cached value for `key` and type `T`.
    ///
    /// * Per-key+type single-flight: at most one `creation_function` runs concurrently for the same `(key, T)` pair.
    /// * On type mismatch, the new value replaces the old.
    pub async fn get_cached_value<T, E, F, Fut>(&self, key: &str, creation_function: F) -> Result<T, E>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
    {
        let typed_key = Self::make_typed_key::<T>(key);

        // Grab or create the per-(key,type) mutex slot.
        let cell = {
            let mut map = self.value_cache.lock().await;
            map.entry(typed_key).or_insert_with(|| Arc::new(Mutex::new(None))).clone()
        };

        // Lock this key+type cell; hold until value ready.
        let mut slot = cell.lock().await;

        // If we already have the right type stored, return clone.
        if let Some(ref boxed) = *slot {
            if let Some(v) = boxed.downcast_ref::<T>() {
                return Ok(v.clone());
            }
        }

        // Otherwise, create and store.
        let built = creation_function().await?;
        *slot = Some(Box::new(built.clone()) as Box<dyn Any + Send + Sync>);
        Ok(built)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::task::JoinSet;
    use tokio::time::{sleep, Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn returns_same_value_without_recreating() {
        let cache = AnyCache::new();
        static CREATED: AtomicUsize = AtomicUsize::new(0);

        let v1: String = cache
            .get_cached_value("k", || async {
                CREATED.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>("hello".to_owned())
            })
            .await
            .unwrap();

        let v2: String = cache
            .get_cached_value("k", || async {
                // would fail if called again
                Err::<String, _>(())
            })
            .await
            .unwrap();

        assert_eq!(v1, "hello");
        assert_eq!(v1, v2);
        assert_eq!(CREATED.load(Ordering::SeqCst), 1, "creator should run only once");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_serializes_concurrent_creators_for_same_key_and_type() {
        let cache = Arc::new(AnyCache::new());
        let creation_count = Arc::new(AtomicUsize::new(0));

        // Fire a bunch of concurrent lookups for the *same* (key, type).
        let mut joinset = JoinSet::new();

        for _ in 0..32 {
            let creation_count_ = creation_count.clone();

            let slow_create = || async move {
                creation_count_.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(100)).await;
                Ok::<_, ()>(42usize)
            };

            let c = cache.clone();
            joinset.spawn(async move { c.get_cached_value("answer", slow_create).await.unwrap() });
        }

        for r in joinset.join_all().await {
            assert_eq!(r, 42usize);
        }
        assert_eq!(creation_count.load(Ordering::SeqCst), 1, "only one creator should have run");
    }

    #[tokio::test]
    async fn different_types_same_key_do_not_collide_or_block() {
        let cache = AnyCache::new();

        // Make creators that take noticeable time so we can measure overlap.
        let t0 = Instant::now();
        let f1 = cache.get_cached_value("shared-key", || async {
            sleep(Duration::from_millis(120)).await;
            Ok::<_, ()>("stringy".to_string())
        });

        let f2 = cache.get_cached_value("shared-key", || async {
            sleep(Duration::from_millis(120)).await;
            Ok::<_, ()>(999u32)
        });

        let (s, n) = tokio::join!(f1, f2);
        let elapsed = t0.elapsed();

        assert_eq!(s.unwrap(), "stringy");
        assert_eq!(n.unwrap(), 999u32);

        // If they were serialized by the same per-key mutex, we'd expect ~240ms.
        // Parallel (separate (key,T) locks) should be close to ~120ms.
        assert!(
            elapsed < Duration::from_millis(200),
            "operations should run in parallel for different types; elapsed={:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn errors_are_propagated_and_not_cached() {
        let cache = AnyCache::new();
        let tries: AtomicUsize = AtomicUsize::new(0);

        // First attempt errors
        let err = cache
            .get_cached_value::<u64, &'static str, _, _>("err-key", || async {
                tries.fetch_add(1, Ordering::SeqCst);
                Err("boom")
            })
            .await
            .unwrap_err();
        assert_eq!(err, "boom");
        assert_eq!(tries.load(Ordering::SeqCst), 1);

        // Second attempt succeeds and should run creator again (not cached error)
        let v = cache
            .get_cached_value("err-key", || async {
                tries.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>(7u64)
            })
            .await
            .unwrap();
        assert_eq!(v, 7);
        assert_eq!(tries.load(Ordering::SeqCst), 2);

        // Third attempt should read from cache without calling creator
        let v2 = cache
            .get_cached_value("err-key", || async {
                tries.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>(999u64)
            })
            .await
            .unwrap();
        assert_eq!(v2, 7);
        assert_eq!(tries.load(Ordering::SeqCst), 2, "no additional creator run after caching");
    }
}
