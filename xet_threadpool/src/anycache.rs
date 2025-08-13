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
