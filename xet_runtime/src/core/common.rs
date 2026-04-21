use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::config::XetConfig;
use crate::utils::adjustable_semaphore::AdjustableSemaphore;

/// Holds shared state that is common across the entire context.
///
/// Accessible via `ctx.common` on a [`super::XetContext`].
pub struct XetCommon {
    /// Limits the number of files being ingested (cleaned/uploaded) concurrently.
    pub file_ingestion_semaphore: Arc<Semaphore>,

    /// Limits the number of files being downloaded concurrently.
    pub file_download_semaphore: Arc<Semaphore>,

    /// Limits total memory used for buffering data during reconstruction downloads.
    pub reconstruction_download_buffer: Arc<AdjustableSemaphore>,

    /// Tracks the number of currently active file downloads for dynamic buffer scaling.
    pub active_downloads: Arc<AtomicU64>,

    /// Type-erased cache for runtime-scoped resources. Subsystems store their own
    /// caches here (keyed by a unique string) instead of using process-global statics,
    /// so everything is cleaned up when the runtime drops.
    runtime_cache: Mutex<HashMap<String, Box<dyn Any + Send + Sync>>>,
}

impl XetCommon {
    /// Creates a new `XetCommon` instance with the given configuration.
    pub fn new(config: &XetConfig) -> Self {
        Self {
            file_ingestion_semaphore: Arc::new(Semaphore::new(config.data.max_concurrent_file_ingestion)),
            file_download_semaphore: Arc::new(Semaphore::new(config.data.max_concurrent_file_downloads)),
            reconstruction_download_buffer: {
                let base = config.reconstruction.download_buffer_size.as_u64();
                let limit = config.reconstruction.download_buffer_limit.as_u64();
                AdjustableSemaphore::new(base, (base, limit))
            },
            active_downloads: Arc::new(AtomicU64::new(0)),
            runtime_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieves a cached value by key, or creates and stores it using `create`.
    ///
    /// Values are stored as type-erased `Box<dyn Any>` and recovered via `downcast_ref`.
    /// Typical stored types are `Arc<Mutex<...>>` or `Arc<RwLock<...>>`, making
    /// the clone cheap (just an Arc bump).
    pub fn cache_get_or_create<T, F>(&self, key: &str, create: F) -> T
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce() -> T,
    {
        let mut guard = self.runtime_cache.lock().unwrap();
        if let Some(existing) = guard.get(key)
            && let Some(val) = existing.downcast_ref::<T>()
        {
            return val.clone();
        }
        let value = create();
        guard.insert(key.to_string(), Box::new(value.clone()));
        value
    }

    /// Gets or creates a reqwest client, using a tag to identify the client type.
    ///
    /// # Arguments
    /// * `tag` - A string identifier for the client (e.g., "tcp" for regular, socket path for UDS)
    /// * `create_client_fn` - A function that creates the client if needed
    ///
    /// # Returns
    /// Returns a clone of the cached client if the tag matches, or creates a new client if the tag differs.
    pub fn get_or_create_reqwest_client<F>(&self, tag: String, create_client_fn: F) -> crate::error::Result<Client>
    where
        F: FnOnce() -> std::result::Result<Client, reqwest::Error>,
    {
        let client_cache: Arc<Mutex<Option<(String, Client)>>> =
            self.cache_get_or_create("global_reqwest_client", || Arc::new(Mutex::new(None)));
        let mut guard = client_cache.lock()?;

        match guard.as_ref() {
            Some((cached_tag, cached_client)) if cached_tag == &tag => Ok(cached_client.clone()),
            _ => {
                let new_client = create_client_fn()?;
                *guard = Some((tag, new_client.clone()));
                Ok(new_client)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn test_get_or_create_reqwest_client_caches_by_tag() {
        let common = XetCommon::new(&XetConfig::new());
        let call_count = AtomicUsize::new(0);

        let _client1 = common
            .get_or_create_reqwest_client("test-tag".to_string(), || {
                call_count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().build()
            })
            .unwrap();

        let _client2 = common
            .get_or_create_reqwest_client("test-tag".to_string(), || {
                call_count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().build()
            })
            .unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_get_or_create_reqwest_client_creates_new_for_different_tag() {
        let common = XetCommon::new(&XetConfig::new());
        let call_count = AtomicUsize::new(0);

        let _client1 = common
            .get_or_create_reqwest_client("tag1".to_string(), || {
                call_count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().user_agent("client1").build()
            })
            .unwrap();

        let _client2 = common
            .get_or_create_reqwest_client("tag2".to_string(), || {
                call_count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().user_agent("client2").build()
            })
            .unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_replaces_client_when_tag_changes() {
        let common = XetCommon::new(&XetConfig::new());

        let _client1 = common
            .get_or_create_reqwest_client("tcp".to_string(), || {
                reqwest::Client::builder().user_agent("tcp-client").build()
            })
            .unwrap();

        let _client2 = common
            .get_or_create_reqwest_client("/tmp/socket.sock".to_string(), || {
                reqwest::Client::builder().user_agent("uds-client").build()
            })
            .unwrap();

        // Second call with a different tag should have triggered creation again;
        // verify by calling with the new tag and confirming no creation happens.
        let call_count = AtomicUsize::new(0);
        let _client3 = common
            .get_or_create_reqwest_client("/tmp/socket.sock".to_string(), || {
                call_count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().build()
            })
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_semaphores_initialized_from_config() {
        let config = XetConfig::new();
        let common = XetCommon::new(&config);

        assert_eq!(common.file_ingestion_semaphore.available_permits(), config.data.max_concurrent_file_ingestion);
        assert_eq!(common.file_download_semaphore.available_permits(), config.data.max_concurrent_file_downloads);

        // Total permits is at least the configured download_buffer_base (may be slightly
        // larger due to rounding up to a whole number of internal permits).
        assert!(
            common.reconstruction_download_buffer.total_permits()
                >= config.reconstruction.download_buffer_size.as_u64()
        );

        assert_eq!(common.active_downloads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_get_or_create_returns_cached_value() {
        let common = XetCommon::new(&XetConfig::new());
        let call_count = AtomicUsize::new(0);

        let v1: Arc<Mutex<Vec<i32>>> = common.cache_get_or_create("my_cache", || {
            call_count.fetch_add(1, Ordering::SeqCst);
            Arc::new(Mutex::new(vec![1, 2, 3]))
        });

        let v2: Arc<Mutex<Vec<i32>>> = common.cache_get_or_create("my_cache", || {
            call_count.fetch_add(1, Ordering::SeqCst);
            Arc::new(Mutex::new(vec![4, 5, 6]))
        });

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&v1, &v2));
    }

    #[test]
    fn test_cache_different_keys_are_independent() {
        let common = XetCommon::new(&XetConfig::new());

        let v1: Arc<String> = common.cache_get_or_create("key_a", || Arc::new("alpha".to_string()));
        let v2: Arc<String> = common.cache_get_or_create("key_b", || Arc::new("beta".to_string()));

        assert_eq!(v1.as_str(), "alpha");
        assert_eq!(v2.as_str(), "beta");
    }

    #[test]
    fn test_cache_type_mismatch_creates_new_entry() {
        let common = XetCommon::new(&XetConfig::new());

        let _v1: Arc<String> = common.cache_get_or_create("shared_key", || Arc::new("original".to_string()));

        // Same key but different type -- downcast fails, so create is called and the
        // entry is replaced with the new type.
        let v2: Arc<i32> = common.cache_get_or_create("shared_key", || Arc::new(42));
        assert_eq!(*v2, 42);
    }
}
