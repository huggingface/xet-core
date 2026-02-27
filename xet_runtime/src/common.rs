use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use reqwest::Client;
use tokio::sync::Semaphore;
use utils::adjustable_semaphore::AdjustableSemaphore;
use xet_config::XetConfig;

/// Holds global values that are shared across the entire runtime.
///
/// Accessible via `XetRuntime::current().common()`.
#[derive(Debug)]
pub struct XetCommon {
    // A cached reqwest Client to be shared by all high-level clients.
    // The String tag identifies the client type (e.g., "tcp" for regular, socket path for UDS).
    global_reqwest_client: Mutex<Option<(String, Client)>>,

    /// Limits the number of files being ingested (cleaned/uploaded) concurrently.
    pub file_ingestion_semaphore: Arc<Semaphore>,

    /// Limits the number of files being downloaded concurrently.
    pub file_download_semaphore: Arc<Semaphore>,

    /// Limits total memory used for buffering data during reconstruction downloads.
    pub reconstruction_download_buffer: Arc<AdjustableSemaphore>,

    /// Tracks the number of currently active file downloads for dynamic buffer scaling.
    pub active_downloads: Arc<AtomicU64>,
}

impl XetCommon {
    /// Creates a new `XetCommon` instance with the given configuration.
    pub fn new(config: &XetConfig) -> Self {
        Self {
            global_reqwest_client: Mutex::new(None),
            file_ingestion_semaphore: Arc::new(Semaphore::new(config.data.max_concurrent_file_ingestion)),
            file_download_semaphore: Arc::new(Semaphore::new(config.data.max_concurrent_file_downloads)),
            reconstruction_download_buffer: {
                let base = config.reconstruction.download_buffer_size.as_u64();
                let limit = config.reconstruction.download_buffer_limit.as_u64();
                AdjustableSemaphore::new(base, (base, limit))
            },
            active_downloads: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gets or creates a reqwest client, using a tag to identify the client type.
    ///
    /// # Arguments
    /// * `tag` - A string identifier for the client (e.g., "tcp" for regular, socket path for UDS)
    /// * `create_client_fn` - A function that creates the client if needed
    ///
    /// # Returns
    /// Returns a clone of the cached client if the tag matches, or creates a new client if the tag differs.
    pub fn get_or_create_reqwest_client<F>(
        &self,
        tag: String,
        create_client_fn: F,
    ) -> std::result::Result<Client, reqwest::Error>
    where
        F: FnOnce() -> std::result::Result<Client, reqwest::Error>,
    {
        let mut guard = self.global_reqwest_client.lock().unwrap();

        match guard.as_ref() {
            Some((cached_tag, cached_client)) if cached_tag == &tag => {
                // Tag matches, return a clone of the existing client
                Ok(cached_client.clone())
            },
            _ => {
                // Tag doesn't match or no client exists, create a new one
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
    fn test_initializes_with_empty_client_cache() {
        let common = XetCommon::new(&XetConfig::new());

        let guard = common.global_reqwest_client.lock().unwrap();
        assert!(guard.is_none());
    }

    #[test]
    fn test_replaces_client_when_tag_changes() {
        let common = XetCommon::new(&XetConfig::new());

        let _client1 = common
            .get_or_create_reqwest_client("tcp".to_string(), || {
                reqwest::Client::builder().user_agent("tcp-client").build()
            })
            .unwrap();

        {
            let guard = common.global_reqwest_client.lock().unwrap();
            let (tag, _) = guard.as_ref().unwrap();
            assert_eq!(tag, "tcp");
        }

        let _client2 = common
            .get_or_create_reqwest_client("/tmp/socket.sock".to_string(), || {
                reqwest::Client::builder().user_agent("uds-client").build()
            })
            .unwrap();

        {
            let guard = common.global_reqwest_client.lock().unwrap();
            let (tag, _) = guard.as_ref().unwrap();
            assert_eq!(tag, "/tmp/socket.sock");
        }
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
}
