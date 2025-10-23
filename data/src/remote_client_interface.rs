use std::sync::Arc;

pub use cas_client::Client;
use cas_client::{MEMORY_CACHE_PERCENTAGE, MemoryCache, RemoteClient};
use chunk_cache::ChunkCache;
use tracing::info;

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    let cas_storage_config = &config.data_config;

    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => {
            // Use MemoryCache instead of DiskCache by default
            let chunk_cache = if *MEMORY_CACHE_PERCENTAGE > 0.0 {
                match MemoryCache::new(*MEMORY_CACHE_PERCENTAGE) {
                    Ok(cache) => {
                        info!("Using MemoryCache with {:.1}% of system RAM", *MEMORY_CACHE_PERCENTAGE * 100.0);
                        Some(Arc::new(cache) as Arc<dyn ChunkCache>)
                    }
                    Err(e) => {
                        info!("Failed to initialize MemoryCache: {e}, proceeding without cache");
                        None
                    }
                }
            } else {
                None
            };

            Ok(Arc::new(RemoteClient::with_cache(
                endpoint,
                &cas_storage_config.auth,
                chunk_cache,
                Some(config.shard_config.cache_directory.clone()),
                session_id,
                dry_run,
            )))
        }
        Endpoint::FileSystem(ref path) => {
            #[cfg(not(target_family = "wasm"))]
            {
                Ok(Arc::new(cas_client::LocalClient::new(path)?))
            }
            #[cfg(target_family = "wasm")]
            unimplemented!("Local file system access is not supported in WASM builds")
        },
    }
}
