use std::sync::Arc;

use cas_client::{HttpShardClient, LocalShardClient, ShardClientInterface};
use mdb_shard::ShardFileManager;
use tracing::debug;

use super::configurations::Endpoint::*;
use super::configurations::StorageConfig;
use super::errors::Result;

pub async fn create_shard_manager(
    shard_storage_config: &StorageConfig,
    download_only_mode: bool,
) -> Result<Arc<ShardFileManager>> {
    let shard_session_directory = shard_storage_config
        .staging_directory
        .as_ref()
        .expect("Need shard staging directory to create ShardFileManager");

    let shard_cache_directory = &shard_storage_config
        .cache_config
        .as_ref()
        .expect("Need shard cache directory to create ShardFileManager")
        .cache_directory;

    let cache_shard_manager = ShardFileManager::builder(shard_cache_directory)
        .with_chunk_dedup(!download_only_mode)
        .with_expired_shard_cleanup(true)
        .from_global_manager_cache(true)
        .build()
        .await?;

    let session_shard_manager = ShardFileManager::builder(shard_session_directory)
        .with_chunk_dedup(!download_only_mode)
        .with_expired_shard_cleanup(false)
        .from_global_manager_cache(false)
        .with_upstream_manager(cache_shard_manager)
        .build()
        .await?;

    Ok(session_shard_manager)
}

pub async fn create_shard_client(
    shard_storage_config: &StorageConfig,
    download_only_mode: bool,
) -> Result<Arc<dyn ShardClientInterface>> {
    debug!("Shard endpoint = {:?}", shard_storage_config.endpoint);
    let client: Arc<dyn ShardClientInterface> = match &shard_storage_config.endpoint {
        Server(endpoint) => Arc::new(HttpShardClient::new(
            endpoint,
            &shard_storage_config.auth,
            shard_storage_config
                .cache_config
                .as_ref()
                .map(|cache| cache.cache_directory.clone()),
        )),
        FileSystem(path) => Arc::new(LocalShardClient::new(path, download_only_mode).await?),
    };

    Ok(client)
}
