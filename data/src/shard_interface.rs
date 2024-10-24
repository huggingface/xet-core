use std::sync::Arc;

use cas_client::{HttpShardClient, LocalShardClient, ShardClientInterface};
use mdb_shard::ShardFileManager;
use tracing::{info, warn};

use super::configurations::Endpoint::*;
use super::configurations::StorageConfig;
use super::errors::Result;

pub async fn create_shard_manager(shard_storage_config: &StorageConfig) -> Result<ShardFileManager> {
    let shard_session_directory = shard_storage_config
        .staging_directory
        .as_ref()
        .expect("Need shard staging directory to create ShardFileManager");
    let shard_cache_directory = &shard_storage_config
        .cache_config
        .as_ref()
        .expect("Need shard cache directory to create ShardFileManager")
        .cache_directory;

    let shard_manager = ShardFileManager::load_dir(shard_session_directory).await?;

    if shard_cache_directory.exists() {
        shard_manager.load_and_cleanup_shards_by_path(&[shard_cache_directory]).await?;
    } else {
        warn!("Merkle DB Cache path {:?} does not exist, skipping registration.", shard_cache_directory);
    }

    Ok(shard_manager)
}

pub async fn create_shard_client(shard_storage_config: &StorageConfig) -> Result<Arc<dyn ShardClientInterface>> {
    info!("Shard endpoint = {:?}", shard_storage_config.endpoint);
    let client: Arc<dyn ShardClientInterface> = match &shard_storage_config.endpoint {
        Server(endpoint) => Arc::new(HttpShardClient::new(
            endpoint,
            &shard_storage_config.auth,
            shard_storage_config
                .cache_config
                .as_ref()
                .map(|cache| cache.cache_directory.clone()),
        )),
        FileSystem(path) => Arc::new(LocalShardClient::new(path).await?),
    };

    Ok(client)
}
