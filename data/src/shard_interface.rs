use super::configurations::{Endpoint::*, StorageConfig};
use super::errors::Result;
use crate::constants::XET_VERSION;
use mdb_shard::ShardFileManager;
use shard_client::{GrpcShardClient, LocalShardClient, ShardClientInterface};
use std::sync::Arc;
use tracing::{info, warn};

pub async fn create_shard_manager(
    shard_storage_config: &StorageConfig,
) -> Result<ShardFileManager> {
    let shard_session_directory = shard_storage_config
        .staging_directory
        .as_ref()
        .expect("Need shard staging directory to create ShardFileManager");
    let shard_cache_directory = &shard_storage_config
        .cache_config
        .as_ref()
        .expect("Need shard cache directory to create ShardFileManager")
        .cache_directory;

    let shard_manager = ShardFileManager::new(shard_session_directory).await?;

    if shard_cache_directory.exists() {
        shard_manager
            .register_shards_by_path(&[shard_cache_directory], true)
            .await?;
    } else {
        warn!(
            "Merkle DB Cache path {:?} does not exist, skipping registration.",
            shard_cache_directory
        );
    }

    Ok(shard_manager)
}

pub async fn create_shard_client(
    shard_storage_config: &StorageConfig,
) -> Result<Arc<dyn ShardClientInterface>> {
    info!("Shard endpoint = {:?}", shard_storage_config.endpoint);
    let client: Arc<dyn ShardClientInterface> = match &shard_storage_config.endpoint {
        Server(endpoint) => {
            let shard_connection_config = shard_client::ShardConnectionConfig {
                endpoint: endpoint.clone(),
                user_id: shard_storage_config.auth.user_id.clone(),
                git_xet_version: XET_VERSION.to_string(),
            };
            Arc::new(GrpcShardClient::from_config(shard_connection_config).await?)
        }
        FileSystem(path) => Arc::new(LocalShardClient::new(path).await?),
    };

    Ok(client)
}
