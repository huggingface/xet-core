use std::path::PathBuf;

use data::configurations::{Endpoint, StorageConfig};
use data::shard_interface::create_shard_manager;
use data::CacheConfig;

#[tokio::main]
async fn main() -> data::errors::Result<()> {
    let config = StorageConfig {
        endpoint: Endpoint::FileSystem(PathBuf::from("/Users/zach/hf/data")),
        auth: None,
        prefix: "default-merkledb".into(),
        cache_config: Some(CacheConfig {
            cache_directory: PathBuf::from("/tmp/xet_shard_cache"),
            cache_size: 1024 * 1024 * 1024,
        }),
        staging_directory: Some(PathBuf::from("/tmp/xet_shard_staging")),
    };
    let shard_manager_result = create_shard_manager(&config).await;
    let shard_manager = shard_manager_result?;
    println!("Shard manager contains: {:#?}", shard_manager.to_vis_json().await);
    Ok(())
}
