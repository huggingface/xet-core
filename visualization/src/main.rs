use std::path::PathBuf;

use data::configurations::{Endpoint, StorageConfig};
use data::shard_interface::create_shard_manager;
use data::CacheConfig;

/*
            endpoint: ,
            auth: Some(AuthConfig {
                token: env::var("XET_CAS_TOKEN").unwrap_or("".to_string()),
                token_expiration: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                token_refresher: Arc::new(NoOpTokenRefresher),
            }),
            prefix: "default-merkledb".into(),
            cache_config: todo!(),
            staging_directory: todo!(),
*/
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
    println!("Shard manager contains: {:#?}", shard_manager);
    Ok(())
}
