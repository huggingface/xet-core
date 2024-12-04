use std::env;
use std::sync::Arc;

use cas_client::remote_client::CAS_ENDPOINT;
use data::configurations::{Endpoint, FileQueryPolicy, StorageConfig, TranslatorConfig};
use data::shard_interface::create_shard_manager;
use utils::auth::AuthConfig;

#[tokio::main]
async fn main() -> data::errors::Result<()> {
    let config = TranslatorConfig {
        file_query_policy: FileQueryPolicy::LocalFirst,
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::Server(env::var("XET_CAS_SERVER").unwrap_or(CAS_ENDPOINT.to_string())),
            auth: Some(AuthConfig{ token: todo!(), token_expiration: todo!(), token_refresher: todo!() }),
            prefix: todo!(),
            cache_config: todo!(),
            staging_directory: todo!(),
        },
        shard_storage_config: todo!(),
        dedup_config: todo!(),
        repo_info: todo!(),
    };
    let shard_manager = Arc::new(create_shard_manager(&config.shard_storage_config).await?);
    println!("Shard manager contains: {:?}", shard_manager);
}
