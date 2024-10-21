use crate::configurations::*;
use crate::errors::Result;
use crate::test_utils::LocalTestClient;
use cas_client::{CacheConfig, RemoteClient};
use mdb_shard::ShardFileManager;
use std::env::current_dir;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use utils::auth::AuthConfig;

pub use cas_client::Client;

pub(crate) fn create_cas_client(
    cas_storage_config: &StorageConfig,
    _maybe_repo_info: &Option<RepoInfo>,
    shard_manager: Arc<ShardFileManager>,
) -> Result<Arc<dyn Client + Send + Sync>> {
    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => remote_client(
            endpoint,
            &cas_storage_config.cache_config,
            &cas_storage_config.auth,
        ),
        Endpoint::FileSystem(ref path) => {
            local_test_cas_client(&cas_storage_config.prefix, path, shard_manager)
        }
    }
}

fn remote_client(
    endpoint: &str,
    cache_config: &Option<CacheConfig>,
    auth: &Option<AuthConfig>,
) -> Result<Arc<dyn Client + Send + Sync>> {
    
    // Raw remote client.
    let remote_client = RemoteClient::new(endpoint, auth, cache_config);

    Ok(Arc::new(remote_client))
}

fn local_test_cas_client(
    prefix: &str,
    path: &Path,
    shard_manager: Arc<ShardFileManager>,
) -> Result<Arc<dyn Client + Send + Sync>> {
    info!("Using local CAS with path: {:?}.", path);
    let path = match path.is_absolute() {
        true => path,
        false => &current_dir()?.join(path),
    };

    let client = LocalTestClient::new(prefix, path, shard_manager);

    Ok(Arc::new(client))
}
