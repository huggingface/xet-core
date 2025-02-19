use std::sync::Arc;

pub use cas_client::Client;
use cas_client::{CacheConfig, LocalClient, RemoteClient};
use cas_object::CompressionScheme;
use utils::auth::AuthConfig;
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_cas_client(
    cas_storage_config: &StorageConfig,
    _maybe_repo_info: &Option<RepoInfo>,
    threadpool: Arc<ThreadPool>,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => remote_client(
            endpoint,
            cas_storage_config.compression,
            &cas_storage_config.cache_config,
            &cas_storage_config.auth,
            threadpool,
            dry_run,
        ),
        Endpoint::FileSystem(ref path) => Ok(Arc::new(LocalClient::new(path)?)),
    }
}

fn remote_client(
    endpoint: &str,
    compression: CompressionScheme,
    cache_config: &Option<CacheConfig>,
    auth: &Option<AuthConfig>,
    threadpool: Arc<ThreadPool>,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    // Raw remote client.
    let remote_client = RemoteClient::new(threadpool, endpoint, compression, auth, cache_config, dry_run);

    Ok(Arc::new(remote_client))
}
