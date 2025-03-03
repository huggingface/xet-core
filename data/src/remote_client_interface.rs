use std::sync::Arc;

pub use cas_client::Client;
use cas_client::{LocalClient, RemoteClient};
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_remote_client(
    config: &TranslatorConfig,
    threadpool: Arc<ThreadPool>,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    let cas_storage_config = &config.cas_storage_config;

    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => Ok(Arc::new(RemoteClient::new(
            threadpool,
            endpoint,
            cas_storage_config.compression,
            &cas_storage_config.auth,
            &cas_storage_config.cache_config,
            config
                .shard_storage_config
                .cache_config
                .as_ref()
                .map(|c| c.cache_directory.clone())
                .unwrap_or_default(),
            dry_run,
        ))),
        Endpoint::FileSystem(ref path) => Ok(Arc::new(LocalClient::new(path, None)?)),
    }
}
