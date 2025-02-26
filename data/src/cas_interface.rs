use std::sync::Arc;

pub use cas_client::Client;
use cas_client::{LocalClient, RemoteClient};
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_cas_client(
    cas_storage_config: &StorageConfig,
    threadpool: Arc<ThreadPool>,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => Ok(Arc::new(RemoteClient::new(
            threadpool,
            endpoint,
            cas_storage_config.compression,
            &cas_storage_config.auth,
            &cas_storage_config.cache_config,
            dry_run,
        ))),
        Endpoint::FileSystem(ref path) => Ok(Arc::new(LocalClient::new(path, None)?)),
    }
}
