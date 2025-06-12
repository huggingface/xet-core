use std::sync::Arc;

pub use cas_client::Client;
use cas_client::{LocalClient, RemoteClient};

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    let cas_storage_config = &config.data_config;

    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => Ok(Arc::new(RemoteClient::new(
            endpoint,
            &cas_storage_config.auth,
            &Some(cas_storage_config.cache_config.clone()),
            config.shard_config.cache_directory.clone(),
            session_id,
            dry_run,
        ))),
        Endpoint::FileSystem(ref path) => Ok(Arc::new(LocalClient::new(path, None)?)),
    }
}
