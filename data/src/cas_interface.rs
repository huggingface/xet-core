use crate::configurations::*;
use crate::errors::Result;
use cas_client::RemoteClient;
use std::env::current_dir;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use utils::auth::AuthConfig;

pub use cas_client::Client;

pub(crate) fn create_cas_client(
    cas_storage_config: &StorageConfig,
    _maybe_repo_info: &Option<RepoInfo>,
) -> Result<Arc<dyn Client + Send + Sync>> {
    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => remote_client(endpoint, &cas_storage_config.auth),
        Endpoint::FileSystem(ref path) => local_test_cas_client(path),
    }
}

pub(crate) fn remote_client(
    endpoint: &str,
    auth: &Option<AuthConfig>,
) -> Result<Arc<dyn Client + Send + Sync>> {
    // Raw remote client.
    let remote_client = Arc::new(RemoteClient::new(endpoint, auth));

    Ok(remote_client)
}

fn local_test_cas_client(path: &Path) -> Result<Arc<dyn Client + Send + Sync>> {
    info!("Using local CAS with path: {:?}.", path);
    let _path = match path.is_absolute() {
        true => path,
        false => &current_dir()?.join(path),
    };
    unimplemented!()
}
