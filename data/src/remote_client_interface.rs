use std::sync::Arc;

use cas_client::{Client, RemoteClient};

use crate::configurations::*;
use crate::errors::Result;

/// Creates a CAS client from a `TranslatorConfig`.
///
/// This is useful for callers that want to wrap the client (e.g. with a cache)
/// before passing it to `FileDownloadSession::from_client`.
pub async fn create_client(config: &TranslatorConfig) -> Result<Arc<dyn Client>> {
    let session_id = config.session_id.clone().unwrap_or_else(|| ulid::Ulid::new().to_string());
    create_remote_client(config, &session_id, false).await
}

pub(crate) async fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client>> {
    let cas_storage_config = &config.data_config;

    match cas_storage_config.endpoint {
        Endpoint::Server(ref endpoint) => Ok(RemoteClient::new(
            endpoint,
            &cas_storage_config.auth,
            session_id,
            dry_run,
            cas_storage_config.custom_headers.clone(),
        )),
        Endpoint::FileSystem(ref path) => {
            #[cfg(not(target_family = "wasm"))]
            {
                Ok(cas_client::LocalClient::new(path).await?)
            }
            #[cfg(target_family = "wasm")]
            unimplemented!("Local file system access is not supported in WASM builds")
        },
        Endpoint::InMemory => {
            #[cfg(not(target_family = "wasm"))]
            {
                Ok(cas_client::MemoryClient::new())
            }
            #[cfg(target_family = "wasm")]
            unimplemented!("In-memory client is not supported in WASM builds")
        },
    }
}
