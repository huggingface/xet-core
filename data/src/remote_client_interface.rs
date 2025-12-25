use std::sync::Arc;

use cas_client::{Client, RemoteClient};

use crate::configurations::*;
use crate::errors::Result;

pub(crate) fn create_remote_client(
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
            &cas_storage_config.user_agent,
        )),
        Endpoint::FileSystem(ref path) => {
            #[cfg(not(target_family = "wasm"))]
            {
                Ok(cas_client::LocalClient::new(path)?)
            }
            #[cfg(target_family = "wasm")]
            unimplemented!("Local file system access is not supported in WASM builds")
        },
    }
}
