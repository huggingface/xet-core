use std::sync::Arc;

pub use cas_client::Client;
use cas_client::RemoteClient;

use crate::configurations::TranslatorConfig;
use crate::errors::Result;

pub(crate) fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client + Send + Sync>> {
    if let Some(local_path) = config.session.local_path() {
        #[cfg(not(target_family = "wasm"))]
        {
            Ok(cas_client::LocalClient::new(&local_path)?)
        }
        #[cfg(target_family = "wasm")]
        unimplemented!("Local file system access is not supported in WASM builds")
    } else {
        Ok(RemoteClient::new(
            &config.session.endpoint,
            &config.session.auth,
            Some(&config.chunk_cache_directory),
            session_id,
            dry_run,
            &config.session.user_agent,
        ))
    }
}
