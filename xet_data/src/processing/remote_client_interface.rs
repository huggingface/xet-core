use std::sync::Arc;

use xet_client::cas_client::{Client, RemoteClient};

use super::configurations::TranslatorConfig;
use crate::error::Result;

pub async fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client>> {
    let session = &config.session;

    if let Some(local_path) = session.local_path() {
        #[cfg(not(target_family = "wasm"))]
        {
            let xorb_path = local_path.join("xet").join("xorbs");
            Ok(xet_client::cas_client::LocalClient::new(xorb_path).await?)
        }
        #[cfg(target_family = "wasm")]
        unimplemented!("Local file system access is not supported in WASM builds")
    } else if session.is_memory() {
        #[cfg(not(target_family = "wasm"))]
        {
            Ok(xet_client::cas_client::MemoryClient::new())
        }
        #[cfg(target_family = "wasm")]
        unimplemented!("In-memory client is not supported in WASM builds")
    } else {
        Ok(RemoteClient::new(
            &session.endpoint,
            &session.auth,
            session_id,
            dry_run,
            session.custom_headers.clone(),
        ))
    }
}
