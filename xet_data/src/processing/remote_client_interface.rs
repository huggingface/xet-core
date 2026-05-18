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
    let runtime = config.ctx.clone();

    if let Some(local_path) = session.local_path(&config.ctx) {
        #[cfg(not(target_family = "wasm"))]
        {
            let xorb_path = local_path.join("xet").join("xorbs");
            Ok(xet_client::cas_client::LocalClient::new(runtime, xorb_path).await?)
        }
        #[cfg(target_family = "wasm")]
        {
            let _ = local_path;
            unimplemented!("Local file system access is not available in WASM")
        }
    } else if session.is_memory() {
        Ok(xet_client::cas_client::MemoryClient::new(runtime))
    } else {
        Ok(RemoteClient::new(
            runtime,
            &session.endpoint,
            &session.auth,
            session_id,
            dry_run,
            session.custom_headers.clone(),
        ))
    }
}
