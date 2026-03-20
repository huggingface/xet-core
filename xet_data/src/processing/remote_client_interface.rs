use std::sync::Arc;

use xet_client::cas_client::{Client, RemoteClient};

use super::configurations::TranslatorConfig;
use crate::error::Result;

pub(crate) async fn create_remote_client(
    config: &TranslatorConfig,
    session_id: &str,
    dry_run: bool,
) -> Result<Arc<dyn Client>> {
    let session = &config.session;

    if let Some(local_path) = session.local_path() {
        #[cfg(all(feature = "simulation", not(target_family = "wasm")))]
        {
            let xorb_path = local_path.join("xet").join("xorbs");
            Ok(xet_client::cas_client::LocalClient::new(xorb_path).await?)
        }
        #[cfg(any(not(feature = "simulation"), target_family = "wasm"))]
        {
            let _ = local_path;
            unimplemented!("Local file system access requires the 'simulation' feature")
        }
    } else if session.is_memory() {
        #[cfg(feature = "simulation")]
        {
            Ok(xet_client::cas_client::MemoryClient::new())
        }
        #[cfg(not(feature = "simulation"))]
        unimplemented!("In-memory client requires the 'simulation' feature")
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
