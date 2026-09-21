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

    #[cfg(feature = "upload")]
    if let Some(local_path) = session.local_path(&config.ctx) {
        #[cfg(not(target_family = "wasm"))]
        {
            let xorb_path = local_path.join("xet").join("xorbs");
            return Ok(xet_client::cas_client::LocalClient::new(runtime, xorb_path).await?);
        }
        #[cfg(target_family = "wasm")]
        {
            let _ = local_path;
            unimplemented!("Local file system access is not available in WASM")
        }
    }

    #[cfg(feature = "upload")]
    if session.is_memory() {
        return Ok(xet_client::cas_client::MemoryClient::new(runtime));
    }

    #[cfg(not(feature = "upload"))]
    {
        // The local and in-memory clients are part of the upload/simulation
        // surface; a download-only build cannot serve these endpoints.
        if session.local_path(&config.ctx).is_some() {
            return Err(crate::error::DataError::CASConfigError(
                "local:// CAS endpoints require the `upload` feature (they are backed by the simulation clients)"
                    .into(),
            ));
        }
        if session.is_memory() {
            return Err(crate::error::DataError::CASConfigError(
                "memory:// CAS endpoints require the `upload` feature (they are backed by the simulation clients)"
                    .into(),
            ));
        }
    }

    let session = &config.session;
    Ok(RemoteClient::new(
        runtime,
        &session.endpoint,
        &session.auth,
        session_id,
        dry_run,
        session.custom_headers.clone(),
    ))
}
