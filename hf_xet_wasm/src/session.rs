use crate::auth::WrappedTokenRefresher;
use cas_types::HexMerkleHash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use web_sys::Blob;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PointerFile {
    pub file_size: u64,
    pub file_hash: HexMerkleHash,
}

type DataProcessingError = std::io::Error;

#[async_trait::async_trait]
trait UploadSession {

    async fn finalize(&self) -> Result<(), DataProcessingError>;
    async fn start_clean(&self, path: String) -> Arc<dyn Cleaner>;
}

struct UploadSessionImpl;

#[async_trait::async_trait]
impl UploadSession for UploadSessionImpl {
    async fn finalize(&self) -> Result<(), DataProcessingError> {
        // Finalize the upload session
        Ok(())
    }

    async fn start_clean(&self, path: String) -> Arc<dyn Cleaner> {
        // Start a clean session
        Arc::new(CleanerImpl)
    }
}


#[async_trait::async_trait]
trait Cleaner {
    async fn add_data(&mut self, data: &[u8]) -> Result<(), DataProcessingError>;
    async fn finish(&self) -> Result<Vec<PointerFile>, DataProcessingError>;
}

struct CleanerImpl;

#[async_trait::async_trait]
impl Cleaner for CleanerImpl {
    async fn add_data(&mut self, data: &[u8]) -> Result<(), DataProcessingError> {
        // Add data to the cleaner
        Ok(())
    }

    async fn finish(&self) -> Result<Vec<PointerFile>, DataProcessingError> {
        // Finish the cleaning process
        Ok(vec![])
    }
}

struct Clients {
    authenticated_http_client: Arc<http_client::ClientWithMiddleware>,
    conservative_authenticated_http_client: Arc<http_client::ClientWithMiddleware>,
    http_client: Arc<http_client::ClientWithMiddleware>,
}

impl Clients {
    fn new(auth: Option<AuthConfig>) -> Self {
        Self {
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(&auth, http_client::RetryConfig::default()).unwrap(),
            ),
            conservative_authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(&auth, http_client::RetryConfig::no429retry()).unwrap(),
            ),
            http_client: Arc::new(http_client::build_http_client(http_client::RetryConfig::default()).unwrap()),
        }
    }
}

#[wasm_bindgen]
pub struct XetSession {
    cas_endpoint: String,
    clients: Clients,
    upload: Arc<dyn UploadSession>,
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(
        cas_endpoint: String,
        token_info: crate::auth::TokenInfo,
        token_refresher: crate::auth::TokenRefresher,
    ) -> Self {
        let (token, token_expiration): utils::auth::TokenInfo = token_info.into();
        let auth = AuthConfig {
            token,
            token_expiration,
            token_refresher: Arc::new(WrappedTokenRefresher::from(token_refresher)),
        };
        Self {
            cas_endpoint,
            clients: Clients::new(Some(auth)),
            upload: Arc::new(UploadSessionImpl),
        }
    }

    #[wasm_bindgen(js_name = "chunk")]
    pub fn chunk(data: Vec<u8>) -> JsValue {
        crate::chunk(data)
    }

    #[wasm_bindgen(js_name = "uploadFileFromRawData")]
    pub async fn upload_file_from_raw(&self, file: Vec<u8>) -> Result<(), JsValue> {
        let blob = Blob::new_with_u8_array_sequence(&js_sys::Uint8Array::from(file.as_slice()))?;
        self.upload_file_from_blob(blob).await
    }

    #[wasm_bindgen(js_name = "uploadFileFromBlob")]
    pub async fn upload_file_from_blob(&self, blob: Blob) -> Result<(), JsValue> {
        // read from blob async
        let _ = self.upload.start_clean("".to_string()).await;
        Ok(())
    }

    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsValue> {
        // flush the session
        self.upload.finalize().await.map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        Ok(())
    }
}
