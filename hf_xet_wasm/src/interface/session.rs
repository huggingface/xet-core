#![allow(dead_code)]

use std::sync::Arc;

use cas_object::CompressionScheme;
use cas_types::HexMerkleHash;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use web_sys::Blob;

use crate::blob_reader::BlobReader;
use crate::configurations::{DataConfig, RepoSalt, ShardConfig, TranslatorConfig};
use crate::interface::auth::WrappedTokenRefresher;

fn convert_error(e: impl std::error::Error) -> JsValue {
    JsValue::from(format!("{e:?}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PointerFile {
    pub file_size: u64,
    pub file_hash: HexMerkleHash,
}

#[wasm_bindgen]
pub struct XetSession {
    upload: Arc<crate::wasm_file_upload_session::FileUploadSession>,
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(
        endpoint: String,
        token_info: super::auth::TokenInfo,
        token_refresher: super::auth::TokenRefresher,
    ) -> Self {
        let (token, token_expiration): utils::auth::TokenInfo = token_info.into();
        let auth = AuthConfig {
            token,
            token_expiration,
            token_refresher: Arc::new(WrappedTokenRefresher::from(token_refresher)),
        };

        let config = TranslatorConfig {
            data_config: DataConfig {
                endpoint,
                compression: Some(CompressionScheme::LZ4),
                auth: Some(auth),
                prefix: "default".to_owned(),
            },
            shard_config: ShardConfig {
                prefix: "default-merkledb".to_owned(),
                repo_salt: RepoSalt::default(),
            },
        };
        let upload = crate::wasm_file_upload_session::FileUploadSession::new(Arc::new(config));

        Self {
            upload: Arc::new(upload),
        }
    }

    #[wasm_bindgen(js_name = "uploadFileFromRawData")]
    pub async fn upload_file_from_raw(&self, tracker_id: String, file: Vec<u8>) -> Result<(), JsValue> {
        let blob = Blob::new_with_u8_array_sequence(&js_sys::Uint8Array::from(file.as_slice()))?;
        self.upload_file_from_blob(tracker_id, blob).await
    }

    #[wasm_bindgen(js_name = "uploadFileFromBlob")]
    pub async fn upload_file_from_blob(&self, tracker_id: String, blob: Blob) -> Result<(), JsValue> {
        // read from blob async
        let mut cleaner = self.upload.start_clean(tracker_id);

        let mut reader = BlobReader::new(blob)?;

        let mut buf = vec![0u8; 1024 * 10]; // 10KB buffer

        loop {
            let num_read = reader.read(&mut buf).await.map_err(convert_error)?;
            if num_read == 0 {
                break;
            }
            cleaner.add_data(&buf[0..num_read]).await.map_err(convert_error)?;
        }

        Ok(())
    }

    #[wasm_bindgen]
    pub async fn finalize(self) -> Result<(), JsValue> {
        // flush the session
        self.upload
            .finalize()
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        Ok(())
    }
}
