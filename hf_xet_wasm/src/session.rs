use std::sync::Arc;

use cas_object::CompressionScheme;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use web_sys::Blob;

use crate::auth::{TokenInfo, TokenRefresher, WrappedTokenRefresher};
use crate::blob_reader::BlobReader;
use crate::configurations::{DataConfig, ShardConfig, TranslatorConfig};
use crate::wasm_file_upload_session::FileUploadSession;

fn convert_error(e: impl std::error::Error) -> JsValue {
    JsValue::from(format!("{e:?}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsPointerFile {
    pub file_size: f64,
    pub file_hash: String,
    pub sha256: String,
}

/// XetSession is the exported public interface to upload files in WebAssembly
///
/// To instantiate a XetSession a caller needs to pass in the endpoint to CAS as well as the auth
/// information to authenticate against cas. see auth.rs or README.md to explain what constructs to pass in.
///
/// After instantiating a XetSession, to upload a file, use the `upload_file_from_{raw/blob}` functions
/// to start an upload. After the `upload*` functions return, the return value is a JS object representing
/// a pointer file (see JsPointerFile). However, the file is only fully uploaded after finalize() is called
/// and returns an ok result.
///
/// the file_id option in upload* functions is for file tracking only.
#[wasm_bindgen(js_name = "XetSession")]
pub struct XetSession {
    upload: Arc<FileUploadSession>,
}

#[wasm_bindgen(js_class = "XetSession")]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token_info: TokenInfo, token_refresher: TokenRefresher) -> Self {
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
            },
            session_id: uuid::Uuid::new_v4().to_string(),
        };
        let upload = FileUploadSession::new(Arc::new(config));

        Self {
            upload: Arc::new(upload),
        }
    }

    // allows uploading a file from raw data if the data/file is not in a blob
    // internally converts the data to a blob.
    #[wasm_bindgen(js_name = "uploadFileFromRawData")]
    pub async fn upload_file_from_raw(&mut self, file_id: u64, file: Vec<u8>) -> Result<JsValue, JsValue> {
        let blob = Blob::new_with_u8_array_sequence(&js_sys::Uint8Array::from(file.as_slice()))?;
        self.upload_file_from_blob(file_id, blob).await
    }

    // uploads a file from a blob. reading from it, and updating the cleaner in 1MB increments
    #[wasm_bindgen(js_name = "uploadFileFromBlob")]
    pub async fn upload_file_from_blob(&mut self, file_id: u64, blob: Blob) -> Result<JsValue, JsValue> {
        // read from blob async
        let mut cleaner = self.upload.start_clean(file_id, None);

        let mut reader = BlobReader::new(blob)?;

        let mut buf = vec![0u8; 1024 * 1024]; // 1MB buffer

        let mut file_size = 0;
        loop {
            let num_read = reader.read(&mut buf).await.map_err(convert_error)?;
            if num_read == 0 {
                break;
            }
            file_size += num_read as u64;
            cleaner.add_data(&buf[0..num_read]).await.map_err(convert_error)?;
        }

        let (file_hash, sha256, _metrics) = cleaner.finish().await.map_err(convert_error)?;

        let file_size = file_size as f64;
        let pf = JsPointerFile {
            file_size,
            file_hash: file_hash.hex(),
            sha256: sha256.hex(),
        };
        serde_wasm_bindgen::to_value(&pf).map_err(|e| JsValue::from_str(&format!("{e:?}")))
    }

    #[wasm_bindgen]
    pub async fn finalize(self) -> Result<(), JsValue> {
        // flush the session
        self.upload.finalize().await.map_err(|e| JsValue::from_str(&format!("{e:?}")))?;
        Ok(())
    }
}
