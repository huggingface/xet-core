use cas_types::HexMerkleHash;
use chunking::{Chunker, TARGET_CHUNK_SIZE};
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use utils::auth::AuthConfig;
use utils::errors::AuthError;
use wasm_bindgen::prelude::*;
use web_sys::js_sys::{ArrayBuffer, Uint8Array};

pub(crate) mod session;
pub(crate) mod auth;

pub use session::*;
pub use auth::*;



const INGESTION_BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[wasm_bindgen]
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    len: u32,
    hash: HexMerkleHash,
}

#[wasm_bindgen]
impl ChunkInfo {
    #[wasm_bindgen(constructor)]
    pub fn js_new(len: u32, hash: String) -> Self {
        let hash = MerkleHash::from_hex(&hash).expect("failed to parse hex hash").into();
        Self { len, hash }
    }

    #[wasm_bindgen(getter)]
    pub fn len(&self) -> u32 {
        self.len
    }

    #[wasm_bindgen(getter, js_name = "hash")]
    pub fn _hash(&self) -> String {
        self.hash.to_string()
    }
}

/// takes a Uint8Array of bytes representing data
#[wasm_bindgen]
pub fn chunk(data: Vec<u8>) -> JsValue {
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);

    let mut result = Vec::new();
    for vec_chunk in data.chunks(INGESTION_BLOCK_SIZE) {
        let chunks = chunker.next_block(vec_chunk, false);
        for chunk in chunks {
            result.push(ChunkInfo {
                len: chunk.data.len() as u32,
                hash: chunk.hash.into(),
            });
        }
    }

    if let Some(chunk) = chunker.finish() {
        result.push(ChunkInfo {
            len: chunk.data.len() as u32,
            hash: chunk.hash.into(),
        });
    }

    serde_wasm_bindgen::to_value(&result).expect("failed to serialize result")
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
}

#[wasm_bindgen]
impl XetSession {
    #[wasm_bindgen(constructor)]
    pub fn new(cas_endpoint: String, token: String, token_expiration: u64, token_refresher: TokenRefresher) -> Self {
        let auth = AuthConfig {
            token,
            token_expiration,
            token_refresher: Arc::new(token_refresher),
        };
        Self {
            cas_endpoint,
            clients: Clients::new(Some(auth)),
        }
    }

    #[wasm_bindgen(js_name = "chunk")]
    pub fn chunk(data: Vec<u8>) -> JsValue {
        chunk(data)
    }
}
