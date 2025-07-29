#![allow(dead_code)]

pub use chunk_cache::{CacheConfig, CHUNK_CACHE_SIZE_BYTES};
pub use http_client::{build_auth_http_client, build_http_client, Api, RetryConfig};
pub use interface::Client;
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;
#[cfg(not(target_family = "wasm"))]
pub use output_provider::{FileProvider, OutputProvider};
pub use remote_client::RemoteClient;

pub use crate::error::CasClientError;

mod constants;
#[cfg(not(target_family = "wasm"))]
mod download_utils;
mod error;
pub mod exports;
mod http_client;
mod interface;
#[cfg(not(target_family = "wasm"))]
mod local_client;
#[cfg(not(target_family = "wasm"))]
mod output_provider;
pub mod remote_client;
mod retry_wrapper;
mod upload_progress_stream;
