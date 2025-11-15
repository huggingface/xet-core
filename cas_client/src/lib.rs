#![allow(dead_code)]

pub use chunk_cache::CacheConfig;
pub use http_client::{Api, ResponseErrorLogger, RetryConfig, build_auth_http_client, build_http_client};
pub use interface::Client;
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;
#[cfg(not(target_family = "wasm"))]
pub use output_provider::*;
pub use remote_client::RemoteClient;

pub use crate::error::CasClientError;

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
