#![allow(dead_code)]

pub use chunk_cache::{CacheConfig, CHUNK_CACHE_SIZE_BYTES};
pub use http_client::{build_auth_http_client, build_http_client, RetryConfig};
#[cfg(not(target_family = "wasm"))]
pub use interface::FileProvider;
use interface::RegistrationClient;
pub use interface::{Client, OutputProvider, Reconstructable, ReconstructionClient, UploadClient};
pub use local_client::LocalClient;
pub use remote_client::RemoteClient;

pub use crate::error::CasClientError;
pub use crate::interface::ShardClientInterface;

mod download_utils;
mod error;
mod http_client;
mod interface;
#[cfg(not(target_family = "wasm"))]
mod local_client;
pub mod remote_client;
