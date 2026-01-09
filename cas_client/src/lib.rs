pub use chunk_cache::CacheConfig;
pub use http_client::{Api, ResponseErrorLogger, build_auth_http_client_no_retry, build_http_client_no_retry};
pub use interface::{Client, URLProvider};
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;
pub use remote_client::RemoteClient;
use tracing::Level;

pub use crate::error::CasClientError;

pub mod adaptive_concurrency;
mod error;
pub mod exports;
pub mod http_client;
mod interface;
#[cfg(not(target_family = "wasm"))]
mod local_client;
#[cfg(not(target_family = "wasm"))]
pub mod local_server;
pub mod remote_client;
pub mod retry_wrapper;
pub mod upload_progress_stream;

pub mod client_testing_utils;

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;

#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;
