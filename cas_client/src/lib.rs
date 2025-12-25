pub use chunk_cache::CacheConfig;
pub use http_client::{Api, ResponseErrorLogger, RetryConfig, build_auth_http_client, build_http_client};
pub use interface::{Client, URLProvider};
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;
pub use remote_client::RemoteClient;
use tracing::Level;

pub use crate::error::CasClientError;

pub mod adaptive_concurrency;
mod error;
pub mod exports;
#[cfg(not(target_family = "wasm"))]
pub mod file_reconstruction_v1;
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

// Re-export commonly used types from file_reconstruction_v1
#[cfg(not(target_family = "wasm"))]
pub use file_reconstruction_v1::{
    FileProvider, FileReconstructorV1, SeekingOutputProvider, SequentialOutput, sequential_output_from_filepath,
    sequential_output_from_writer,
};

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;

#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;
