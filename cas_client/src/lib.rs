pub use http_client::{Api, ResponseErrorLogger, build_auth_http_client, build_http_client};
pub use interface::{Client, URLProvider};
pub use remote_client::RemoteClient;
pub use simulation::{ClientTestingUtils, DirectAccessClient, MemoryClient, RandomFileContents, RandomXorb};
#[cfg(not(target_family = "wasm"))]
pub use simulation::{LocalClient, LocalServer, LocalServerConfig, LocalTestServer};
use tracing::Level;

pub use crate::error::CasClientError;

pub mod adaptive_concurrency;
mod error;
pub mod exports;
pub mod http_client;
mod interface;
pub mod remote_client;
pub mod retry_wrapper;
pub mod simulation;
pub mod upload_progress_stream;

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;

#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;
