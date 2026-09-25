pub use interface::{Client, URLProvider};
pub use remote_client::RemoteClient;
#[cfg(feature = "upload")]
pub use simulation::{ClientTestingUtils, DirectAccessClient, MemoryClient, RandomFileContents, RandomXorb};
#[cfg(all(feature = "upload", not(target_family = "wasm")))]
pub use simulation::{DeletionControlableClient, LocalClient};
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use simulation::{
    LocalServer, LocalServerConfig, LocalTestServer, LocalTestServerBuilder, RemoteSimulationClient,
    SimulationControlClient,
};
use tracing::Level;

pub use crate::common::http_client::{Api, ResponseErrorLogger, build_auth_http_client, build_http_client};

pub mod adaptive_concurrency;
pub mod auth;
#[cfg(feature = "upload")]
pub mod chunk_window_builder;
pub mod exports;
mod interface;
pub mod multipart;
pub mod progress_tracked_streams;
pub mod remote_client;
pub mod retry_wrapper;
#[cfg(all(feature = "upload", not(target_family = "wasm")))]
mod shard_upload_v2;
#[cfg(feature = "upload")]
pub mod simulation;
// No `XetRuntime::spawn` on wasm, so there is no way to report without blocking a transfer.
#[cfg(not(target_family = "wasm"))]
pub mod telemetry;

#[cfg(feature = "upload")]
pub use interface::{ShardUploadProgressCallback, ShardUploadProgressType};
#[cfg(feature = "upload")]
pub use progress_tracked_streams::UploadProgressStream;
pub use progress_tracked_streams::{DownloadProgressStream, ProgressCallback};
#[cfg(not(target_family = "wasm"))]
pub use telemetry::{Direction, TelemetryEnvelope, TransferTelemetry};

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;

#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;
