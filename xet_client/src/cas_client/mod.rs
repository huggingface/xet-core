pub use http_client::{Api, ResponseErrorLogger, build_auth_http_client, build_http_client};
pub use interface::{Client, URLProvider};
pub use remote_client::RemoteClient;
#[cfg(feature = "simulation")]
pub use simulation::{ClientTestingUtils, DirectAccessClient, MemoryClient, RandomFileContents, RandomXorb};
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use simulation::{
    DeletionControlableClient, LocalClient, LocalServer, LocalServerConfig, LocalTestServer, LocalTestServerBuilder,
    RemoteSimulationClient, SimulationControlClient,
};
use tracing::Level;

pub mod adaptive_concurrency;
pub mod auth;
pub mod exports;
pub mod http_client;
mod interface;
pub mod multipart;
pub mod progress_tracked_streams;
pub mod remote_client;
pub mod retry_wrapper;
#[cfg(feature = "simulation")]
pub mod simulation;

pub use progress_tracked_streams::{
    DownloadProgressStream, ProgressCallback, StreamProgressReporter, UploadProgressStream,
};

#[cfg(not(feature = "elevated_information_level"))]
pub const INFORMATION_LOG_LEVEL: Level = Level::DEBUG;

#[cfg(feature = "elevated_information_level")]
pub const INFORMATION_LOG_LEVEL: Level = Level::INFO;
