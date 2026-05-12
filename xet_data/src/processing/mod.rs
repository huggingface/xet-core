pub mod configurations;
pub mod data_client;
#[cfg(not(target_family = "wasm"))]
mod deduplication_interface;
mod file_cleaner;
mod file_download_session;
#[cfg(not(target_family = "wasm"))]
mod file_upload_session;
#[cfg(not(target_family = "wasm"))]
pub mod migration_tool;
mod remote_client_interface;
mod sha256;
#[cfg(not(target_family = "wasm"))]
mod shard_interface;
mod xet_file;

// Reexport this one for now
pub use file_cleaner::Sha256Policy;
#[cfg(not(target_family = "wasm"))]
pub use file_cleaner::SingleFileCleaner;
pub use file_download_session::FileDownloadSession;
#[cfg(not(target_family = "wasm"))]
pub use file_upload_session::FileUploadSession;
pub use remote_client_interface::create_remote_client;
pub use xet_client::cas_client::Client as CasClient;
#[cfg(not(target_family = "wasm"))]
pub use xet_client::chunk_cache::get_cache;
pub use xet_client::chunk_cache::{CacheConfig, ChunkCache};
pub use xet_file::XetFileInfo;

#[cfg(not(target_family = "wasm"))]
pub use crate::deduplication::RawXorbData;
pub use crate::file_reconstruction::{DownloadStream, UnorderedDownloadStream};

#[cfg(all(debug_assertions, not(target_family = "wasm")))]
pub mod test_utils;
