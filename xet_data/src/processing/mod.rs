pub mod configurations;
pub mod data_client;
#[cfg(feature = "upload")]
mod deduplication_interface;
#[cfg(feature = "upload")]
mod file_cleaner;
mod file_download_session;
#[cfg(feature = "upload")]
mod file_upload_session;
#[cfg(all(feature = "upload", not(target_family = "wasm")))]
pub mod migration_tool;
#[cfg(feature = "upload")]
pub mod range_upload;
mod remote_client_interface;
#[cfg(feature = "upload")]
mod sha256;
#[cfg(feature = "upload")]
mod shard_interface;
mod xet_file;

// Reexport this one for now
#[cfg(feature = "upload")]
pub use file_cleaner::{Sha256Policy, SingleFileCleaner};
pub use file_download_session::FileDownloadSession;
#[cfg(feature = "upload")]
pub use file_upload_session::FileUploadSession;
#[cfg(feature = "upload")]
pub use range_upload::{DirtyInput, upload_ranges};
pub use remote_client_interface::create_remote_client;
pub use xet_client::cas_client::Client as CasClient;
#[cfg(not(target_family = "wasm"))]
pub use xet_client::chunk_cache::get_cache;
pub use xet_client::chunk_cache::{CacheConfig, ChunkCache};
pub use xet_core_structures::merklehash::ChunkHashList;
pub use xet_file::XetFileInfo;

#[cfg(feature = "upload")]
pub use crate::deduplication::RawXorbData;
pub use crate::file_reconstruction::{DownloadStream, UnorderedDownloadStream};

#[cfg(all(debug_assertions, feature = "upload", not(target_family = "wasm")))]
pub mod test_utils;
