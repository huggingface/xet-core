#![cfg_attr(feature = "strict", deny(warnings))]

use std::ops::Range;
use std::{fmt::Debug, sync::Arc};

use crate::error::Result;
pub use block::BlockConverter;
use cas::key::Key;
use cas::singleflight;
pub use disk::DiskCache;
pub use error::CacheError;
pub use interface::{BlockReadRequest, BlockReader, FileMetadata};
pub use metrics::set_metrics_service_name;
pub use xorb_cache::XorbCacheImpl;

mod block;
mod disk;
mod error;
mod interface;
pub mod lru;
mod metrics;
mod util;
mod xorb_cache;

/// Provides a way for the the cache to read data from the remote source on cache miss.
/// Clients of the cache should adapt their remote store to this API.
#[mockall::automock]
#[async_trait::async_trait]
pub trait Remote: Debug + Sync + Send {
    async fn fetch(
        &self,
        key: &cas::key::Key,
        range: Range<u64>,
    ) -> std::result::Result<Vec<u8>, anyhow::Error>;
}

/// A XorbCache is the top level caching service that can be used to read
/// data from a Xorb.
#[async_trait::async_trait]
pub trait XorbCache: Debug + Sync + Send {
    async fn fetch_xorb_range(
        &self,
        key: &Key,
        range: Range<u64>,
        size: Option<u64>,
    ) -> Result<Vec<u8>>;
    async fn put_cache(&self, key: &Key, contents: &[u8]) -> Result<()>;
}

pub struct CacheConfig {
    pub cache_dir: String,
    pub capacity: u64,   // size in bytes
    pub block_size: u64, // size in bytes
}

/// Factory method for building the XORB cache.
pub fn from_config<ErrorType: Debug + Sync + Send + 'static>(
    cfg: CacheConfig,
    remote: Arc<dyn Remote>,
) -> Result<Arc<dyn XorbCache>> {
    let cache = DiskCache::from_config(cfg.cache_dir.as_str(), cfg.capacity)?;
    let converter = BlockConverter::new(cfg.block_size);
    let request_merger = singleflight::Group::new();

    Ok(Arc::new(XorbCacheImpl::new(
        cache,
        remote,
        converter,
        request_merger,
    )))
}
