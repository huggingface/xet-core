mod chunk;
mod chunking;
pub mod constants;
mod data_aggregator;
mod dedup_metrics;
mod defrag_prevention;
mod file_deduplication;
mod interface;
mod raw_xorb_data;

#[cfg(feature = "parallel-chunking")]
mod parallel_chunking;

pub use chunk::Chunk;
pub use chunking::{Chunker, find_partitions};
pub use data_aggregator::DataAggregator;
pub use dedup_metrics::DeduplicationMetrics;
pub use file_deduplication::FileDeduper;
pub use interface::DeduplicationDataInterface;
#[cfg(feature = "parallel-chunking")]
pub use parallel_chunking::chunk_file_parallel;
pub use raw_xorb_data::{RawXorbData, test_utils};
