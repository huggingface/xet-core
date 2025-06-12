mod chunk;
mod chunking;
pub mod constants;
mod data_aggregator;
mod dedup_metrics;
mod defrag_prevention;
mod file_deduplication;
mod interface;
mod raw_xorb_data;

pub use chunk::Chunk;
pub use chunking::{find_partitions, Chunker};
pub use data_aggregator::DataAggregator;
pub use dedup_metrics::DeduplicationMetrics;
pub use file_deduplication::FileDeduper;
pub use interface::DeduplicationDataInterface;
pub use raw_xorb_data::{test_utils, RawXorbData};
