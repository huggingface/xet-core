mod chunking;
pub mod constants;
mod data_aggregator;
mod dedup_metrics;
mod defrag_prevention;
mod file_deduplication;
mod interface;
mod raw_xorb_data;

pub use chunking::{Chunk, Chunker};
pub use data_aggregator::DataAggregator;
pub use dedup_metrics::DeduplicationMetrics;
pub use file_deduplication::FileDeduper;
pub use interface::{DeduplicationDataInterface, FileXorbDependency};
pub use raw_xorb_data::RawXorbData;
