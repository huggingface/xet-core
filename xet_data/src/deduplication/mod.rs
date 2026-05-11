mod chunking;
mod data_aggregator;
mod dedup_metrics;
mod defrag_prevention;
mod file_deduplication;
mod interface;

pub use chunking::{Chunker, find_partitions, next_stable_chunk_boundary};
pub use data_aggregator::DataAggregator;
pub use dedup_metrics::DeduplicationMetrics;
pub use file_deduplication::FileDeduper;
pub use interface::DeduplicationDataInterface;
// Re-export types that moved to xorb_object for backward compatibility.
pub use xet_core_structures::xorb_object::Chunk;
pub use xet_core_structures::xorb_object::{RawXorbData, constants, test_utils};
