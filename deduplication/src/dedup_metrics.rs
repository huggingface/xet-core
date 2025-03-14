#[derive(Default, Debug, Clone, Copy)]
pub struct DeduplicationMetrics {
    pub total_bytes: usize,
    pub deduped_bytes: usize,
    pub new_bytes: usize,
    pub deduped_bytes_by_global_dedup: usize,
    pub defrag_prevented_dedup_bytes: usize,

    pub total_chunks: usize,
    pub deduped_chunks: usize,
    pub new_chunks: usize,
    pub deduped_chunks_by_global_dedup: usize,
    pub defrag_prevented_dedup_chunks: usize,
}

/// Implement + for the metrics above, so they can be added
/// and updated after each call to process_chunks.

impl std::ops::Add for DeduplicationMetrics {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        DeduplicationMetrics {
            total_bytes: self.total_bytes + other.total_bytes,
            deduped_bytes: self.deduped_bytes + other.deduped_bytes,
            new_bytes: self.new_bytes + other.new_bytes,
            deduped_bytes_by_global_dedup: self.deduped_bytes_by_global_dedup + other.deduped_bytes_by_global_dedup,
            defrag_prevented_dedup_bytes: self.defrag_prevented_dedup_bytes + other.defrag_prevented_dedup_bytes,

            total_chunks: self.total_chunks + other.total_chunks,
            deduped_chunks: self.deduped_chunks + other.deduped_chunks,
            new_chunks: self.new_chunks + other.new_chunks,
            deduped_chunks_by_global_dedup: self.deduped_chunks_by_global_dedup + other.deduped_chunks_by_global_dedup,
            defrag_prevented_dedup_chunks: self.defrag_prevented_dedup_chunks + other.defrag_prevented_dedup_chunks,
        }
    }
}
