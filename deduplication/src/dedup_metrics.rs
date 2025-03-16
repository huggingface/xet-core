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

    pub xorb_bytes_uploaded: usize,
    pub shard_bytes_uploaded: usize,
    pub total_bytes_uploaded: usize,
}

/// Implement + for the metrics above, so they can be added
/// and updated after each call to process_chunks.
impl DeduplicationMetrics {
    pub fn merge_in(&mut self, other: &Self) {
        self.total_bytes += other.total_bytes;
        self.deduped_bytes += other.deduped_bytes;
        self.new_bytes += other.new_bytes;
        self.deduped_bytes_by_global_dedup += other.deduped_bytes_by_global_dedup;
        self.defrag_prevented_dedup_bytes += other.defrag_prevented_dedup_bytes;

        self.total_chunks += other.total_chunks;
        self.deduped_chunks += other.deduped_chunks;
        self.new_chunks += other.new_chunks;
        self.deduped_chunks_by_global_dedup += other.deduped_chunks_by_global_dedup;
        self.defrag_prevented_dedup_chunks += other.defrag_prevented_dedup_chunks;

        self.xorb_bytes_uploaded += other.xorb_bytes_uploaded;
        self.shard_bytes_uploaded += other.shard_bytes_uploaded;
        self.total_bytes_uploaded += other.total_bytes_uploaded;
    }
}
