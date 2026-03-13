#[derive(Default, Debug, Clone, Copy)]
pub struct DeduplicationMetrics {
    pub total_bytes: u64,
    pub deduped_bytes: u64,
    pub new_bytes: u64,
    pub deduped_bytes_by_global_dedup: u64,
    pub defrag_prevented_dedup_bytes: u64,

    pub total_chunks: u64,
    pub deduped_chunks: u64,
    pub new_chunks: u64,
    pub deduped_chunks_by_global_dedup: u64,
    pub defrag_prevented_dedup_chunks: u64,

    pub xorb_bytes_uploaded: u64,
    pub shard_bytes_uploaded: u64,
    pub total_bytes_uploaded: u64,
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
