use std::sync::Mutex;

use super::reconstruction_terms::XorbBlockData;

/// Records the length of every chunk delivered for a file, in file order.
///
/// A file's hash is computed over its chunks as they were stored. Those boundaries depend on
/// the chunker that uploaded the file, so they cannot always be recomputed from the file's
/// bytes alone. The reconstruction already carries them; this keeps them so the finished file
/// can be checked against its hash.
#[derive(Default)]
pub struct ChunkLayout {
    /// (offset of the chunk in the file, length of the chunk)
    chunks: Mutex<Vec<(u64, u64)>>,
}

impl ChunkLayout {
    /// Records the `n_chunks` chunks starting at `start_index` in `block`, the first of which
    /// begins at `first_chunk_offset` in the file.
    pub(crate) fn record(&self, first_chunk_offset: u64, start_index: usize, n_chunks: usize, block: &XorbBlockData) {
        let mut chunks = self.chunks.lock().expect("ChunkLayout mutex poisoned");
        let mut offset = first_chunk_offset;
        for k in start_index..start_index + n_chunks {
            let start = block.chunk_offsets[k].1;
            let end = block.chunk_offsets.get(k + 1).map_or(block.data.len(), |(_, next)| *next);
            let len = (end - start) as u64;
            chunks.push((offset, len));
            offset += len;
        }
    }

    /// Returns the chunk lengths in file order.
    ///
    /// Terms can finish in any order, and a fetch batch can begin part way into a chunk that
    /// the previous batch also delivered, so chunks are ordered by offset and kept once each.
    pub fn chunk_lengths(&self) -> Vec<u64> {
        let mut chunks = std::mem::take(&mut *self.chunks.lock().expect("ChunkLayout mutex poisoned"));
        chunks.sort_unstable_by_key(|(offset, _)| *offset);
        chunks.dedup_by_key(|(offset, _)| *offset);
        chunks.into_iter().map(|(_, len)| len).collect()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    /// Three chunks of 10, 20 and 30 bytes in one block.
    fn block() -> XorbBlockData {
        XorbBlockData {
            chunk_offsets: vec![(0, 0), (1, 10), (2, 30)],
            data: Bytes::from(vec![0u8; 60]),
        }
    }

    #[test]
    fn test_terms_recorded_out_of_order_come_back_in_file_order() {
        let layout = ChunkLayout::default();
        layout.record(30, 2, 1, &block());
        layout.record(0, 0, 2, &block());
        assert_eq!(layout.chunk_lengths(), vec![10, 20, 30]);
    }

    #[test]
    fn test_a_chunk_delivered_twice_is_kept_once() {
        let layout = ChunkLayout::default();
        layout.record(0, 0, 2, &block());
        layout.record(10, 1, 2, &block());
        assert_eq!(layout.chunk_lengths(), vec![10, 20, 30]);
    }
}
