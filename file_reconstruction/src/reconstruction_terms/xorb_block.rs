use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback};
use cas_types::ChunkRange;
use merklehash::MerkleHash;
use progress_tracking::download_tracking::DownloadTaskUpdater;
use tokio::sync::{Mutex, RwLock};
use utils::UniqueId;

use crate::error::Result;
use crate::reconstruction_terms::retrieval_urls::{TermBlockRetrievalURLs, XorbURLProvider};

/// Downloaded and decompressed data for a xorb block, including chunk boundary offsets.
pub struct XorbBlockData {
    pub chunk_offsets: Vec<usize>,
    pub uncompressed_size: u64,
    pub data: Bytes,
}

#[derive(Debug)]
pub struct XorbReference {
    pub term_chunks: ChunkRange,
    pub uncompressed_size: usize,
}

/// A downloadable xorb block identified by hash and chunk range, with cached data.
/// Multiple file terms may reference the same xorb block.
pub struct XorbBlock {
    pub xorb_hash: MerkleHash,
    pub chunk_range: ChunkRange,
    pub xorb_block_index: usize,
    /// All file-term chunk ranges covered by this xorb block, sorted by range start.
    pub references: Vec<XorbReference>,
    /// Expected decompressed size of the block when known. Used for debug_assert in clients.
    pub uncompressed_size_if_known: Option<usize>,
    pub data: RwLock<Option<Arc<XorbBlockData>>>,
}

impl PartialEq for XorbBlock {
    fn eq(&self, other: &Self) -> bool {
        self.xorb_hash == other.xorb_hash
            && self.chunk_range == other.chunk_range
            && self.xorb_block_index == other.xorb_block_index
    }
}

impl Eq for XorbBlock {}

impl XorbBlock {
    /// Retrieve the xorb block data from the client, caching it for subsequent calls.
    pub async fn retrieve_data(
        self: Arc<Self>,
        client: Arc<dyn Client>,
        permit: ConnectionPermit,
        url_info: Arc<TermBlockRetrievalURLs>,
        progress_updater: Option<Arc<DownloadTaskUpdater>>,
    ) -> Result<Arc<XorbBlockData>> {
        // Check again in case another task already downloaded it.
        if let Some(ref xorb_block_data) = *self.data.read().await {
            return Ok(xorb_block_data.clone());
        }

        // Okay, now it's not there, so let's retrieve it.
        let mut xbd_lg = self.data.write().await;

        // See if it's been filled while we were waiting for the write lock.
        if let Some(ref xorb_block_data) = *xbd_lg {
            return Ok(xorb_block_data.clone());
        }

        let url_provider = XorbURLProvider {
            client: client.clone(),
            url_info,
            xorb_block_index: self.xorb_block_index,
            last_acquisition_id: Mutex::new(UniqueId::null()),
        };

        // Progress callback reports only transfer (network) bytes during get_file_term_data.
        // Decompressed bytes are reported by the data writer when written to disk.
        let progress_callback: Option<ProgressCallback> = progress_updater.as_ref().map(|updater| {
            let updater = updater.clone();
            Arc::new(move |delta: u64, _completed: u64, _total: u64| {
                updater.update_progress(0, delta); // Updates only the transfer bytes
            }) as ProgressCallback
        });

        let (data, chunk_byte_offsets) = client
            .get_file_term_data(Box::new(url_provider), permit, progress_callback, self.uncompressed_size_if_known)
            .await?;

        let chunk_offsets: Vec<usize> = chunk_byte_offsets.iter().map(|&x| x as usize).collect();
        let uncompressed_size = data.len() as u64;

        let xorb_block_data = Arc::new(XorbBlockData {
            chunk_offsets,
            uncompressed_size,
            data,
        });

        // Store the data in the xorb block.
        *xbd_lg = Some(xorb_block_data.clone());

        Ok(xorb_block_data)
    }

    /// Determines the total uncompressed size of the xorb block from the reference terms,
    /// if possible.
    ///
    /// The size can be determined when:
    /// 1. A single term's chunk range exactly matches the full xorb range, or
    /// 2. A chain of term chunk ranges exactly covers the full xorb range with no gaps (e.g. [0..3, 3..5] covers 0..5).
    ///
    /// The `terms` slice must be sorted by chunk range start index.
    pub fn determine_size_if_possible(xorb_range: ChunkRange, terms: &[XorbReference]) -> Option<usize> {
        debug_assert!(
            terms.windows(2).all(|w| w[0].term_chunks.start <= w[1].term_chunks.start),
            "terms must be sorted by chunk range start"
        );

        // DP approach: track which chunk endpoints are reachable from xorb_range.start
        // via contiguous chains, along with accumulated uncompressed sizes.
        // This correctly handles multiple terms with the same start index by
        // considering all possible chain continuations.
        let mut reachable: BTreeMap<u32, usize> = BTreeMap::new();
        reachable.insert(xorb_range.start, 0);

        for term in terms {
            if let Some(&accumulated) = reachable.get(&term.term_chunks.start) {
                reachable
                    .entry(term.term_chunks.end)
                    .or_insert(accumulated + term.uncompressed_size);
            }
        }

        reachable.get(&xorb_range.end).copied()
    }
}

#[cfg(test)]
mod tests {
    use cas_types::ChunkRange;

    use super::*;

    fn build_refs(pairs: &[(ChunkRange, usize)]) -> Vec<XorbReference> {
        pairs
            .iter()
            .map(|(range, size)| XorbReference {
                term_chunks: *range,
                uncompressed_size: *size,
            })
            .collect()
    }

    #[test]
    fn test_single_term_exact_match() {
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(0, 5), 1000)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_two_terms_chained() {
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(0, 3), 600), (ChunkRange::new(3, 5), 400)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_three_terms_chained() {
        let xorb_range = ChunkRange::new(0, 6);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(2, 4), 300),
            (ChunkRange::new(4, 6), 500),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_gap_in_chain() {
        let xorb_range = ChunkRange::new(0, 6);
        let terms = build_refs(&[(ChunkRange::new(0, 2), 200), (ChunkRange::new(4, 6), 500)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_does_not_start_at_xorb_start() {
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(1, 5), 800)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_does_not_end_at_xorb_end() {
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(0, 3), 600)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_empty_terms() {
        let xorb_range = ChunkRange::new(0, 5);
        let terms: Vec<XorbReference> = vec![];
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_overlapping_terms_with_exact_cover() {
        // Terms [0..3, 1..4, 3..5] - the chain 0..3, 3..5 covers 0..5.
        // The overlapping term 1..4 should be skipped.
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 600),
            (ChunkRange::new(1, 4), 700),
            (ChunkRange::new(3, 5), 400),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_duplicate_terms_first_covers() {
        // Two identical terms covering the full range.
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(0, 5), 1000), (ChunkRange::new(0, 5), 1000)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_nonzero_xorb_start() {
        let xorb_range = ChunkRange::new(3, 8);
        let terms = build_refs(&[(ChunkRange::new(3, 5), 400), (ChunkRange::new(5, 8), 600)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(1000));
    }

    #[test]
    fn test_nonzero_xorb_start_no_match() {
        let xorb_range = ChunkRange::new(3, 8);
        let terms = build_refs(&[(ChunkRange::new(3, 5), 400)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_single_chunk_range() {
        let xorb_range = ChunkRange::new(0, 1);
        let terms = build_refs(&[(ChunkRange::new(0, 1), 42)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(42));
    }

    #[test]
    fn test_chain_with_extra_terms_before_and_after() {
        // Extra terms that don't participate in the chain but are within the sorted list.
        let xorb_range = ChunkRange::new(2, 8);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 100),  // before xorb range
            (ChunkRange::new(2, 5), 500),  // chain start
            (ChunkRange::new(3, 6), 999),  // overlapping, skipped
            (ChunkRange::new(5, 8), 300),  // chain end
            (ChunkRange::new(8, 10), 200), // after xorb range
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(800));
    }

    #[test]
    fn test_partial_overlap_no_cover() {
        // Terms partially overlap but don't form a contiguous chain covering the full range.
        let xorb_range = ChunkRange::new(0, 10);
        let terms = build_refs(&[
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(3, 7), 400),
            (ChunkRange::new(6, 10), 400),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_same_start_short_then_long_covering_full() {
        // Short range first, then a long range that covers the full xorb.
        let xorb_range = ChunkRange::new(0, 5);
        let terms = build_refs(&[(ChunkRange::new(0, 3), 300), (ChunkRange::new(0, 5), 500)]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(500));
    }

    #[test]
    fn test_same_start_short_then_long_with_chain() {
        // Short range first, then a longer range, where the short range can also chain.
        let xorb_range = ChunkRange::new(0, 6);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 6), 300),
        ]);
        // Chain via 0..3 + 3..6 = 600
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(600));
    }

    #[test]
    fn test_same_start_multiple_duplicates_chain_through_second() {
        // Multiple terms at start 0 with different lengths; only the middle one chains.
        let xorb_range = ChunkRange::new(0, 6);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(0, 5), 500),
            (ChunkRange::new(4, 6), 200),
        ]);
        // Chain via 0..4 + 4..6 = 600
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(600));
    }

    #[test]
    fn test_same_start_at_midpoint() {
        // Duplicate starts at a midpoint in the chain, not just at the beginning.
        let xorb_range = ChunkRange::new(0, 8);
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 5), 200),
            (ChunkRange::new(3, 6), 300),
            (ChunkRange::new(6, 8), 200),
        ]);
        // Chain via 0..3 + 3..6 + 6..8 = 800
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(800));
    }

    #[test]
    fn test_same_start_none_covers() {
        // Multiple terms at start 0, but none chain to cover the full range.
        let xorb_range = ChunkRange::new(0, 10);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(0, 6), 600),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), None);
    }

    #[test]
    fn test_same_start_two_groups_chained() {
        // Two groups of duplicate-start terms that chain together.
        let xorb_range = ChunkRange::new(0, 6);
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 5), 200),
            (ChunkRange::new(3, 6), 300),
        ]);
        // Chain via 0..3 + 3..6 = 600
        assert_eq!(XorbBlock::determine_size_if_possible(xorb_range, &terms), Some(600));
    }
}
