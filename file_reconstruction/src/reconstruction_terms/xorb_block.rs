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
///
/// A single `XorbBlockData` may hold data from multiple disjoint chunk ranges
/// (V2 multi-range fetch). The chunks are concatenated in range order, and
/// `chunk_offsets` maps each chunk index to its byte position within `data`.
pub struct XorbBlockData {
    /// Pairs of (chunk_index, byte_offset) mapping each chunk to its start position
    /// within `data`. Because the block can span multiple disjoint chunk ranges,
    /// storing the chunk index alongside the offset avoids ambiguity.
    pub chunk_offsets: Vec<(usize, usize)>,

    /// The concatenated decompressed chunk data for all ranges in this block.
    pub data: Bytes,
}

/// A reference from a file term back to the xorb block it belongs to.
/// Used by `determine_size_if_possible` to check whether the block's total
/// uncompressed size can be inferred from the terms that reference it.
#[derive(Debug)]
pub struct XorbReference {
    /// The chunk range within the xorb that this file term covers.
    pub term_chunks: ChunkRange,
    /// The uncompressed byte size of this term's data.
    pub uncompressed_size: usize,
}

/// A downloadable xorb block identified by hash and chunk ranges, with cached data.
///
/// A block may contain multiple disjoint chunk ranges from the same xorb (V2 multi-range).
/// Multiple file terms may reference the same block. Downloaded data is cached in `data`
/// so that the first term to request it triggers the download, and subsequent terms
/// reuse the cached result.
pub struct XorbBlock {
    pub xorb_hash: MerkleHash,
    /// The chunk ranges fetched for this block. For V1 this is a single range;
    /// for V2 multi-range fetches this may contain multiple disjoint ranges.
    pub chunk_ranges: Vec<ChunkRange>,
    /// Index into the parent `TermBlockRetrievalURLs` for URL lookup.
    pub xorb_block_index: usize,
    /// All file-term references covered by this block, sorted by chunk range start.
    /// Populated during `retrieve_file_term_block` and used to compute `uncompressed_size_if_known`.
    pub references: Vec<XorbReference>,
    /// Expected total decompressed size across all chunk ranges, if it can be determined
    /// from the references. Passed to clients as a debug assertion hint.
    pub uncompressed_size_if_known: Option<usize>,
    /// Cached downloaded data. `None` until the first download completes.
    pub data: RwLock<Option<Arc<XorbBlockData>>>,
}

impl PartialEq for XorbBlock {
    fn eq(&self, other: &Self) -> bool {
        self.xorb_hash == other.xorb_hash
            && self.chunk_ranges == other.chunk_ranges
            && self.xorb_block_index == other.xorb_block_index
    }
}

impl Eq for XorbBlock {}

impl XorbBlock {
    /// Retrieve the xorb block data from the client, caching it for subsequent calls.
    ///
    /// Uses double-checked locking: a fast read-lock check, then a write-lock check,
    /// so only the first caller actually downloads. Makes exactly one download call
    /// (and acquires exactly one download permit) regardless of how many chunk ranges
    /// exist — the `URLProvider` supplies all byte ranges to the client in one shot.
    pub async fn retrieve_data(
        self: Arc<Self>,
        client: Arc<dyn Client>,
        permit: ConnectionPermit,
        url_info: Arc<TermBlockRetrievalURLs>,
        progress_updater: Option<Arc<DownloadTaskUpdater>>,
    ) -> Result<Arc<XorbBlockData>> {
        // Fast path: another task may have already downloaded this block.
        if let Some(ref xorb_block_data) = *self.data.read().await {
            return Ok(xorb_block_data.clone());
        }

        // Acquire the write lock and check again (another task may have completed
        // the download while we were waiting for the lock).
        let mut xbd_lg = self.data.write().await;

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
                updater.report_transfer_progress(delta);
            }) as ProgressCallback
        });

        // Single download call: the url_provider carries all byte ranges for this block.
        // Returns decompressed data and a Vec of byte offsets marking chunk boundaries
        // (one entry per chunk, plus a final entry for total size).
        let (data, chunk_byte_offsets) = client
            .get_file_term_data(Box::new(url_provider), permit, progress_callback, self.uncompressed_size_if_known)
            .await?;

        // Build chunk_offsets by zipping each chunk index (from all chunk_ranges)
        // with the corresponding byte offset from the returned data.
        // chunk_byte_offsets[i] is the start offset of chunk i in the concatenated data.
        let mut chunk_offsets = Vec::new();
        let mut offset_idx = 0;
        for range in &self.chunk_ranges {
            for chunk_idx in range.start..range.end {
                chunk_offsets.push((chunk_idx as usize, chunk_byte_offsets[offset_idx] as usize));
                offset_idx += 1;
            }
        }

        let xorb_block_data = Arc::new(XorbBlockData { chunk_offsets, data });
        *xbd_lg = Some(xorb_block_data.clone());

        Ok(xorb_block_data)
    }

    /// Determines the total uncompressed size of the xorb block from the reference terms,
    /// if possible.
    ///
    /// Uses a forward-chaining DP: starting from the first chunk range's start,
    /// we track which chunk positions are "reachable" (i.e., fully covered by a
    /// contiguous chain of terms) along with the accumulated uncompressed size.
    ///
    /// For multi-range blocks with disjoint chunk ranges (e.g. `[0,3)` and `[5,8)`),
    /// the gaps between ranges are inserted as zero-cost bridges. This lets the DP
    /// traverse the full set of ranges in a single pass — a gap `[3,5)` contributes
    /// no data but connects the end of one range to the start of the next.
    ///
    /// Returns `Some(total_size)` if every range is fully covered, `None` otherwise.
    ///
    /// The `terms` slice must be sorted by `term_chunks.start`.
    pub fn determine_size_if_possible(xorb_ranges: &[ChunkRange], terms: &[XorbReference]) -> Option<usize> {
        debug_assert!(
            terms.windows(2).all(|w| w[0].term_chunks.start <= w[1].term_chunks.start),
            "terms must be sorted by chunk range start"
        );

        debug_assert!(
            terms.iter().all(|term| xorb_ranges
                .iter()
                .any(|r| term.term_chunks.start >= r.start && term.term_chunks.end <= r.end)),
            "all terms must fall within one of the xorb ranges"
        );

        if xorb_ranges.is_empty() {
            return Some(0);
        }

        // Build a lookup from range-end -> next-range-start for gap bridging.
        // E.g. for ranges [0,3) and [5,8), maps 3 -> 5, meaning once chunk 3
        // is reachable we can bridge to chunk 5 at zero cost.
        let gap_bridges: BTreeMap<u32, u32> = xorb_ranges
            .windows(2)
            .filter(|pair| pair[0].end < pair[1].start)
            .map(|pair| (pair[0].end, pair[1].start))
            .collect();

        // DP map: chunk position -> accumulated uncompressed size to reach that position.
        // Seed with the start of the first range.
        let mut reachable: BTreeMap<u32, usize> = BTreeMap::new();
        reachable.insert(xorb_ranges[0].start, 0);

        // Process terms in sorted order, extending reachable positions.
        for term in terms {
            if let Some(&accumulated) = reachable.get(&term.term_chunks.start) {
                let new_end = term.term_chunks.end;
                let new_size = accumulated + term.uncompressed_size;

                reachable.entry(new_end).or_insert(new_size);

                // If this term reaches the end of a range that has a gap bridge,
                // make the start of the next range reachable at the same accumulated size.
                if let Some(&bridge_target) = gap_bridges.get(&new_end) {
                    reachable.entry(bridge_target).or_insert(new_size);
                }
            }
        }

        // The block is fully covered if we can reach the end of the last range.
        reachable.get(&xorb_ranges.last().unwrap().end).copied()
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
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(0, 5), 1000)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_two_terms_chained() {
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 600), (ChunkRange::new(3, 5), 400)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_three_terms_chained() {
        let ranges = &[ChunkRange::new(0, 6)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(2, 4), 300),
            (ChunkRange::new(4, 6), 500),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_gap_in_chain() {
        let ranges = &[ChunkRange::new(0, 6)];
        let terms = build_refs(&[(ChunkRange::new(0, 2), 200), (ChunkRange::new(4, 6), 500)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_does_not_start_at_xorb_start() {
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(1, 5), 800)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_does_not_end_at_xorb_end() {
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 600)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_empty_terms() {
        let ranges = &[ChunkRange::new(0, 5)];
        let terms: Vec<XorbReference> = vec![];
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_overlapping_terms_with_exact_cover() {
        // Terms [0..3, 1..4, 3..5] - the chain 0..3, 3..5 covers 0..5.
        // The overlapping term 1..4 should be skipped.
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 600),
            (ChunkRange::new(1, 4), 700),
            (ChunkRange::new(3, 5), 400),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_duplicate_terms_first_covers() {
        // Two identical terms covering the full range.
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(0, 5), 1000), (ChunkRange::new(0, 5), 1000)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_nonzero_xorb_start() {
        let ranges = &[ChunkRange::new(3, 8)];
        let terms = build_refs(&[(ChunkRange::new(3, 5), 400), (ChunkRange::new(5, 8), 600)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_nonzero_xorb_start_no_match() {
        let ranges = &[ChunkRange::new(3, 8)];
        let terms = build_refs(&[(ChunkRange::new(3, 5), 400)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_single_chunk_range() {
        let ranges = &[ChunkRange::new(0, 1)];
        let terms = build_refs(&[(ChunkRange::new(0, 1), 42)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(42));
    }

    #[test]
    fn test_chain_with_overlapping_inner_terms() {
        let ranges = &[ChunkRange::new(2, 8)];
        // The overlapping term [3,6) is within the range but doesn't form
        // a better chain than [2,5) + [5,8), so it's harmlessly ignored.
        let terms = build_refs(&[
            (ChunkRange::new(2, 5), 500),
            (ChunkRange::new(3, 6), 999),
            (ChunkRange::new(5, 8), 300),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(800));
    }

    #[test]
    fn test_partial_overlap_no_cover() {
        // Terms partially overlap but don't form a contiguous chain covering the full range.
        let ranges = &[ChunkRange::new(0, 10)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(3, 7), 400),
            (ChunkRange::new(6, 10), 400),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_same_start_short_then_long_covering_full() {
        // Short range first, then a long range that covers the full xorb.
        let ranges = &[ChunkRange::new(0, 5)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 300), (ChunkRange::new(0, 5), 500)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(500));
    }

    #[test]
    fn test_same_start_short_then_long_with_chain() {
        // Short range first, then a longer range, where the short range can also chain.
        // Chain via 0..3 + 3..6 = 600
        let ranges = &[ChunkRange::new(0, 6)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 6), 300),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(600));
    }

    #[test]
    fn test_same_start_multiple_duplicates_chain_through_second() {
        // Multiple terms at start 0 with different lengths; only the middle one chains.
        // Chain via 0..4 + 4..6 = 600
        let ranges = &[ChunkRange::new(0, 6)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(0, 5), 500),
            (ChunkRange::new(4, 6), 200),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(600));
    }

    #[test]
    fn test_same_start_at_midpoint() {
        // Duplicate starts at a midpoint in the chain, not just at the beginning.
        // Chain via 0..3 + 3..6 + 6..8 = 800
        let ranges = &[ChunkRange::new(0, 8)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 5), 200),
            (ChunkRange::new(3, 6), 300),
            (ChunkRange::new(6, 8), 200),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(800));
    }

    #[test]
    fn test_same_start_none_covers() {
        // Multiple terms at start 0, but none chain to cover the full range.
        let ranges = &[ChunkRange::new(0, 10)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 4), 400),
            (ChunkRange::new(0, 6), 600),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }

    #[test]
    fn test_same_start_two_groups_chained() {
        // Two groups of duplicate-start terms that chain together.
        // Chain via 0..3 + 3..6 = 600
        let ranges = &[ChunkRange::new(0, 6)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 2), 200),
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 5), 200),
            (ChunkRange::new(3, 6), 300),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(600));
    }

    #[test]
    fn test_multiple_disjoint_ranges_both_covered() {
        let ranges = &[ChunkRange::new(0, 3), ChunkRange::new(5, 8)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 300), (ChunkRange::new(5, 8), 400)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(700));
    }

    #[test]
    fn test_multiple_disjoint_ranges_one_uncovered() {
        let ranges = &[ChunkRange::new(0, 3), ChunkRange::new(5, 8)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 300)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
    }
}
