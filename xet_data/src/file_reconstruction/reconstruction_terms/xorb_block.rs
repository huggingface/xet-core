use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex;
#[cfg(target_family = "wasm")]
use tokio_with_wasm::alias as tokio;
use xet_client::ClientError;
use xet_client::cas_client::adaptive_concurrency::ConnectionPermit;
use xet_client::cas_client::{Client, ProgressCallback, XorbBlockData};
use xet_client::cas_types::ChunkRange;
#[cfg(not(target_family = "wasm"))]
use xet_client::cas_types::Key;
use xet_client::chunk_cache::ChunkCache;
use xet_core_structures::merklehash::MerkleHash;
use xet_runtime::core::XetContext;
use xet_runtime::utils::UniqueId;

use super::super::FileReconstructionError;
use super::super::error::Result;
use super::super::run_state::RunState;
use super::retrieval_urls::{TermBlockRetrievalURLs, XorbURLProvider};
use crate::progress_tracking::ItemProgressUpdater;

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

/// A downloadable xorb block identified by hash and chunk ranges, with its shared data buffer.
///
/// A block may contain multiple disjoint chunk ranges from the same xorb (V2 multi-range).
/// Multiple file terms may reference the same block. The first term to need the block starts a single
/// download task that appends decompressed chunks to `data`; every term on the block reads its bytes from
/// `data` as they become ready.
pub struct XorbBlock {
    pub xorb_hash: MerkleHash,
    /// The chunk ranges fetched for this block. For V1 this is a single range;
    /// for V2 multi-range fetches this may contain multiple disjoint ranges.
    pub chunk_ranges: Vec<ChunkRange>,
    /// Index into the parent `TermBlockRetrievalURLs` for URL lookup.
    pub xorb_block_index: usize,
    /// All file-term references covered by this block, sorted by chunk range start.
    pub references: Vec<XorbReference>,
    /// Exact total decompressed size across all chunk ranges, if it can be determined from the references.
    pub uncompressed_size_if_known: Option<usize>,
    /// The block's decompressed data, filled by the download task and read by the block's terms.
    pub data: Arc<XorbBlockData>,
    /// Set by the first term that starts the download, so the block is downloaded once.
    fetch_started: AtomicBool,
}

impl PartialEq for XorbBlock {
    fn eq(&self, other: &Self) -> bool {
        self.xorb_hash == other.xorb_hash
            && self.chunk_ranges == other.chunk_ranges
            && self.xorb_block_index == other.xorb_block_index
    }
}

impl Eq for XorbBlock {}

/// Fails the block's data if the download task ends without finishing it (a panic, or the runtime shutting down),
/// so that readers waiting on the block are woken instead of waiting forever.
struct FailUnfinishedOnDrop(Arc<XorbBlockData>);

impl Drop for FailUnfinishedOnDrop {
    fn drop(&mut self) {
        self.0
            .fail(Arc::new(ClientError::Other("xorb block download ended before completing".to_string())));
    }
}

impl XorbBlock {
    /// Creates a block from its chunk ranges and the terms that reference it, sizing its data buffer from the
    /// terms' decompressed sizes.
    ///
    /// The terms give no per-chunk sizes, so the block's exact size is known only when terms sit end to end
    /// across it. When terms overlap, the buffer is sized to the smallest sum of terms that covers the block, an
    /// upper bound. A block that no set of terms covers is rejected.
    pub fn new(
        xorb_hash: MerkleHash,
        chunk_ranges: Vec<ChunkRange>,
        xorb_block_index: usize,
        mut references: Vec<XorbReference>,
    ) -> Result<Self> {
        references.sort_by_key(|r| r.term_chunks.start);
        let uncompressed_size_if_known = Self::determine_size_if_possible(&chunk_ranges, &references);
        let data = match uncompressed_size_if_known {
            Some(size) => XorbBlockData::with_exact_size(size),
            None => {
                let bound = Self::determine_size_bound(&chunk_ranges, &references).ok_or_else(|| {
                    FileReconstructionError::CorruptedReconstruction(format!(
                        "terms do not cover xorb block {xorb_hash} chunk ranges {chunk_ranges:?}"
                    ))
                })?;
                XorbBlockData::with_size_bound(bound)
            },
        };

        Ok(Self {
            xorb_hash,
            chunk_ranges,
            xorb_block_index,
            references,
            uncompressed_size_if_known,
            data: Arc::new(data),
            fetch_started: AtomicBool::new(false),
        })
    }

    /// Starts downloading the block into `data` in a background task, unless a term already started it.
    ///
    /// The download permit is acquired here, before the task is spawned, so that blocks queue for permits in the
    /// order their terms are scheduled, which is file order. Acquiring it inside the spawned task would queue blocks
    /// in whatever order the runtime happens to run the tasks, and a stream could wait behind several later blocks
    /// before its first bytes arrive.
    ///
    /// On failure the task marks `data` failed, which wakes its readers, and records the error in `run_state` so
    /// the reconstruction stops even if no reader is waiting on this block yet. If the reconstruction is cancelled,
    /// the download is dropped and `data` is marked failed. The same happens if this call is dropped while waiting
    /// for the permit.
    pub(crate) async fn start_fetch(
        self: &Arc<Self>,
        ctx: XetContext,
        client: Arc<dyn Client>,
        url_info: Arc<TermBlockRetrievalURLs>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
        chunk_cache: Option<Arc<dyn ChunkCache>>,
        run_state: Arc<RunState>,
    ) -> Result<()> {
        if self.fetch_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let fail_guard = FailUnfinishedOnDrop(self.data.clone());
        let permit = match client.acquire_download_permit().await {
            Ok(permit) => permit,
            Err(err) => {
                let err = Arc::new(err);
                self.data.fail(err.clone());
                return Err(FileReconstructionError::ClientError(err));
            },
        };

        let block = self.clone();
        tokio::task::spawn(async move {
            let _fail_guard = fail_guard;

            let result = tokio::select! {
                biased;
                _ = run_state.cancelled() => {
                    Err(ClientError::Other("reconstruction cancelled".to_string()))
                },
                result = block.fetch(ctx, client, url_info, progress_updater, chunk_cache, permit) => result,
            };

            if let Err(err) = result {
                let err = Arc::new(err);
                // Record the error before waking readers, so that they find it in the run state.
                if !run_state.is_cancelled() {
                    run_state.set_error(FileReconstructionError::ClientError(err.clone()));
                }
                block.data.fail(err);
            }
        });
        Ok(())
    }

    /// Fills `data` from the chunk cache or, failing that, from CAS using `permit`, then marks it finished.
    async fn fetch(
        &self,
        ctx: XetContext,
        client: Arc<dyn Client>,
        url_info: Arc<TermBlockRetrievalURLs>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
        #[cfg_attr(target_family = "wasm", allow(unused_variables))] chunk_cache: Option<Arc<dyn ChunkCache>>,
        permit: ConnectionPermit,
    ) -> std::result::Result<(), ClientError> {
        let xorb_block_index = self.xorb_block_index;

        // NOTE: cache key uses only the first ChunkRange. This works when each
        // XorbBlock has a single range, but will need rework if multi-range
        // blocks (multiple disjoint chunk ranges per block) are cached.
        // Wasm has no disk-backed ChunkCache.
        #[cfg(not(target_family = "wasm"))]
        let cache_key = Key {
            prefix: ctx.config.data.default_prefix.clone(),
            hash: self.xorb_hash,
        };
        #[cfg(not(target_family = "wasm"))]
        let chunk_range = self.chunk_ranges.first().copied().unwrap_or_default();

        #[cfg(not(target_family = "wasm"))]
        let permit = match chunk_cache.as_ref() {
            Some(cache) => match cache.get(&cache_key, &chunk_range).await {
                Ok(Some(cache_range)) => {
                    // A cache hit is a local read: free the connection slot for the next block at once. Dropping
                    // an unused permit reports nothing to the concurrency controller.
                    drop(permit);

                    // A cached range that doesn't fit this block is ignored, and the block is downloaded instead.
                    // A failed `append_chunks` writes nothing, so the download starts from an empty block.
                    let fits = self
                        .uncompressed_size_if_known
                        .is_none_or(|size| size == cache_range.data.len());
                    if fits && self.data.append_chunks(&cache_range.data, &cache_range.offsets).is_ok() {
                        // Report cached bytes as completed so progress tracking stays consistent.
                        if let Some(ref updater) = progress_updater {
                            let (_, _, http_ranges) = url_info.get_retrieval_url(xorb_block_index).await;
                            let transfer_bytes: u64 = http_ranges.iter().map(|r| r.length()).sum();
                            updater.report_transfer_bytes_completed(transfer_bytes);
                        }
                        return self.data.finish();
                    }
                    client.acquire_download_permit().await?
                },
                _ => permit,
            },
            None => permit,
        };

        let url_provider = XorbURLProvider {
            ctx: ctx.clone(),
            client: client.clone(),
            url_info,
            xorb_block_index,
            last_acquisition_id: Mutex::new(UniqueId::null()),
        };

        // Progress callback reports only transfer (network) bytes during get_file_term_data.
        // Decompressed bytes are reported by the data writer when written out.
        let progress_callback: Option<ProgressCallback> = progress_updater.as_ref().map(|updater| {
            let updater = updater.clone();
            Arc::new(move |delta: u64, _completed: u64, _total: u64| {
                updater.report_transfer_bytes_completed(delta);
            }) as ProgressCallback
        });

        client
            .get_file_term_data(Box::new(url_provider), permit, progress_callback, self.data.clone())
            .await?;
        self.data.finish()?;

        // Store in chunk cache (best-effort, non-blocking).
        #[cfg(not(target_family = "wasm"))]
        if let Some(cache) = chunk_cache
            && let Some((data, chunk_byte_offsets)) = self.data.contents()
        {
            tokio::spawn(async move {
                if let Err(err) = cache.put(&cache_key, &chunk_range, &chunk_byte_offsets, &data).await {
                    tracing::warn!("chunk cache put failed: {err}");
                }
            });
        }

        Ok(())
    }

    /// Returns an upper bound on the block's total uncompressed size: the smallest sum of term sizes over a set
    /// of terms that together cover every chunk range of the block, allowing terms to overlap. Equals the exact
    /// size when terms sit end to end.
    ///
    /// Same forward pass as [`determine_size_if_possible`](Self::determine_size_if_possible), except a term may
    /// extend coverage from any reachable position inside it rather than only from its start. Processing terms by
    /// start finds the minimum: a path that uses a term B and then an earlier-starting term A can use A alone
    /// from where B started, which covers at least as much for less.
    ///
    /// Returns `None` if some chunk is covered by no term. The `terms` slice must be sorted by
    /// `term_chunks.start`.
    pub fn determine_size_bound(xorb_ranges: &[ChunkRange], terms: &[XorbReference]) -> Option<usize> {
        debug_assert!(terms.windows(2).all(|w| w[0].term_chunks.start <= w[1].term_chunks.start));

        let Some(first_range) = xorb_ranges.first() else {
            return Some(0);
        };

        let gap_bridges: BTreeMap<u32, u32> = xorb_ranges
            .windows(2)
            .filter(|pair| pair[0].end < pair[1].start)
            .map(|pair| (pair[0].end, pair[1].start))
            .collect();

        // Covered-up-to position -> smallest accumulated size reaching it.
        let mut reachable: BTreeMap<u32, usize> = BTreeMap::new();
        reachable.insert(first_range.start, 0);

        for term in terms {
            let Some(from) = reachable
                .range(term.term_chunks.start..term.term_chunks.end)
                .map(|(_, &size)| size)
                .min()
            else {
                continue;
            };
            let new_size = from + term.uncompressed_size;
            for position in std::iter::once(term.term_chunks.end).chain(gap_bridges.get(&term.term_chunks.end).copied())
            {
                reachable
                    .entry(position)
                    .and_modify(|size| *size = (*size).min(new_size))
                    .or_insert(new_size);
            }
        }

        reachable.get(&xorb_ranges.last()?.end).copied()
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
    use xet_client::cas_types::ChunkRange;

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

    #[test]
    fn test_size_bound_equals_exact_size_when_terms_tile() {
        let ranges = &[ChunkRange::new(0, 8)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(3, 5), 200),
            (ChunkRange::new(5, 8), 300),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(800));
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), Some(800));
    }

    #[test]
    fn test_size_bound_with_overlapping_terms() {
        // [0,5) and [3,8) overlap on chunks 3 and 4, whose size is unknown.
        let ranges = &[ChunkRange::new(0, 8)];
        let terms = build_refs(&[(ChunkRange::new(0, 5), 500), (ChunkRange::new(3, 8), 500)]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), Some(1000));
    }

    #[test]
    fn test_size_bound_picks_smallest_cover() {
        // Covers: [0,8) alone = 900; [0,5) + [4,8) = 500 + 450 = 950; [0,5) + [2,8) = 1100.
        let ranges = &[ChunkRange::new(0, 8)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 5), 500),
            (ChunkRange::new(0, 8), 900),
            (ChunkRange::new(2, 8), 600),
            (ChunkRange::new(4, 8), 450),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), Some(900));
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), Some(900));

        let terms = build_refs(&[
            (ChunkRange::new(0, 5), 500),
            (ChunkRange::new(2, 8), 600),
            (ChunkRange::new(4, 8), 450),
        ]);
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), Some(950));
    }

    #[test]
    fn test_size_bound_across_disjoint_ranges() {
        let ranges = &[ChunkRange::new(0, 4), ChunkRange::new(6, 9)];
        let terms = build_refs(&[
            (ChunkRange::new(0, 3), 300),
            (ChunkRange::new(2, 4), 200),
            (ChunkRange::new(6, 9), 300),
        ]);
        assert_eq!(XorbBlock::determine_size_if_possible(ranges, &terms), None);
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), Some(800));
    }

    #[test]
    fn test_size_bound_none_when_a_chunk_is_uncovered() {
        let ranges = &[ChunkRange::new(0, 8)];
        let terms = build_refs(&[(ChunkRange::new(0, 3), 300), (ChunkRange::new(4, 8), 400)]);
        assert_eq!(XorbBlock::determine_size_bound(ranges, &terms), None);
    }
}
