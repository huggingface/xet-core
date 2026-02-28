use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::Client;
use cas_types::{ChunkRange, FileRange, HttpRange};
use merklehash::MerkleHash;
use progress_tracking::download_tracking::DownloadTaskUpdater;
use tokio::sync::RwLock;
use utils::UniqueId;

use crate::FileReconstructionError;
use crate::data_writer::DataFuture;
use crate::error::Result;
use crate::reconstruction_terms::retrieval_urls::TermBlockRetrievalURLs;
use crate::reconstruction_terms::xorb_block::{XorbBlock, XorbBlockData, XorbReference};
/// A single term in a file reconstruction, representing a contiguous byte range
/// in the output file that maps to a chunk range within a xorb block.
#[derive(Clone)]
pub struct FileTerm {
    // The byte range in the file of this term.
    pub byte_range: FileRange,

    // Absolute chunk range within the full xorb.  Doesn't account for only a partial xorb being downloaded.
    pub xorb_chunk_range: ChunkRange,

    // The index of the (chunk index, byte offset) pair in the xorb block that starts this file term.
    pub xorb_block_start_index: usize,

    // The byte offset into the first range of the xorb block should this term not start on a chunk boundary.
    pub offset_into_first_range: u64,

    // The xorb block that sourced this file term.
    pub xorb_block: Arc<XorbBlock>,

    // The retrieval URL information for this file term.
    pub url_info: Arc<TermBlockRetrievalURLs>,
}

impl FileTerm {
    pub fn extract_bytes(&self, xorb_block_data: &XorbBlockData) -> Bytes {
        let (_, start_byte_offset) = xorb_block_data.chunk_offsets[self.xorb_block_start_index];
        let start_byte_offset = start_byte_offset + self.offset_into_first_range as usize;
        let expected_size = (self.byte_range.end - self.byte_range.start) as usize;
        let end_byte_offset = start_byte_offset + expected_size;

        xorb_block_data.data.slice(start_byte_offset..end_byte_offset)
    }

    /// Get a future that will retrieve and extract the data bytes for this file term.
    ///
    /// If the xorb data is already cached, returns a future that immediately resolves (no progress
    /// report, since the block was already reported by the term that triggered the download).
    /// Otherwise, acquires a download permit and returns a future that downloads the data (progress
    /// is reported inside retrieve_data during get_file_term_data and in reconciliation after).
    pub async fn get_data_task(
        &self,
        client: Arc<dyn Client>,
        progress_updater: Option<Arc<DownloadTaskUpdater>>,
    ) -> Result<DataFuture> {
        // First, try to read the cached data without blocking.
        if let Ok(guard) = self.xorb_block.data.try_read()
            && let Some(ref xorb_block_data) = *guard
        {
            let bytes = self.extract_bytes(xorb_block_data);
            return Ok(Box::pin(async move { Ok(bytes) }));
        }

        // Data not cached - need to download it.
        let permit = client.acquire_download_permit().await?;

        let file_term = self.clone();
        let url_info = self.url_info.clone();
        let xorb_block = self.xorb_block.clone();

        let task = tokio::task::spawn(async move {
            let xorb_block_data = xorb_block.retrieve_data(client, permit, url_info, progress_updater).await?;
            Ok(file_term.extract_bytes(&xorb_block_data))
        });

        Ok(Box::pin(async move { task.await? }))
    }
}

/// Intermediate data for a single file term, collected during the first pass of
/// `retrieve_file_term_block` before the final `FileTerm` structs are built.
///
/// We need this because `FileTerm` requires `Arc<XorbBlock>` and `Arc<TermBlockRetrievalURLs>`,
/// which can't be constructed until all terms have been processed.
struct FileTermEntry {
    /// The byte range in the output file that this term covers.
    byte_range: FileRange,
    /// The chunk range within the xorb that sources this term's data.
    xorb_chunk_range: ChunkRange,
    /// Byte offset into the first chunk's data, non-zero only for the first term
    /// when the query range starts mid-chunk.
    offset_into_first_range: u64,
    /// Index into the `xorb_blocks` / `xorb_block_retrieval_urls` vectors.
    xorb_block_index: usize,
    /// Flattened index into the xorb block's `chunk_offsets` for this term's start chunk.
    xorb_block_start_index: usize,
}

/// Computes the index into chunk_offsets for a given chunk start within a set of chunk ranges.
/// Returns the flattened index: sum of chunks in earlier ranges + offset within the containing range.
fn compute_block_start_index(chunk_ranges: &[ChunkRange], chunk_start: u32) -> usize {
    let mut idx = 0;
    for range in chunk_ranges {
        if chunk_start >= range.start && chunk_start < range.end {
            idx += (chunk_start - range.start) as usize;
            return idx;
        }
        idx += (range.end - range.start) as usize;
    }
    idx
}

/// Retrieve file terms from the client for a given file hash and byte range.
/// Returns None if the requested byte range is past the end of the file.
/// Returns the actual retrieved range and the number of bytes required for the
/// download (with dedup and compression enabled)
/// along with the Vec<FileTerm>.
pub async fn retrieve_file_term_block(
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    query_file_byte_range: FileRange,
) -> Result<Option<(FileRange, u64, Vec<FileTerm>)>> {
    // get_reconstruction always returns V2 format (the client converts V1 internally).
    let Some(raw_reconstruction) = client.get_reconstruction(&file_hash, Some(query_file_byte_range)).await? else {
        // None means we've requested a byte range beyond the end of the file.
        return Ok(None);
    };

    // Each acquisition gets a unique ID used for single-flight URL refresh dedup.
    let acquisition_id = UniqueId::new();

    // First pass: iterate through the reconstruction terms and build up intermediate
    // FileTermEntry data, XorbBlock objects, and retrieval URL info.  We can't construct
    // the final FileTerm structs yet because they need Arc<XorbBlock> and Arc<TermBlockRetrievalURLs>,
    // which require all terms to be processed first.
    let mut file_term_data = Vec::<FileTermEntry>::with_capacity(raw_reconstruction.terms.len());

    // Parallel vectors indexed by xorb_block_index:
    // - xorb_blocks: the block metadata (hash, chunk ranges, references)
    // - xorb_block_retrieval_urls: the download URL and byte ranges for each block
    let mut xorb_blocks: Vec<XorbBlock> = Vec::new();
    let mut xorb_block_retrieval_urls = Vec::<(String, Vec<HttpRange>)>::new();

    // Dedup map: (xorb_hash, first_range_chunk_start) -> xorb_block_index.
    // Multiple terms may reference the same xorb block; this ensures we create
    // each block only once and share it across terms.
    let mut xorb_index_lookup = HashMap::<(MerkleHash, u32), usize>::new();

    // Track the current byte offset in the output file as we process terms sequentially.
    let mut cur_file_byte_offset = query_file_byte_range.start;

    for (local_term_index, term) in raw_reconstruction.terms.iter().enumerate() {
        let xorb_hash: MerkleHash = term.hash.into();

        let Some(xorb_descriptor) = raw_reconstruction.xorbs.get(&term.hash) else {
            return Err(FileReconstructionError::CorruptedReconstruction(format!(
                "Xorb info not found for xorb hash {xorb_hash:?}"
            )));
        };

        // Find the XorbMultiRangeFetch entry that contains this term's chunk range.
        // A V2 xorb descriptor may have multiple fetch entries (e.g. when ranges were
        // split due to URL length limits), so we search for the one that covers this term.
        let xorb_block_index = 'find_xorb_block: {
            for fetch_entry in xorb_descriptor.fetch.iter() {
                // Check if the term's chunk range is fully contained within
                // one of this fetch entry's ranges.
                let term_contained = fetch_entry
                    .ranges
                    .iter()
                    .any(|r| r.chunks.start <= term.range.start && term.range.end <= r.chunks.end);

                if !term_contained {
                    continue;
                }

                // Use (xorb_hash, first_chunk_start) as a dedup key so that multiple
                // terms referencing the same fetch entry share a single XorbBlock.
                let first_chunk_start = fetch_entry.ranges[0].chunks.start;

                let index = match xorb_index_lookup.entry((xorb_hash, first_chunk_start)) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        // First time seeing this fetch entry — create a new XorbBlock.
                        let new_index = xorb_blocks.len();

                        let chunk_ranges: Vec<ChunkRange> = fetch_entry.ranges.iter().map(|r| r.chunks).collect();
                        let http_ranges: Vec<HttpRange> = fetch_entry.ranges.iter().map(|r| r.bytes).collect();

                        xorb_blocks.push(XorbBlock {
                            xorb_hash,
                            chunk_ranges,
                            xorb_block_index: new_index,
                            references: vec![],
                            uncompressed_size_if_known: None,
                            data: RwLock::new(None),
                        });

                        xorb_block_retrieval_urls.push((fetch_entry.url.clone(), http_ranges));

                        entry.insert(new_index);
                        new_index
                    },
                };

                break 'find_xorb_block index;
            }
            return Err(FileReconstructionError::CorruptedReconstruction(format!(
                "No xorb fetch entry found for file term {local_term_index:?} in xorb info for xorb hash {xorb_hash:?}"
            )));
        };

        // Only the first term can have a non-zero offset into its first chunk,
        // which happens when the query byte range starts mid-chunk.
        let offset_into_first_range = if local_term_index == 0 {
            raw_reconstruction.offset_into_first_range
        } else {
            0
        };

        // The term's contribution to the output file is its full uncompressed size
        // minus any offset into the first chunk.
        let term_byte_size = term.unpacked_length as u64 - offset_into_first_range;

        // Record this term as a reference on its xorb block (used later to determine
        // whether the block's total uncompressed size can be inferred).
        xorb_blocks[xorb_block_index].references.push(XorbReference {
            term_chunks: term.range,
            uncompressed_size: term.unpacked_length as usize,
        });

        // Compute the flattened index into the block's chunk_offsets for this term's
        // starting chunk. This accounts for disjoint chunk ranges in multi-range blocks.
        let xorb_block_start_index =
            compute_block_start_index(&xorb_blocks[xorb_block_index].chunk_ranges, term.range.start);

        file_term_data.push(FileTermEntry {
            byte_range: FileRange::new(cur_file_byte_offset, cur_file_byte_offset + term_byte_size),
            xorb_chunk_range: term.range,
            offset_into_first_range,
            xorb_block_index,
            xorb_block_start_index,
        });

        cur_file_byte_offset += term_byte_size;
    }

    // Sort each block's references by chunk start so that determine_size_if_possible
    // can use its forward-chaining DP to check coverage.
    for block in &mut xorb_blocks {
        block.references.sort_by_key(|r| r.term_chunks.start);
        block.uncompressed_size_if_known =
            XorbBlock::determine_size_if_possible(&block.chunk_ranges, &block.references);
    }

    // The last term in the reconstruction may extend beyond the requested range
    // (e.g. when the query ends mid-chunk). Trim it to the query boundary.
    if cur_file_byte_offset > query_file_byte_range.end {
        let last_term_shrinkage = cur_file_byte_offset - query_file_byte_range.end;

        debug_assert!(!file_term_data.is_empty());

        if let Some(entry) = file_term_data.last_mut() {
            entry.byte_range.end -= last_term_shrinkage;
        }
    }

    // The actual range covered, which may be smaller than requested if the file
    // ends before the requested range.
    let actual_range = FileRange::new(
        file_term_data.first().map(|e| e.byte_range.start).unwrap_or(0),
        file_term_data.last().map(|e| e.byte_range.end).unwrap_or(0),
    );

    // Total compressed bytes that will be transferred across all xorb block downloads.
    let total_transfer_bytes: u64 = xorb_block_retrieval_urls
        .iter()
        .flat_map(|(_, ranges)| ranges)
        .map(|r| r.length())
        .sum();

    // Wrap the retrieval URLs in a shared struct so all file terms can share them
    // and coordinate URL refreshes through a single lock.
    let url_info =
        Arc::new(TermBlockRetrievalURLs::new(file_hash, actual_range, acquisition_id, xorb_block_retrieval_urls));

    // Second pass: convert the intermediate FileTermEntry data into final FileTerm
    // structs, now that we can wrap xorb blocks in Arc and share the url_info.
    let xorb_blocks_arc: Vec<Arc<XorbBlock>> = xorb_blocks.into_iter().map(Arc::new).collect();

    let file_terms: Vec<FileTerm> = file_term_data
        .into_iter()
        .map(|entry| FileTerm {
            byte_range: entry.byte_range,
            xorb_chunk_range: entry.xorb_chunk_range,
            xorb_block_start_index: entry.xorb_block_start_index,
            offset_into_first_range: entry.offset_into_first_range,
            xorb_block: xorb_blocks_arc[entry.xorb_block_index].clone(),
            url_info: url_info.clone(),
        })
        .collect();

    Ok(Some((actual_range, total_transfer_bytes, file_terms)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cas_client::{ClientTestingUtils, LocalClient, RandomFileContents};
    use cas_types::{ChunkRange, FileRange};
    use more_asserts::assert_le;
    use utils::UniqueId;

    use super::*;

    const TEST_CHUNK_SIZE: usize = 101;

    fn verify_xorb_block_references(file_terms: &[FileTerm]) {
        for file_term in file_terms {
            let refs = &file_term.xorb_block.references;
            assert!(
                refs.iter().any(|r| r.term_chunks == file_term.xorb_chunk_range),
                "xorb_chunk_range {:?} must be in block references {:?}",
                file_term.xorb_chunk_range,
                refs.as_slice()
            );
        }
        let mut seen_blocks = std::collections::HashSet::new();
        for file_term in file_terms {
            if seen_blocks.insert(file_term.xorb_block.xorb_block_index) {
                let refs = &file_term.xorb_block.references;
                for w in refs.windows(2) {
                    assert_le!(w[0].term_chunks.start, w[1].term_chunks.start);
                }
            }
        }
    }

    /// Creates a test client and uploads a random file with the given term specification.
    /// Returns the client and file contents for verification.
    async fn setup_test_file(term_spec: &[(u64, (u64, u64))]) -> (Arc<LocalClient>, RandomFileContents) {
        let client = LocalClient::temporary().await.unwrap();
        let file_contents = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();
        (client, file_contents)
    }

    /// Retrieves file terms and thoroughly verifies their correctness.
    ///
    /// If `requested_range` is None, retrieves the full file range.
    ///
    /// This function:
    /// - Retrieves file terms from the client for the given range
    /// - Verifies file terms are contiguous and cover the range
    /// - Verifies each file term's xorb block references are valid
    /// - Verifies chunk ranges are within xorb block boundaries
    /// - Cross-references with the known file contents for correctness
    /// - Verifies number of file terms matches expected from term_spec
    async fn retrieve_and_verify(
        client: &Arc<LocalClient>,
        file_contents: &RandomFileContents,
        requested_range: Option<FileRange>,
    ) {
        let requested_range = requested_range.unwrap_or_else(|| FileRange::new(0, file_contents.data.len() as u64));
        let dyn_client: Arc<dyn Client> = client.clone();

        let (returned_range, _, file_terms) =
            retrieve_file_term_block(dyn_client.clone(), file_contents.file_hash, requested_range)
                .await
                .expect("retrieve_file_term_block should succeed")
                .expect("file_terms should not be None for valid range");

        // Verify the returned range matches the requested range.
        assert_eq!(returned_range, requested_range);

        // Track position within the requested range.
        let mut current_pos = requested_range.start;
        let mut file_term_data_offset = 0usize;

        // Find the starting term index in file_contents based on requested_range.start.
        let mut expected_term_idx = 0;
        let mut byte_offset = 0u64;
        for (idx, term) in file_contents.terms.iter().enumerate() {
            let term_end = byte_offset + term.data.len() as u64;
            if term_end > requested_range.start {
                expected_term_idx = idx;
                file_term_data_offset = (requested_range.start - byte_offset) as usize;
                break;
            }
            byte_offset = term_end;
        }

        // Collect unique xorb block indices to verify count
        let mut seen_xorb_indices = std::collections::HashSet::new();

        // Now verify actual data reconstruction by fetching all file terms.
        let mut reconstructed_data = Vec::with_capacity((requested_range.end - requested_range.start) as usize);
        let mut term_count = 0;

        for file_term in &file_terms {
            // Verify byte range is contiguous.
            assert_eq!(file_term.byte_range.start, current_pos);
            assert!(file_term.byte_range.end > file_term.byte_range.start);
            assert_le!(file_term.byte_range.end, requested_range.end);

            // Track xorb block index
            seen_xorb_indices.insert(file_term.xorb_block.xorb_block_index);

            // Verify chunk range is within xorb block boundaries: the term's chunk range
            // must be contained within at least one of the block's chunk ranges.
            let xorb_block = &file_term.xorb_block;
            let term_in_some_range = xorb_block
                .chunk_ranges
                .iter()
                .any(|cr| file_term.xorb_chunk_range.start >= cr.start && file_term.xorb_chunk_range.end <= cr.end);
            assert!(
                term_in_some_range,
                "term chunk range {:?} not within any block chunk range {:?}",
                file_term.xorb_chunk_range, xorb_block.chunk_ranges
            );

            // Cross-reference with known file contents.
            if expected_term_idx < file_contents.terms.len() {
                let expected_term = &file_contents.terms[expected_term_idx];

                // Verify xorb hash matches.
                assert_eq!(xorb_block.xorb_hash, expected_term.xorb_hash);

                // Verify chunk range matches (accounting for partial first term).
                if file_term_data_offset == 0 {
                    assert_eq!(file_term.xorb_chunk_range.start, expected_term.chunk_start);
                }
            }

            // Verify all xorb blocks referenced have valid hashes.
            assert!(file_contents.xorbs.contains_key(&file_term.xorb_block.xorb_hash));

            // Get the data task and await it.
            let data_future = file_term.get_data_task(dyn_client.clone(), None).await.unwrap();
            let data = data_future.await.unwrap();

            // Verify the data size matches the byte range.
            let expected_size = (file_term.byte_range.end - file_term.byte_range.start) as usize;
            assert_eq!(data.len(), expected_size, "Term {term_count} data size mismatch");

            reconstructed_data.extend_from_slice(&data);

            current_pos = file_term.byte_range.end;
            expected_term_idx += 1;
            file_term_data_offset = 0;
            term_count += 1;
        }

        // Verify we covered the entire requested range.
        assert_eq!(current_pos, requested_range.end);

        // For full file range, verify we have the expected number of file terms.
        if requested_range.start == 0 && requested_range.end == file_contents.data.len() as u64 {
            assert_eq!(term_count, file_contents.terms.len());
        }

        // Compare reconstructed data with expected file contents.
        let expected_data = &file_contents.data[requested_range.start as usize..requested_range.end as usize];
        assert_eq!(reconstructed_data.len(), expected_data.len());
        assert_eq!(reconstructed_data, expected_data);

        verify_xorb_block_references(&file_terms);
    }

    // ==================== Test Cases ====================

    #[tokio::test]
    async fn test_xorb_block_references_exact() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))]).await;
        let file_range = FileRange::new(0, file_contents.data.len() as u64);
        let dyn_client: Arc<dyn Client> = client.clone();
        let (_, _, file_terms) = retrieve_file_term_block(dyn_client, file_contents.file_hash, file_range)
            .await
            .unwrap()
            .unwrap();
        verify_xorb_block_references(&file_terms);
        assert_eq!(file_terms.len(), 3);
        let block = &file_terms[0].xorb_block;
        let ref_ranges: Vec<ChunkRange> = block.references.iter().map(|r| r.term_chunks).collect();
        let expected = vec![ChunkRange::new(0, 2), ChunkRange::new(2, 4), ChunkRange::new(4, 6)];
        assert_eq!(ref_ranges, expected);

        let (client2, file_contents2) = setup_test_file(&[(1, (0, 5)), (1, (0, 5))]).await;
        let file_range2 = FileRange::new(0, file_contents2.data.len() as u64);
        let dyn_client2: Arc<dyn Client> = client2.clone();
        let (_, _, file_terms2) = retrieve_file_term_block(dyn_client2, file_contents2.file_hash, file_range2)
            .await
            .unwrap()
            .unwrap();
        verify_xorb_block_references(&file_terms2);
        let block2 = &file_terms2[0].xorb_block;
        let ref_ranges2: Vec<ChunkRange> = block2.references.iter().map(|r| r.term_chunks).collect();
        let expected2 = vec![ChunkRange::new(0, 5), ChunkRange::new(0, 5)];
        assert_eq!(ref_ranges2, expected2);
    }

    #[tokio::test]
    async fn test_single_xorb_full_range() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_multiple_terms_same_xorb() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_multiple_xorbs() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 2)), (3, (0, 4))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_overlapping_chunk_ranges() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (1, (1, 3)), (1, (2, 4))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_partial_range_middle() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(file_len / 4, file_len * 3 / 4))).await;
    }

    #[tokio::test]
    async fn test_partial_range_start() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(0, file_len / 2))).await;
    }

    #[tokio::test]
    async fn test_partial_range_end() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(file_len / 2, file_len))).await;
    }

    #[tokio::test]
    async fn test_beyond_file_end() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3))]).await;
        let file_len = file_contents.data.len() as u64;
        let beyond_range = FileRange::new(file_len + 1000, file_len + 2000);

        let dyn_client: Arc<dyn Client> = client.clone();
        let result = retrieve_file_term_block(dyn_client, file_contents.file_hash, beyond_range).await;

        match result {
            Ok(None) => {},
            Ok(Some((_, _, file_terms))) => assert!(file_terms.is_empty()),
            Err(_) => {},
        }
    }

    #[tokio::test]
    async fn test_interleaved_xorbs() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (2, (0, 2)), (1, (2, 4)), (2, (2, 4))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_non_contiguous_chunks() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_adjacent_chunks() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (1, (3, 5))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_single_chunk_terms() {
        let (client, file_contents) =
            setup_test_file(&[(1, (0, 1)), (1, (1, 2)), (1, (2, 3)), (2, (0, 1)), (2, (1, 2))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_large_file_many_xorbs() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=10).map(|i| (i, (0, 3))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_xorb_block_deduplication() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (1, (0, 5))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_retrieval_url_acquisition() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_range = FileRange::new(0, file_contents.data.len() as u64);
        let dyn_client: Arc<dyn Client> = client.clone();

        let (_, _, file_terms) = retrieve_file_term_block(dyn_client, file_contents.file_hash, file_range)
            .await
            .unwrap()
            .unwrap();

        // Get the first file term's xorb block to test URL retrieval
        let file_term = &file_terms[0];
        let xorb_block_index = file_term.xorb_block.xorb_block_index;
        let (unique_id, url, http_ranges) = file_term.url_info.get_retrieval_url(xorb_block_index).await;

        assert!(!url.is_empty());
        assert!(!http_ranges.is_empty());
        assert!(http_ranges[0].start <= http_ranges[0].end);
        assert!(unique_id != UniqueId::null());
    }

    #[tokio::test]
    async fn test_complex_mixed_pattern() {
        let term_spec = &[
            (1, (0, 3)),
            (2, (0, 2)),
            (1, (3, 5)),
            (3, (1, 4)),
            (2, (4, 6)),
            (1, (0, 2)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_repeated_xorb_different_ranges() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (3, 5)), (1, (1, 3)), (1, (4, 6))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_single_chunk_file() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 1))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_many_small_terms_from_different_xorbs() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=20).map(|i| (i, (0, 1))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    #[tokio::test]
    async fn test_range_few_bytes_before_end() {
        // Test requesting a range that ends just a few bytes before the file end,
        // within the same chunk as the file end.
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;

        // Request range ending 3 bytes before the end
        let range = FileRange::new(0, file_len - 3);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;

        // Request range ending 1 byte before the end
        let range = FileRange::new(0, file_len - 1);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    #[tokio::test]
    async fn test_range_few_bytes_after_start() {
        // Test requesting a range that starts just a few bytes after the file start,
        // within the same chunk as the file start.
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;

        // Request range starting 3 bytes after the start
        let range = FileRange::new(3, file_len);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;

        // Request range starting 1 byte after the start
        let range = FileRange::new(1, file_len);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    #[tokio::test]
    async fn test_range_few_bytes_offset_both_ends() {
        // Test requesting a range with small offsets at both ends within the same chunk.
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;

        // Request range with 2 bytes trimmed from start and 2 bytes from end
        let range = FileRange::new(2, file_len - 2);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;

        // Request just the middle byte of a small range
        let range = FileRange::new(file_len / 2 - 1, file_len / 2 + 1);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    #[tokio::test]
    async fn test_range_single_byte_at_various_positions() {
        // Test requesting single bytes at various positions in the file.
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;

        // First byte
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(0, 1))).await;

        // Last byte
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(file_len - 1, file_len))).await;

        // Middle byte
        let mid = file_len / 2;
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(mid, mid + 1))).await;
    }

    #[tokio::test]
    async fn test_multi_term_range_ends_mid_chunk() {
        // Test with multiple terms where the requested range ends in the middle of the last term's chunk.
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 3)), (3, (0, 3))]).await;
        let file_len = file_contents.data.len() as u64;

        // End a few bytes before the file end
        let range = FileRange::new(0, file_len - 5);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    #[tokio::test]
    async fn test_multi_term_range_starts_mid_chunk() {
        // Test with multiple terms where the requested range starts in the middle of the first term's chunk.
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 3)), (3, (0, 3))]).await;
        let file_len = file_contents.data.len() as u64;

        // Start a few bytes after the file start
        let range = FileRange::new(5, file_len);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    // ==================== Multi-Disjoint Range Edge Cases ====================

    /// Single xorb with three disjoint chunk ranges.
    /// This creates one XorbBlock with chunk_ranges = [(0,2), (4,6), (8,10)].
    #[tokio::test]
    async fn test_triple_disjoint_same_xorb() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))]).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    /// Triple disjoint ranges with a partial byte range spanning the gap.
    #[tokio::test]
    async fn test_triple_disjoint_partial_range_across_gap() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        retrieve_and_verify(&client, &file_contents, Some(range)).await;
    }

    /// Two xorbs, each with two disjoint ranges, interleaved in file order.
    #[tokio::test]
    async fn test_two_xorbs_interleaved_disjoint() {
        let term_spec = &[(1, (0, 2)), (2, (0, 2)), (1, (4, 6)), (2, (4, 6))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    /// Two xorbs interleaved with disjoint ranges, partial byte range.
    #[tokio::test]
    async fn test_two_xorbs_interleaved_disjoint_partial() {
        let term_spec = &[(1, (0, 2)), (2, (0, 2)), (1, (4, 6)), (2, (4, 6))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        let file_len = file_contents.data.len() as u64;
        retrieve_and_verify(&client, &file_contents, Some(FileRange::new(file_len / 3, file_len * 2 / 3))).await;
    }

    /// Single xorb with four disjoint ranges, each a single chunk wide.
    #[tokio::test]
    async fn test_four_single_chunk_disjoint() {
        let term_spec = &[(1, (0, 1)), (1, (3, 4)), (1, (6, 7)), (1, (9, 10))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    /// Mix of contiguous and disjoint ranges from the same xorb.
    /// Chunks 0-4 are contiguous, then a gap, then chunk 8-10.
    #[tokio::test]
    async fn test_contiguous_then_disjoint() {
        let term_spec = &[(1, (0, 2)), (1, (2, 4)), (1, (8, 10))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }

    /// Three xorbs with complex disjoint access patterns.
    #[tokio::test]
    async fn test_three_xorbs_complex_disjoint() {
        let term_spec = &[
            (1, (0, 2)),
            (2, (0, 3)),
            (3, (2, 5)),
            (1, (5, 8)),
            (2, (6, 8)),
            (3, (0, 2)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        retrieve_and_verify(&client, &file_contents, None).await;
    }
}
