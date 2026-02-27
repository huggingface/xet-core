use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::Client;
use cas_types::{
    ChunkRange, FileRange, HttpRange, QueryReconstructionResponse, QueryReconstructionResponseV2,
    ReconstructionResponse,
};
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
    pub byte_range: FileRange,
    pub xorb_chunk_range: ChunkRange,
    pub offset_into_first_range: u64,
    pub xorb_block: Arc<XorbBlock>,
    pub url_info: Arc<TermBlockRetrievalURLs>,
}

impl FileTerm {
    pub fn extract_bytes(&self, xorb_block_data: &XorbBlockData) -> Bytes {
        let local_start_chunk = (self.xorb_chunk_range.start - self.xorb_block.chunk_range.start) as usize;
        let start_byte_offset = xorb_block_data.chunk_offsets[local_start_chunk];
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
    let response = client.get_reconstruction_v2(&file_hash, Some(query_file_byte_range)).await?;

    match response {
        Some(ReconstructionResponse::V2(v2)) => retrieve_from_v2(client, file_hash, v2, query_file_byte_range).await,
        Some(ReconstructionResponse::V1(v1)) => retrieve_from_v1(file_hash, v1, query_file_byte_range),
        None => Ok(None),
    }
}

/// Process a V1 reconstruction response into file terms.
fn retrieve_from_v1(
    file_hash: MerkleHash,
    raw_reconstruction: QueryReconstructionResponse,
    query_file_byte_range: FileRange,
) -> Result<Option<(FileRange, u64, Vec<FileTerm>)>> {
    // Set a new url acquisition id to ensure that we don't double up the url acquisitions.
    let acquisition_id = UniqueId::new();

    // Intermediate storage for file term data before we create the actual FileTerm structs.
    // (byte_range, xorb_chunk_range, offset_into_first_range, index into xorb_blocks)
    let mut file_term_data = Vec::<(FileRange, ChunkRange, u64, usize)>::with_capacity(raw_reconstruction.terms.len());

    let n_xorb_terms = raw_reconstruction.fetch_info.values().map(|v| v.len()).sum();

    // Keep track of the xorb blocks we've created, keyed by (xorb_hash, first chunk index).
    let mut xorb_blocks: Vec<XorbBlock> = Vec::with_capacity(n_xorb_terms);

    // Keep track of the URLs for each.
    let mut xorb_block_retrieval_urls = Vec::<(String, HttpRange)>::with_capacity(n_xorb_terms);

    // Get a hash map so we can reindex the xorb terms; map of (xorb_hash, first chunk index) -> xorb block index.
    let mut xorb_index_lookup = HashMap::<(MerkleHash, u64), usize>::with_capacity(n_xorb_terms);

    // Keep track of where we are so as to map the file terms to the byte range within the file.
    let mut cur_file_byte_offset = query_file_byte_range.start;

    // We'll create the URL info after processing all terms, once we know the actual range.

    // Iterate over the terms and build the file terms and xorb terms.
    for (local_term_index, term) in raw_reconstruction.terms.iter().enumerate() {
        let xorb_hash: MerkleHash = term.hash.into();

        // Get the xorb info here.
        let Some(xorb_info) = raw_reconstruction.fetch_info.get(&term.hash) else {
            return Err(FileReconstructionError::CorruptedReconstruction(format!(
                "Xorb info not found for xorb hash {xorb_hash:?}"
            )));
        };

        // Get the xorb block index that this term belongs to.
        let xorb_block_index = 'find_xorb_block: {
            for raw_xorb_block_info in xorb_info.iter() {
                let chunk_range = raw_xorb_block_info.range;

                if chunk_range.start <= term.range.start && term.range.start <= chunk_range.end {
                    // Verify that the term range is contained within the xorb block.
                    if term.range.end > chunk_range.end {
                        return Err(FileReconstructionError::CorruptedReconstruction(format!(
                            "Term range extends beyond xorb block range for xorb hash {xorb_hash:?}"
                        )));
                    }

                    // Reuse the previous one if it exists, otherwise insert a new one.
                    let index = match xorb_index_lookup.entry((xorb_hash, chunk_range.start as u64)) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => {
                            let new_index = xorb_blocks.len();
                            xorb_blocks.push(XorbBlock {
                                xorb_hash,
                                chunk_range,
                                xorb_block_index: new_index,
                                references: vec![],
                                uncompressed_size_if_known: None,
                                data: RwLock::new(None),
                            });

                            // Store the retrieval URL and range for this xorb block.
                            xorb_block_retrieval_urls
                                .push((raw_xorb_block_info.url.clone(), raw_xorb_block_info.url_range));

                            // Store the index.
                            entry.insert(new_index);
                            new_index
                        },
                    };

                    break 'find_xorb_block index;
                }
            }
            return Err(FileReconstructionError::CorruptedReconstruction(format!(
                "No xorb chunk range found for file term {local_term_index:?} in xorb info for xorb hash {xorb_hash:?}"
            )));
        };

        // Do we need to adjust for an offset into the first range?
        let offset_into_first_range = {
            if local_term_index == 0 {
                raw_reconstruction.offset_into_first_range
            } else {
                0
            }
        };

        // The effective size of this term in the file.
        let term_byte_size = term.unpacked_length as u64 - offset_into_first_range;

        // Update the references term on the XorbBlock to track where the xorb gets used.
        xorb_blocks[xorb_block_index].references.push(XorbReference {
            term_chunks: term.range,
            uncompressed_size: term.unpacked_length as usize,
        });

        // Store the file term data (byte_range, xorb_chunk_range, offset_into_first_range, xorb_block_index).
        // We'll create the FileTerm structs after we know the actual range.
        file_term_data.push((
            FileRange::new(cur_file_byte_offset, cur_file_byte_offset + term_byte_size),
            term.range,
            offset_into_first_range,
            xorb_block_index,
        ));

        cur_file_byte_offset += term_byte_size;
    }

    // Sort the block references so that we can easily scan the terms to figure out how many references
    // a particular chunk may have.
    for block in &mut xorb_blocks {
        block.references.sort_by_key(|r| r.term_chunks.start);
        block.uncompressed_size_if_known = XorbBlock::determine_size_if_possible(block.chunk_range, &block.references);
    }

    // Now, it's possible that we have to shrink the byte range of the last term, as we may have retrieved more
    // due to chunk offsets.
    if cur_file_byte_offset > query_file_byte_range.end {
        let last_term_shrinkage = cur_file_byte_offset - query_file_byte_range.end;

        debug_assert!(!file_term_data.is_empty());

        if let Some(fi) = file_term_data.last_mut() {
            fi.0.end -= last_term_shrinkage;
        }
    }

    // Calculate the actual retrieved range from the file terms.
    let actual_range = FileRange::new(
        file_term_data.first().map(|(br, _, _, _)| br.start).unwrap_or(0),
        file_term_data.last().map(|(br, _, _, _)| br.end).unwrap_or(0),
    );

    // Now, calculate the total number of bytes that needs to be downloaded given dedup and compression savings.
    let total_transfer_bytes = xorb_block_retrieval_urls
        .iter()
        .map(|(_, http_range)| {
            let file_range = FileRange::from(*http_range);
            file_range.end.saturating_sub(file_range.start)
        })
        .sum();

    // Now create the URL info with the actual range and retrieval URLs.
    let url_info =
        Arc::new(TermBlockRetrievalURLs::new(file_hash, actual_range, acquisition_id, xorb_block_retrieval_urls));

    // Convert xorb_blocks to Arc<XorbBlock> for use in FileTerms.
    let xorb_blocks_arc: Vec<Arc<XorbBlock>> = xorb_blocks.into_iter().map(Arc::new).collect();

    // Convert the intermediate data to FileTerm structs with the shared url_info.
    let file_terms: Vec<FileTerm> = file_term_data
        .into_iter()
        .map(|(byte_range, xorb_chunk_range, offset_into_first_range, xorb_block_index)| FileTerm {
            byte_range,
            xorb_chunk_range,
            offset_into_first_range,
            xorb_block: xorb_blocks_arc[xorb_block_index].clone(),
            url_info: url_info.clone(),
        })
        .collect();

    Ok(Some((actual_range, total_transfer_bytes, file_terms)))
}

/// Process a V2 reconstruction response: eagerly download all xorb data via multi-range
/// requests and return pre-populated file terms.
async fn retrieve_from_v2(
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    v2: QueryReconstructionResponseV2,
    query_file_byte_range: FileRange,
) -> Result<Option<(FileRange, u64, Vec<FileTerm>)>> {
    // Build file_term_data from v2.terms (identical structure to V1 terms).
    let mut file_term_data = Vec::<(FileRange, ChunkRange, u64, usize)>::with_capacity(v2.terms.len());

    // Map from (xorb_hash, chunk_range_start) -> xorb_block_index for dedup.
    let mut xorb_index_lookup = HashMap::<(MerkleHash, ChunkRange), usize>::new();
    let mut xorb_blocks: Vec<XorbBlock> = Vec::new();

    // Track total transfer bytes.
    let mut total_transfer_bytes: u64 = 0;

    let mut cur_file_byte_offset = query_file_byte_range.start;

    // First pass: build the xorb block structure from the V2 xorb descriptors.
    // Each XorbMultiRangeFetch entry has a URL and a list of ranges.
    // Each range becomes a separate xorb block.
    for (xorb_hex_hash, descriptor) in &v2.xorbs {
        let xorb_hash: MerkleHash = (*xorb_hex_hash).into();
        for fetch_entry in &descriptor.fetch {
            for range_desc in &fetch_entry.ranges {
                let key = (xorb_hash, range_desc.chunks);
                if let Entry::Vacant(entry) = xorb_index_lookup.entry(key) {
                    let new_index = xorb_blocks.len();
                    xorb_blocks.push(XorbBlock {
                        xorb_hash,
                        chunk_range: range_desc.chunks,
                        xorb_block_index: new_index,
                        references: vec![],
                        uncompressed_size_if_known: None,
                        data: RwLock::new(None),
                    });
                    entry.insert(new_index);
                    total_transfer_bytes += range_desc.bytes.length();
                }
            }
        }
    }

    // Second pass: process terms to build file_term_data and references.
    for (local_term_index, term) in v2.terms.iter().enumerate() {
        let xorb_hash: MerkleHash = term.hash.into();

        // Find which xorb block this term belongs to.
        let xorb_block_index = xorb_index_lookup
            .iter()
            .find_map(|(&(hash, chunk_range), &idx)| {
                if hash == xorb_hash && chunk_range.start <= term.range.start && term.range.end <= chunk_range.end {
                    Some(idx)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                FileReconstructionError::CorruptedReconstruction(format!(
                    "No xorb block found for term {local_term_index} with hash {xorb_hash:?} range {:?}",
                    term.range
                ))
            })?;

        let offset_into_first_range = if local_term_index == 0 {
            v2.offset_into_first_range
        } else {
            0
        };

        let term_byte_size = term.unpacked_length as u64 - offset_into_first_range;

        xorb_blocks[xorb_block_index].references.push(XorbReference {
            term_chunks: term.range,
            uncompressed_size: term.unpacked_length as usize,
        });

        file_term_data.push((
            FileRange::new(cur_file_byte_offset, cur_file_byte_offset + term_byte_size),
            term.range,
            offset_into_first_range,
            xorb_block_index,
        ));

        cur_file_byte_offset += term_byte_size;
    }

    // Sort block references and determine sizes.
    for block in &mut xorb_blocks {
        block.references.sort_by_key(|r| r.term_chunks.start);
        block.uncompressed_size_if_known = XorbBlock::determine_size_if_possible(block.chunk_range, &block.references);
    }

    // Shrink last term if needed.
    if cur_file_byte_offset > query_file_byte_range.end {
        let last_term_shrinkage = cur_file_byte_offset - query_file_byte_range.end;
        debug_assert!(!file_term_data.is_empty());
        if let Some(fi) = file_term_data.last_mut() {
            fi.0.end -= last_term_shrinkage;
        }
    }

    let actual_range = FileRange::new(
        file_term_data.first().map(|(br, _, _, _)| br.start).unwrap_or(0),
        file_term_data.last().map(|(br, _, _, _)| br.end).unwrap_or(0),
    );

    // Eagerly download all xorb data via multi-range requests.
    for (xorb_hex_hash, descriptor) in &v2.xorbs {
        let xorb_hash: MerkleHash = (*xorb_hex_hash).into();

        for fetch_entry in &descriptor.fetch {
            // Extract the Range header value from the URL's X-Xet-Signed-Range query param.
            let range_header = extract_signed_range(&fetch_entry.url).ok_or_else(|| {
                FileReconstructionError::CorruptedReconstruction(format!(
                    "No X-Xet-Signed-Range query param found in URL for xorb {xorb_hash:?}"
                ))
            })?;

            let permit = client.acquire_download_permit().await?;

            let parts = client
                .get_multi_range_term_data(&fetch_entry.url, &range_header, fetch_entry.ranges.len(), permit, None)
                .await?;

            // Match each downloaded part to its xorb block and pre-populate.
            for (i, (data, chunk_byte_indices)) in parts.into_iter().enumerate() {
                let range_desc = &fetch_entry.ranges[i];
                let key = (xorb_hash, range_desc.chunks);

                if let Some(&block_idx) = xorb_index_lookup.get(&key) {
                    let chunk_offsets: Vec<usize> = chunk_byte_indices.iter().map(|&x| x as usize).collect();
                    let xorb_block_data = Arc::new(XorbBlockData { chunk_offsets, data });
                    *xorb_blocks[block_idx].data.write().await = Some(xorb_block_data);
                }
            }
        }
    }

    // Create a dummy TermBlockRetrievalURLs for API compatibility.
    // V2 data is already downloaded, so URL refresh should never be needed.
    let dummy_urls: Vec<(String, HttpRange)> =
        xorb_blocks.iter().map(|_| (String::new(), HttpRange::new(0, 0))).collect();
    let url_info = Arc::new(TermBlockRetrievalURLs::new(file_hash, actual_range, UniqueId::new(), dummy_urls));

    let xorb_blocks_arc: Vec<Arc<XorbBlock>> = xorb_blocks.into_iter().map(Arc::new).collect();

    let file_terms: Vec<FileTerm> = file_term_data
        .into_iter()
        .map(|(byte_range, xorb_chunk_range, offset_into_first_range, xorb_block_index)| FileTerm {
            byte_range,
            xorb_chunk_range,
            offset_into_first_range,
            xorb_block: xorb_blocks_arc[xorb_block_index].clone(),
            url_info: url_info.clone(),
        })
        .collect();

    Ok(Some((actual_range, total_transfer_bytes, file_terms)))
}

/// Extract the X-Xet-Signed-Range query parameter value from a URL.
fn extract_signed_range(url: &str) -> Option<String> {
    let query_start = url.find('?')?;
    let query = &url[query_start + 1..];
    for param in query.split('&') {
        if let Some(value) = param.strip_prefix("X-Xet-Signed-Range=") {
            // URL-decode the value (Range headers use simple chars, minimal decoding needed)
            return Some(value.replace("%20", " ").replace("%3D", "=").replace("%2C", ","));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cas_client::{ClientTestingUtils, LocalClient, RandomFileContents};
    use cas_types::{ChunkRange, FileRange};
    use more_asserts::{assert_ge, assert_le};
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

            // Verify chunk range is within xorb block boundaries.
            let xorb_block = &file_term.xorb_block;
            assert_ge!(file_term.xorb_chunk_range.start, xorb_block.chunk_range.start);
            assert_le!(file_term.xorb_chunk_range.end, xorb_block.chunk_range.end);

            // Cross-reference with known file contents.
            if expected_term_idx < file_contents.terms.len() {
                let expected_term = &file_contents.terms[expected_term_idx];

                // Verify xorb hash matches.
                assert_eq!(xorb_block.xorb_hash, expected_term.xorb_hash);

                // Verify chunk range matches (accounting for partial first term).
                if file_term_data_offset == 0 {
                    assert_eq!(file_term.xorb_chunk_range.start as u32, expected_term.chunk_start);
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
        let (unique_id, url, http_range) = file_term.url_info.get_retrieval_url(xorb_block_index).await;

        assert!(!url.is_empty());
        assert!(http_range.start < http_range.end);
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
}
