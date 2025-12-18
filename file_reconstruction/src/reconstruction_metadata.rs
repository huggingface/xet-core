use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::Client;
use cas_types::{ChunkRange, FileRange, HttpRange};
use merklehash::MerkleHash;
use more_asserts::*;
use rangemap::RangeMap;
use tokio::sync::RwLock;
use utils::RwTaskLock;

use crate::FileReconstructionError;
use crate::error::Result;
use crate::unique_key::{UniqueId, unique_id};

pub struct XorbBlockData {
    // The offsets into the data for each of the chunk starts, end inclusive.
    pub chunk_offsets: Vec<usize>,

    // The uncompressed size of the xorb.
    pub uncompressed_size: u64,

    // The uncompressed data for that xorb.
    pub data: Bytes,
}

// The states of a xorb term:
// 1. URL, stored as handle to block of URLs that might have to be refreshed.
// 2. Loadable from cache.
// 3.
pub struct XorbBlock {
    // Index information.
    pub xorb_hash: MerkleHash,
    pub chunk_range: ChunkRange,

    // The data source if it's been downloaded or in the process of being downloaded.
    pub data: RwTaskLock<Option<XorbBlockData>, FileReconstructionError>,
}
// The term information and possibly the pointers to the data that
pub struct FileTerm {
    // The index in the list of terms that define this file.
    pub term_index: usize,

    // The bytes in the file that this term comprises.
    pub file_byte_range: FileRange,

    // The ID of the reconstruction term block in the term manager that this refers to.
    pub term_block_id: UniqueId,

    // The index of the referencing xorb within the block.
    pub xorb_block_index: usize,

    // The chunk range within the xorb that we would pull from.
    pub xorb_chunk_range: ChunkRange,

    // The offset into the range that's given here.
    pub offset_into_first_range: u64,
}

struct ReconstructionTermBlock {
    term_index_range: Range<usize>,

    // The bytes in the file that this block covers.
    file_byte_range: FileRange,

    // The list of file terms here.  Should be length equal to the size of the range of term indices in this block.
    file_terms: Vec<Arc<FileTerm>>,

    // The list of xorb blocks here; equal to the number of unique xorb ranges in this block.
    xorb_blocks: Vec<Arc<XorbBlock>>,

    // The xorb retreival URLs.  These could be refreshed if need be.
    xorb_block_retrieval_urls: RwLock<(UniqueId, Vec<(String, HttpRange)>)>,
}

impl ReconstructionTermBlock {
    pub async fn get_file_term(&self, term_index: usize) -> Arc<FileTerm> {
        debug_assert_ge!(self.term_index_range.start, term_index);
        debug_assert_lt!(term_index, self.term_index_range.end);

        self.file_terms[term_index - self.term_index_range.start].clone()
    }

    pub async fn get_xorb_block_retrieval_url(&self, xorb_block_index: usize) -> (UniqueId, String, HttpRange) {
        let xbru = self.xorb_block_retrieval_urls.read().await;

        let (url, url_range) = xbru.1[xorb_block_index].clone();

        (xbru.0, url, url_range)
    }
}

pub struct ReconstructionTermManager {
    file_hash: MerkleHash,
    file_byte_range: FileRange,

    // Is a key to the current active blocks.
    current_blocks: HashMap<u64, RwTaskLock<Arc<ReconstructionTermBlock>, FileReconstructionError>>,

    // Mapping from a given range to a
    term_range: RangeMap<u64, u64>,
}

impl ReconstructionTermManager {
    pub fn new(client: Arc<dyn Client>, file_hash: MerkleHash, range: FileRange) -> Arc<Self> {
        todo!()
    }

    // Gets the file term most quickly associated with this.
    pub async fn get_file_term(self: &Arc<Self>, term_index: usize) -> Arc<FileTerm> {
        todo!()
    }

    pub async fn refresh_file_term_url(self: &Arc<Self>, file_term: Arc<FileTerm>) -> Arc<FileTerm> {
        // Get the refresh data stored in the block handle.
        todo!()

        // Call refresh on the inner block handle.
    }

    pub async fn mark_term_completed(self: &Arc<Self>, term_index: usize) {
        todo!()
    }

    /// Returns the amount currently prefetched.
    pub async fn current_prefetched_amount(&self) -> u64 {
        todo!();
    }

    /// Prefetches the next block of terms.  Returns true if all terms are completed.  
    pub async fn prefetch_next_term_data(self: &Arc<Self>, fetch_size: u64) {
        todo!();
    }

    /// Returns true if we've prefetched the full range given here.
    pub async fn prefetched_full_range(&self) -> bool {
        todo!();
    }
}

// The real guts of it.
impl ReconstructionTermManager {
    async fn retrieve_term_block(
        self: Arc<Self>,
        client: Arc<dyn Client>,
        term_start_index: usize,
        file_byte_range: FileRange,
        term_block_id: UniqueId,
    ) -> Result<Option<ReconstructionTermBlock>> {
        let Some(raw_reconstruction) = client.get_reconstruction(&self.file_hash, Some(file_byte_range)).await? else {
            // None means we've requested a byte range beyond the end of the file.
            return Ok(None);
        };

        let acquisition_id = unique_id();

        // Now, we need to map all the terms into the ReconstructionTermBlock.
        let mut file_terms = Vec::<FileTerm>::with_capacity(raw_reconstruction.terms.len());

        let n_xorb_terms = raw_reconstruction.fetch_info.values().map(|v| v.len()).sum();
        let mut xorb_blocks = Vec::<Arc<XorbBlock>>::with_capacity(n_xorb_terms);

        // Keep track of the URLs for each.
        let mut xorb_block_retrieval_urls = Vec::<(String, HttpRange)>::with_capacity(n_xorb_terms);

        // Get a hash map so we can reindex the xorb terms; map of (xorb_hash, first chunk index) -> xorb block index.
        let mut xorb_index_lookup = HashMap::<(MerkleHash, u64), usize>::with_capacity(n_xorb_terms);

        // Keep track of where we are so as to map the file terms to the byte range within the file.
        let mut cur_file_byte_offset = file_byte_range.start;

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
            let xorb_block_index: usize = 'find_xorb_block_index: {
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
                        let xorb_block_index = match xorb_index_lookup.entry((xorb_hash, chunk_range.start as u64)) {
                            Entry::Occupied(entry) => *entry.get(),
                            Entry::Vacant(entry) => {
                                let new_index = xorb_blocks.len();

                                xorb_blocks.push(Arc::new(XorbBlock {
                                    xorb_hash,
                                    chunk_range,
                                    data: RwTaskLock::from_value(None),
                                }));

                                // Store the retrieval URL and range for this xorb block.
                                xorb_block_retrieval_urls
                                    .push((raw_xorb_block_info.url.clone(), raw_xorb_block_info.url_range.clone()));

                                // Store the index.
                                entry.insert(new_index);
                                new_index
                            },
                        };

                        break 'find_xorb_block_index xorb_block_index;
                    }
                }
                return Err(FileReconstructionError::CorruptedReconstruction(format!(
                    "No xorb chunk range found for file term {local_term_index:?} in xorb info for xorb hash {xorb_hash:?}"
                )));
            };

            // This xorb block now has the raw data.
            let xorb_block = xorb_blocks[xorb_block_index].clone();

            // Do we need to adjust for an offset into the first range?
            let offset_into_first_range = {
                if local_term_index == 0 {
                    raw_reconstruction.offset_into_first_range
                } else {
                    0
                }
            };

            // The effective size of this term in the file..
            let term_byte_size = term.unpacked_length as u64 - offset_into_first_range;

            // Get the file term here.
            file_terms.push(FileTerm {
                term_block_id,
                term_index: local_term_index + term_start_index,
                file_byte_range: FileRange::new(cur_file_byte_offset, cur_file_byte_offset + term_byte_size),
                xorb_block_index,
                xorb_chunk_range: term.range,
                offset_into_first_range,
            });

            cur_file_byte_offset += term_byte_size;
        }

        // Now, it's possible that we have to shrink the byte rango of the last term, as we may have retrieved more
        // due to chunk offsets.
        if cur_file_byte_offset > file_byte_range.end {
            let last_term_shrinkage = cur_file_byte_offset - file_byte_range.end;

            debug_assert!(!file_terms.is_empty());

            if let Some(fi) = file_terms.last_mut() {
                fi.file_byte_range.end -= last_term_shrinkage;
            }
        }

        Ok(Some(ReconstructionTermBlock {
            term_index_range: term_start_index..(term_start_index + raw_reconstruction.terms.len()),
            file_byte_range,
            file_terms: file_terms.into_iter().map(Arc::new).collect(),
            xorb_blocks,
            xorb_block_retrieval_urls: RwLock::new((acquisition_id, xorb_block_retrieval_urls)),
        }))
    }
}
