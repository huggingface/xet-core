use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use cas_types::{ChunkRange, FileRange, FileRange, HttpRange};
use merklehash::MerkleHash;
use more_asserts::*;
use tokio::sync::RwLock;
use utils::RwTaskLock;

use super::single_terms::FileTerm;
use super::xorb_block::XorbBlock;
use crate::FileReconstructionError;
use crate::reconstruction_terms::term_block::ReconstructionTermBlock;
use crate::unique_key::UniqueId;

// The base info of each file term.  Data is copied from this to
// fill the FileTerm struct.
#[derive(PartialEq, Eq)]
struct FileTermRawInfo {
    byte_range: FileRange,
    xorb_chunk_range: ChunkRange,
    offset_into_first_range: u64,
    xorb_block_index: usize,
}

pub struct ReconstructionTermBlock {
    // The file hash.
    pub file_hash: MerkleHash,

    // The bytes in the file that this block covers.
    pub byte_range: FileRange,

    // The raw file terms.
    raw_file_terms: Vec<FileTermRawInfo>,

    // The xorb block info corresponding to each xorb block in this block.
    xorb_block_info: Vec<Arc<XorbBlock>>,

    // The xorb retreival URLs.  These could be refreshed if need be.
    xorb_block_retrieval_urls: RwLock<(UniqueId, Vec<(String, HttpRange)>)>,

    // The start time when we started downloading.  If zero, then unrecorded.
    start_time_ms: AtomicU64,
    // The callback function on drop to record the completion time.
}

impl ReconstructionTermBlock {
    pub fn get_file_term(self: &Arc<Self>, within_block_index: usize) -> FileTerm {
        // Let current

        let file_term_info = &self.raw_file_terms[within_block_index];

        FileTerm {
            byte_range: file_term_info.byte_range,
            xorb_chunk_range: file_term_info.xorb_chunk_range,
            offset_into_first_range: file_term_info.offset_into_first_range,
            xorb_block: self.xorb_block_info[file_term_info.xorb_block_index].clone(),
            term_block: self.clone(),
        }
    }

    pub fn n_file_terms(&self) -> usize {
        self.raw_file_terms.len()
    }

    /// Gets the retrieval URL for a given xorb block.  All URL requests go through
    /// this method in order to manage url refreshes; this function returns the
    /// most recent retrieval URL in the case of a refresh.
    pub async fn get_retrieval_url(&self, xorb_block: &Arc<XorbBlock>) -> XorbBlockRetrievalUrl {
        let xbru = self.xorb_block_retrieval_urls.read().await;

        let (url, url_range) = xbru.1[xorb_block_index].clone();

        XorbBlockRetrievalUrl {
            unique_id: xbru.0,
            url,
            http_range: url_range,
        }
    }

    pub async fn refresh_retrieval_urls(&self, client: Arc<dyn Client>, acquisition_id: UniqueId) -> Result<()> {
        if self.xorb_block_retrieval_urls.read().await.0 != acquisition_id {
            // This means another process has got in here while we're waiting for the lock and
            // refreshed them.
            return Ok(());
        }

        let mut retrieval_urls = self.xorb_block_retrieval_urls.write().await;

        if retrieval_urls.0 != acquisition_id {
            // It's already been refreshed by another process.
            return Ok(());
        }

        // Since this hopefully doesn't happen too often, go through and retrieve an
        // entire new block, then make sure everything matches up and take in the new stuff.
        let new_block = Self::retrieve_from_client(client, self.file_byte_range).await?;

        // Check that all the other fields of new_block match identically with the existing block; this
        // would be a serious bug if it happened but would cause file corruption so check it here.
        if new_block.file_terms != self.file_terms || new_block.xorb_blocks != self.xorb_blocks {
            return Err(FileReconstructionError::CorruptedReconstruction(
                "On URL refresh, the returned reconstruction differs from the previous reconstruction.".to_owned(),
            ));
        }

        // It all checked out, so update the retrieval URLs in place.
        {
            let mut new_retrieval_urls = new_block.xorb_block_retrieval_urls.write().await;
            retrieval_urls.0 = new_retrieval_urls.0;
            retrieval_urls.1 = std::mem::take(&mut new_retrieval_urls.1);
        }
    }

    /// The primary way to retrieve a reconstruction term block.
    pub async fn retrieve_from_client(client: Arc<dyn Client>, file_byte_range: FileRange) -> Result<Option<Self>> {
        // First, get the raw reconstruction.
        let Some(raw_reconstruction) = client.get_reconstruction(&self.file_hash, Some(file_byte_range)).await? else {
            // None means we've requested a byte range beyond the end of the file.
            return Ok(None);
        };

        // Set a new url acquisition id to ensure that we don't double up the url acquisitions.
        let acquisition_id = unique_id();

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
                                    xorb_block_index: new_index,
                                    data: RwLock::new(None),
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
            file_terms.push(FileTermRawInfo {
                byte_range: FileRange::new(cur_file_byte_offset, cur_file_byte_offset + term_byte_size),
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
            byte_range: file_byte_range,
            raw_file_terms: file_terms,
            xorb_block_info: xorb_blocks,
            xorb_block_retrieval_urls: RwLock::new((acquisition_id, xorb_block_retrieval_urls)),
        }))
    }
}
