use std::sync::Arc;

use bytes::Bytes;
use cas_types::{ChunkRange, FileRange, HttpRange};
use merklehash::MerkleHash;
use tokio::sync::RwLock;

use crate::reconstruction_terms::term_block::ReconstructionTermBlock;
use crate::unique_key::UniqueId;

pub struct XorbBlockData {
    // The offsets into the data for each of the chunk starts, end inclusive.
    pub chunk_offsets: Vec<usize>,

    // The uncompressed size of the xorb.
    pub uncompressed_size: u64,

    // The uncompressed data for that xorb.
    pub data: Bytes,
}

pub struct XorbBlockRetrievalUrl {
    pub unique_id: UniqueId,
    pub url: String,
    pub http_range: HttpRange,
}

// The states of a xorb term:
// 1. URL, stored as handle to block of URLs that might have to be refreshed.
// 2. Loadable from cache.
pub struct XorbBlock {
    pub xorb_hash: MerkleHash,
    pub chunk_range: ChunkRange,

    // Information needed to acquire the url.
    pub xorb_block_index: usize,

    // The data source if it's been downloaded.
    pub data: RwLock<Option<Arc<XorbBlockData>>>,
}

// Implement this for checking and verification purposes.
impl PartialEq for XorbBlock {
    fn eq(&self, other: &Self) -> bool {
        self.xorb_hash == other.xorb_hash
            && self.chunk_range == other.chunk_range
            && xorb_block_index == other.xorb_block_index
    }
}

impl Eq for XorbBlock {}

impl XorbBlock {
    /// Retrieve the data from the client for this xorb block.
    pub async fn retrieve_data(&self, client: Arc<dyn Client>) -> Result<Arc<XorbBlockData>> {
        // If the data has already been retrieved, return it.
        if let Some(xorb_block_data) = self.data.read().await {
            return Ok(xorb_block_data.clone());
        }

        // Okay, now it's not there, so let's retrieve it.
        let mut xbd_lg = self.data.write().await;

        // Acquire the download permit to retrieve the data.
        let permit = client.acquire_download_permit().await?;
    }
}

// The term information and possibly the pointers to the data that
#[derive(Clone)]
pub struct FileTerm {
    // The byte range of the term in the destination file.
    pub byte_range: FileRange,

    // The file block that this references.  When this instance
    pub term_block: Arc<ReconstructionTermBlock>,

    // The location within the xorb block that this term references.
    pub xorb_chunk_range: ChunkRange,
    pub offset_into_first_range: u64,

    // The xorb block that contains the data for this term.
    pub xorb_block: Arc<XorbBlock>,
}

impl FileTerm {
    pub async fn xorb_block_retrieval_url(&self) -> (UniqueId, String, HttpRange) {
        self.term_block
            .get_xorb_block_retrieval_url(self.data_info.xorb_block_index)
            .await
    }
}
