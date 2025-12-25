use std::sync::Arc;

use bytes::Bytes;
use cas_client::Client;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_types::ChunkRange;
use merklehash::MerkleHash;
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

/// A downloadable xorb block identified by hash and chunk range, with cached data.
/// Multiple file terms may reference the same xorb block.
pub struct XorbBlock {
    pub xorb_hash: MerkleHash,
    pub chunk_range: ChunkRange,
    pub xorb_block_index: usize,
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

        let (data, chunk_byte_offsets) = client.get_file_term_data(Box::new(url_provider), permit).await?;

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
}
