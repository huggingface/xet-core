use std::sync::Arc;

use cas_client::{Client, URLProvider};
use cas_types::{FileRange, HttpRange};
use merklehash::MerkleHash;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};
use utils::UniqueId;

use super::file_term::retrieve_file_term_block;
use crate::FileReconstructionError;
use crate::error::Result;

/// The shared, immutable data for a reconstruction term block.
/// This is passed to FileTerm instances for URL retrieval operations.
pub struct TermBlockRetrievalURLs {
    // The hash of the file for this block.
    pub file_hash: MerkleHash,

    // The bytes in the file that this block covers. This is the actual retrieved range,
    // which may be smaller than the originally requested range if the file ends early.
    pub byte_range: FileRange,

    // The xorb retreival URLs.  These could be refreshed if need be.
    // Indexed by xorb_block_index stored in each XorbBlock.
    pub(crate) xorb_block_retrieval_urls: RwLock<(UniqueId, Vec<(String, HttpRange)>)>,
}

impl TermBlockRetrievalURLs {
    /// Create a new TermBlockRetrievalURLs instance.
    pub fn new(
        file_hash: MerkleHash,
        byte_range: FileRange,
        acquisition_id: UniqueId,
        retrieval_urls: Vec<(String, HttpRange)>,
    ) -> Self {
        Self {
            file_hash,
            byte_range,
            xorb_block_retrieval_urls: RwLock::new((acquisition_id, retrieval_urls)),
        }
    }

    /// Gets the retrieval URL for a given xorb block.  All URL requests go through
    /// this method in order to manage url refreshes; this function returns the
    /// most recent retrieval URL in the case of a refresh.
    pub async fn get_retrieval_url(&self, xorb_block_index: usize) -> (UniqueId, String, HttpRange) {
        let xbru = self.xorb_block_retrieval_urls.read().await;

        let (url, url_range) = xbru.1[xorb_block_index].clone();

        (xbru.0, url, url_range)
    }

    /// Refresh the retrieval URLs for all xorb blocks in this block.
    ///
    /// Each acquisition has a unique acquisition ID; this is used as like a single-flight
    /// to ensure that only one request actually refreshes the URLs; a refresh request is
    /// ignored if the acquisition ID of the current URLs is different from the one passed in
    /// as reference in the request; this indicates that the caller has a stale URL already and
    /// the new request will get a new URL.  
    pub async fn refresh_retrieval_urls(&self, client: Arc<dyn Client>, acquisition_id: UniqueId) -> Result<()> {
        if self.xorb_block_retrieval_urls.read().await.0 != acquisition_id {
            // This means another process has got in here while we're waiting for the lock and
            // refreshed them.
            debug!(
                file_hash = %self.file_hash,
                byte_range = ?(self.byte_range.start, self.byte_range.end),
                "URL refresh skipped - already refreshed by another request"
            );
            return Ok(());
        }

        let mut retrieval_urls = self.xorb_block_retrieval_urls.write().await;

        if retrieval_urls.0 != acquisition_id {
            // It's already been refreshed by another process.
            debug!(
                file_hash = %self.file_hash,
                byte_range = ?(self.byte_range.start, self.byte_range.end),
                "URL refresh skipped - already refreshed while waiting for lock"
            );
            return Ok(());
        }

        info!(
            file_hash = %self.file_hash,
            byte_range = ?(self.byte_range.start, self.byte_range.end),
            url_count = retrieval_urls.1.len(),
            "Refreshing expired retrieval URLs"
        );

        // Since this hopefully doesn't happen too often, go through and retrieve an
        // entire new block, then make sure everything matches up and take in the new stuff.
        let Some((returned_range, file_terms)) =
            retrieve_file_term_block(client, self.file_hash, self.byte_range).await?
        else {
            return Err(FileReconstructionError::CorruptedReconstruction(
                "On URL refresh, the returned reconstruction was None.".to_owned(),
            ));
        };

        // Check that the returned range matches what we expect.
        if returned_range != self.byte_range {
            return Err(FileReconstructionError::CorruptedReconstruction(
                "On URL refresh, the returned reconstruction range differs from expected.".to_owned(),
            ));
        }

        // Get the new URL info from the first file term (they all share the same url_info).
        let Some(first_term) = file_terms.first() else {
            return Err(FileReconstructionError::CorruptedReconstruction(
                "On URL refresh, the returned reconstruction had no terms.".to_owned(),
            ));
        };

        // It all checked out, so update the retrieval URLs in place.
        {
            let mut new_retrieval_urls = first_term.url_info.xorb_block_retrieval_urls.write().await;
            retrieval_urls.0 = new_retrieval_urls.0;
            retrieval_urls.1 = std::mem::take(&mut new_retrieval_urls.1);
        }

        info!(
            file_hash = %self.file_hash,
            byte_range = ?(self.byte_range.start, self.byte_range.end),
            "Retrieval URLs refreshed successfully"
        );

        Ok(())
    }
}

/// Provides download URLs for a xorb block, handling URL refresh on expiration.
pub struct XorbURLProvider {
    pub client: Arc<dyn Client>,
    pub url_info: Arc<TermBlockRetrievalURLs>,
    pub xorb_block_index: usize,
    pub last_acquisition_id: Mutex<UniqueId>,
}

#[async_trait::async_trait]
impl URLProvider for XorbURLProvider {
    async fn retrieve_url(
        &self,
        force_refresh: bool,
    ) -> std::result::Result<(String, HttpRange), cas_client::CasClientError> {
        if force_refresh {
            self.url_info
                .refresh_retrieval_urls(self.client.clone(), *self.last_acquisition_id.lock().await)
                .await
                .map_err(|e| cas_client::CasClientError::Other(e.to_string()))?;
        }

        let (unique_id, url, http_range) = self.url_info.get_retrieval_url(self.xorb_block_index).await;
        *self.last_acquisition_id.lock().await = unique_id;

        Ok((url, http_range))
    }
}
