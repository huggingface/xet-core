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
    // Each entry is (url, http_ranges) to support multi-range V2 blocks.
    #[allow(clippy::type_complexity)]
    pub(crate) xorb_block_retrieval_urls: RwLock<(UniqueId, Vec<(String, Vec<HttpRange>)>)>,
}

impl TermBlockRetrievalURLs {
    /// Create a new TermBlockRetrievalURLs instance.
    pub fn new(
        file_hash: MerkleHash,
        byte_range: FileRange,
        acquisition_id: UniqueId,
        retrieval_urls: Vec<(String, Vec<HttpRange>)>,
    ) -> Self {
        Self {
            file_hash,
            byte_range,
            xorb_block_retrieval_urls: RwLock::new((acquisition_id, retrieval_urls)),
        }
    }

    /// Gets the retrieval URL and all byte ranges for a given xorb block.
    /// All URL requests go through this method in order to manage URL refreshes;
    /// this function returns the most recent retrieval URL in the case of a refresh.
    pub async fn get_retrieval_url(&self, xorb_block_index: usize) -> (UniqueId, String, Vec<HttpRange>) {
        let xbru = self.xorb_block_retrieval_urls.read().await;
        let (url, url_ranges) = &xbru.1[xorb_block_index];
        (xbru.0, url.clone(), url_ranges.clone())
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
            // Another task already refreshed while we were waiting for the read lock.
            debug!(
                file_hash = %self.file_hash,
                byte_range = ?(self.byte_range.start, self.byte_range.end),
                "URL refresh skipped - already refreshed by another request"
            );
            return Ok(());
        }

        let mut retrieval_urls = self.xorb_block_retrieval_urls.write().await;

        if retrieval_urls.0 != acquisition_id {
            // Already refreshed by another task while waiting for the write lock.
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

        // Re-fetch the entire block to get fresh URLs, then verify the structure matches.
        let Some((returned_range, _transfer_bytes, file_terms)) =
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
    async fn retrieve_url(&self) -> std::result::Result<(String, Vec<HttpRange>), cas_client::CasClientError> {
        let (unique_id, url, http_ranges) = self.url_info.get_retrieval_url(self.xorb_block_index).await;
        *self.last_acquisition_id.lock().await = unique_id;

        Ok((url, http_ranges))
    }

    async fn refresh_url(&self) -> std::result::Result<(), cas_client::CasClientError> {
        self.url_info
            .refresh_retrieval_urls(self.client.clone(), *self.last_acquisition_id.lock().await)
            .await
            .map_err(|e| cas_client::CasClientError::Other(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cas_client::{ClientTestingUtils, LocalClient, URLProvider};
    use cas_types::{FileRange, HttpRange};
    use merklehash::MerkleHash;
    use tokio::sync::Mutex;
    use utils::UniqueId;

    use super::{TermBlockRetrievalURLs, XorbURLProvider};

    fn sample_urls(n: usize) -> Vec<(String, Vec<HttpRange>)> {
        (0..n)
            .map(|i| (format!("https://example.com/xorb_{i}"), vec![HttpRange::new(0, 100)]))
            .collect()
    }

    #[tokio::test]
    async fn test_new_and_get_retrieval_url() {
        let id = UniqueId::new();
        let urls = sample_urls(3);
        let block = TermBlockRetrievalURLs::new(MerkleHash::default(), FileRange::new(0, 100), id, urls.clone());

        for (i, expected) in urls.iter().enumerate() {
            let (ret_id, url, ranges) = block.get_retrieval_url(i).await;
            assert!(ret_id == id, "acquisition ID mismatch for block {i}");
            assert_eq!(url, expected.0);
            assert_eq!(ranges, expected.1);
        }
    }

    #[tokio::test]
    async fn test_refresh_skipped_when_already_refreshed() {
        let (client, file_contents) = {
            let c = LocalClient::temporary().await.unwrap();
            let fc = c.upload_random_file(&[(1, (0, 3))], 64).await.unwrap();
            (c, fc)
        };

        let file_range = FileRange::new(0, file_contents.data.len() as u64);
        let dyn_client: Arc<dyn cas_client::Client> = client.clone();

        let (_, _, file_terms) =
            super::retrieve_file_term_block(dyn_client.clone(), file_contents.file_hash, file_range)
                .await
                .unwrap()
                .unwrap();

        let url_info = file_terms[0].url_info.clone();

        // Get original acquisition ID
        let (original_id, _, _) = url_info.get_retrieval_url(0).await;

        // Refresh with a stale (different) ID should be a no-op.
        let stale_id = UniqueId::new();
        url_info.refresh_retrieval_urls(dyn_client.clone(), stale_id).await.unwrap();
        let (id_after, _, _) = url_info.get_retrieval_url(0).await;
        assert!(id_after == original_id, "refresh with stale ID should not change acquisition ID");

        // Refresh with the correct ID should update URLs.
        url_info.refresh_retrieval_urls(dyn_client.clone(), original_id).await.unwrap();
        let (refreshed_id, _, _) = url_info.get_retrieval_url(0).await;
        assert!(refreshed_id != original_id, "refresh with correct ID should change acquisition ID");
    }

    #[tokio::test]
    async fn test_xorb_url_provider_retrieve_and_refresh() {
        let (client, file_contents) = {
            let c = LocalClient::temporary().await.unwrap();
            let fc = c.upload_random_file(&[(1, (0, 3))], 64).await.unwrap();
            (c, fc)
        };

        let file_range = FileRange::new(0, file_contents.data.len() as u64);
        let dyn_client: Arc<dyn cas_client::Client> = client.clone();

        let (_, _, file_terms) =
            super::retrieve_file_term_block(dyn_client.clone(), file_contents.file_hash, file_range)
                .await
                .unwrap()
                .unwrap();

        let url_info = file_terms[0].url_info.clone();

        let provider = XorbURLProvider {
            client: dyn_client.clone(),
            url_info,
            xorb_block_index: 0,
            last_acquisition_id: Mutex::new(UniqueId::null()),
        };

        // retrieve_url should succeed and return a valid URL.
        let (url, ranges) = provider.retrieve_url().await.unwrap();
        assert!(!url.is_empty());
        assert!(!ranges.is_empty());

        // refresh_url should succeed (refreshes with the current acquisition ID).
        provider.refresh_url().await.unwrap();

        // After refresh, retrieve_url should still work with updated URLs.
        let (url2, ranges2) = provider.retrieve_url().await.unwrap();
        assert!(!url2.is_empty());
        assert!(!ranges2.is_empty());
    }
}
