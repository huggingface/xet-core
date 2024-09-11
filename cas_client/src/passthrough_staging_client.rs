use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use async_trait::async_trait;

use merklehash::MerkleHash;

use crate::error::{CasClientError, Result};
use crate::interface::Client;
use crate::staging_trait::*;

const PASSTHROUGH_STAGING_MAX_CONCURRENT_UPLOADS: usize = 16;

type FutureCollectionType = FuturesUnordered<Pin<Box<dyn Future<Output = Result<()>> + Send>>>;

/// The PassthroughStagingClient is a simple wrapper around
/// a Client that provides the trait implementations required for StagingClient
/// All staging operations are no-op.
#[derive(Debug)]
pub struct PassthroughStagingClient {
    client: Arc<dyn Client + Sync + Send>,
    put_futures: Mutex<FutureCollectionType>,
}

impl PassthroughStagingClient {
    /// Create a new passthrough staging client which wraps any other client.
    /// All operations are simply passthrough to the internal client.
    /// All staging operations are no-op.
    pub fn new(client: Arc<dyn Client + Sync + Send>) -> PassthroughStagingClient {
        PassthroughStagingClient {
            client,
            put_futures: Mutex::new(FutureCollectionType::new()),
        }
    }
}

impl Staging for PassthroughStagingClient {}

#[async_trait]
impl StagingUpload for PassthroughStagingClient {
    /// Upload all staged will upload everything to the remote client.
    /// TODO : Caller may need to be wary of a HashMismatch error which will
    /// indicate that the local staging environment has been corrupted somehow.
    async fn upload_all_staged(&self, _max_concurrent: usize, _retain: bool) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl StagingBypassable for PassthroughStagingClient {
    async fn put_bypass_stage(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        self.client.put(prefix, hash, data, chunk_boundaries).await
    }
}

#[async_trait]
impl StagingInspect for PassthroughStagingClient {
    async fn list_all_staged(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn get_length_staged(&self, _prefix: &str, hash: &MerkleHash) -> Result<usize> {
        Ok(Err(CasClientError::XORBNotFound(*hash))?)
    }

    async fn get_length_remote(&self, prefix: &str, hash: &MerkleHash) -> Result<usize> {
        let item = self.client.get_length(prefix, hash).await?;

        Ok(item as usize)
    }

    fn get_staging_path(&self) -> PathBuf {
        PathBuf::default()
    }

    fn get_staging_size(&self) -> Result<usize> {
        Ok(0)
    }
}

#[async_trait]
impl Client for PassthroughStagingClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        let prefix = prefix.to_string();
        let hash = *hash;
        let client = self.client.clone();
        let mut put_futures = self.put_futures.lock().await;
        while put_futures.len() >= PASSTHROUGH_STAGING_MAX_CONCURRENT_UPLOADS {
            if let Some(Err(e)) = put_futures.next().await {
                info!("Error occurred with a background CAS upload.");
                // a background upload failed. we returning that error here.
                return Err(e);
            }
        }
        put_futures.push(Box::pin(async move {
            client.put(&prefix, &hash, data, chunk_boundaries).await
        }));
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        let mut put_futures = self.put_futures.lock().await;
        while put_futures.len() > 0 {
            if let Some(Err(e)) = put_futures.next().await {
                info!("Error occurred with a background CAS upload.");
                // a background upload failed. we returning that error here.
                return Err(e);
            }
        }
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        self.client.get(prefix, hash).await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        self.client.get_object_range(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        self.client.get_length(prefix, hash).await
    }
}
