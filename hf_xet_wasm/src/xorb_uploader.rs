// #![cfg(target_arch = "wasm32")]

use std::result::Result as stdResult;
use std::sync::Arc;

use async_trait::async_trait;
use cas_client::{CasClientError, Client};
use merklehash::MerkleHash;
use tokio::sync::Semaphore;
use tokio_with_wasm::alias as wasmtokio;

use crate::errors::*;

type XorbUploadType = (MerkleHash, Vec<u8>, Vec<(MerkleHash, u32)>);

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait XorbUploader {
    async fn upload_xorb(&mut self, input: XorbUploadType) -> Result<()>;
    async fn finalize(&mut self) -> Result<()>;
}

pub struct XorbUploaderLocalSequential {
    client: Arc<dyn Client + Send + Sync>,
    cas_prefix: String,
}

impl XorbUploaderLocalSequential {
    pub fn new(client: Arc<dyn Client + Send + Sync>, cas_prefix: &str, _upload_concurrency: usize) -> Self {
        Self {
            client,
            cas_prefix: cas_prefix.to_owned(),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl XorbUploader for XorbUploaderLocalSequential {
    async fn upload_xorb(&mut self, input: XorbUploadType) -> Result<()> {
        let _ = self.client.put(&self.cas_prefix, &input.0, input.1, input.2).await?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct XorbUploaderSpawnParallel {
    client: Arc<dyn Client + Send + Sync>,
    cas_prefix: String,
    semaphore: Arc<Semaphore>,
    tasks: wasmtokio::task::JoinSet<stdResult<usize, CasClientError>>,
}

impl XorbUploaderSpawnParallel {
    pub fn new(client: Arc<dyn Client + Send + Sync>, cas_prefix: &str, upload_concurrency: usize) -> Self {
        Self {
            client,
            cas_prefix: cas_prefix.to_owned(),
            semaphore: Arc::new(Semaphore::new(upload_concurrency)),
            tasks: wasmtokio::task::JoinSet::new(),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl XorbUploader for XorbUploaderSpawnParallel {
    async fn upload_xorb(&mut self, input: XorbUploadType) -> Result<()> {
        while let Some(ret) = self.tasks.try_join_next() {
            ret.map_err(DataProcessingError::internal)??;
        }

        let client = self.client.clone();
        let cas_prefix = self.cas_prefix.clone();
        let permit = self.semaphore.clone().acquire_owned();
        self.tasks.spawn(async move {
            let ret = client.put(&cas_prefix, &input.0, input.1, input.2).await;
            drop(permit);
            ret
        });

        Ok(())
    }

    async fn finalize(&mut self) -> Result<()> {
        while let Some(ret) = self.tasks.join_next().await {
            ret.map_err(DataProcessingError::internal)??;
        }

        Ok(())
    }
}
