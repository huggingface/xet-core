use std::result::Result as stdResult;
use std::sync::Arc;

use async_trait::async_trait;
use cas_client::{CasClientError, Client};
use cas_object::SerializedCasObject;
use tokio::sync::Semaphore;
use tokio_with_wasm::alias as wasmtokio;

use crate::errors::*;
use crate::wasm_timer::ConsoleTimer;

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait XorbUploader {
    async fn upload_xorb(&mut self, input: SerializedCasObject) -> Result<()>;
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
    async fn upload_xorb(&mut self, input: SerializedCasObject) -> Result<()> {
        let _ = self.client.upload_xorb(&self.cas_prefix, input, None).await?;
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
    tasks: wasmtokio::task::JoinSet<stdResult<u64, CasClientError>>,
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
    async fn upload_xorb(&mut self, input: SerializedCasObject) -> Result<()> {
        while let Some(ret) = self.tasks.try_join_next() {
            ret.map_err(DataProcessingError::internal)??;
        }

        let client = self.client.clone();
        let cas_prefix = self.cas_prefix.clone();
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(DataProcessingError::internal)?;
        self.tasks.spawn(async move {
            let _timer = ConsoleTimer::new(format!("upload xorb {}", input.hash));
            let ret = client.upload_xorb(&cas_prefix, input, None).await;
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
