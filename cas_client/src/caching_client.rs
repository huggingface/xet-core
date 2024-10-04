#![allow(unused_variables)]
use crate::error::Result;
use crate::interface::*;
use async_trait::async_trait;
use cas_types::QueryReconstructionResponse;
use merklehash::MerkleHash;
use std::io::Write;
use std::path::Path;

#[derive(Debug)]
#[allow(private_bounds)]
pub struct CachingClient<T: Client + Reconstructable + Send + Sync> {
    client: T,
}

#[async_trait]
impl<T: Client + Reconstructable + Send + Sync> UploadClient for CachingClient<T> {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<()> {
        todo!()
    }

    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool> {
        todo!()
    }

    async fn flush(&self) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<T: Client + Reconstructable + Send + Sync> ReconstructionClient for CachingClient<T> {
    async fn get_file(&self, hash: &MerkleHash, writer: &mut Box<dyn Write + Send>) -> Result<()> {
        /*
        let file_info = self.reconstruct(hash, None).await?;

        for entry in file_info.reconstruction {
            if let Some(bytes) = self.cache.get(entry.hash, entry.range) {
                // write out
            } else {
                let bytes = crate::get_one_range(&entry).await?;
                // put into cache
                // write out
            }
        }
        */

        todo!()
    }

    async fn get_file_byte_range(
        &self,
        hash: &MerkleHash,
        offset: u64,
        length: u64,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<T: Client + Reconstructable + Send + Sync> Reconstructable for CachingClient<T> {
    async fn reconstruct(
        &self,
        hash: &MerkleHash,
        byte_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse> {
        self.reconstruct(hash, byte_range).await
    }
}

impl<T: Client + Reconstructable + Send + Sync> Client for CachingClient<T> {}

#[allow(private_bounds)]
impl<T: Client + Reconstructable + Send + Sync> CachingClient<T> {
    pub fn new(client: T, cache_directory: &Path, cache_size: u64) -> Self {
        Self { client }
    }
}
