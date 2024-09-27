use crate::error::Result;
use crate::interface::*;
use async_trait::async_trait;
use merklehash::MerkleHash;
use std::io::Write;

#[derive(Debug)]
pub struct CachingClient {}

#[async_trait]
impl UploadClient for CachingClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
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
impl ReconstructionClient for CachingClient {
    async fn get_file(&self, hash: &MerkleHash, writer: &mut Box<dyn Write + Send>) -> Result<()> {
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
