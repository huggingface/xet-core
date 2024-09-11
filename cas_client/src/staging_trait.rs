use crate::error::CasClientError;
use crate::interface::Client;
use async_trait::async_trait;
use merklehash::MerkleHash;
use std::path::PathBuf;

#[async_trait]
pub trait StagingUpload {
    async fn upload_all_staged(
        &self,
        max_concurrent: usize,
        retain: bool,
    ) -> Result<(), CasClientError>;
}

#[async_trait]
pub trait StagingBypassable {
    async fn put_bypass_stage(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError>;
}

#[async_trait]
pub trait StagingInspect {
    /// Returns a vector of the XORBs in staging
    async fn list_all_staged(&self) -> Result<Vec<String>, CasClientError>;

    /// Gets the length of the XORB. This is the same as the
    /// get_length method on the Client trait, but it forces the check to
    /// come from staging only.
    async fn get_length_staged(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError>;

    /// Gets the length of the XORB. This is the same as the
    /// get_length method on the Client trait, but this forces the check to
    /// come from the remote CAS server.
    async fn get_length_remote(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError>;

    /// Gets the path to the staging directory.
    fn get_staging_path(&self) -> PathBuf;

    /// Gets the sum of the file sizes of the valid XORBS in staging.
    fn get_staging_size(&self) -> Result<usize, CasClientError>;
}

#[async_trait]
pub trait Staging: StagingUpload + StagingInspect + Client + StagingBypassable {}
