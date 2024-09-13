use async_trait::async_trait;
use merklehash::MerkleHash;

use crate::file_structs::MDBFileInfo;

#[async_trait]
pub trait FileReconstructor<E> {
    /// Returns a pair of (file reconstruction information,  maybe shard ID)
    /// Err(_) if an error occured
    /// Ok(None) if the file is not found.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>, E>;
}
