use merklehash::MerkleHash;

use crate::file_structs::MDBFileInfo;

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait FileReconstructor<E> {
    /// Returns a pair of (file reconstruction information,  maybe shard ID)
    /// Err(_) if an error occured
    /// Ok(None) if the file is not found.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>, E>;
}
