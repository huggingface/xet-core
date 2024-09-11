use async_trait::async_trait;
use cas_client::{Client, LocalClient};
use itertools::Itertools;
use mdb_shard::error::MDBShardError;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::{shard_file_reconstructor::FileReconstructor, ShardFileManager};
use mdb_shard::{MDBShardFile, MDBShardInfo};
use merkledb::aggregate_hashes::with_salt;
use merklehash::MerkleHash;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use crate::error::ShardClientError;
use crate::{
    error::Result, global_dedup_table::DiskBasedGlobalDedupTable, RegistrationClient,
    ShardClientInterface,
};

/// This creates a persistent local shard client that simulates the shard server.  It
/// Is intended to use for testing interactions between local repos that would normally
/// require the use of the remote shard server.  
pub struct LocalShardClient {
    shard_manager: ShardFileManager,
    cas: LocalClient,
    shard_directory: PathBuf,
    global_dedup: DiskBasedGlobalDedupTable,
}

impl LocalShardClient {
    pub async fn new(cas_directory: &Path) -> Result<Self> {
        let shard_directory = cas_directory.join("shards");
        if !shard_directory.exists() {
            std::fs::create_dir_all(&shard_directory).map_err(|e| {
                ShardClientError::Other(format!(
                    "Error creating local shard directory {shard_directory:?}: {e:?}."
                ))
            })?;
        }

        let shard_manager = ShardFileManager::new(&shard_directory).await?;
        shard_manager
            .register_shards_by_path(&[&shard_directory], true)
            .await?;

        let cas = LocalClient::new(cas_directory, false);

        let global_dedup = DiskBasedGlobalDedupTable::open_or_create(
            cas_directory.join("ddb").join("chunk2shard.db"),
        )?;

        Ok(LocalShardClient {
            shard_manager,
            cas,
            shard_directory,
            global_dedup,
        })
    }
}

#[async_trait]
impl FileReconstructor for LocalShardClient {
    /// Query the shard server for the file reconstruction info.
    /// Returns the FileInfo for reconstructing the file and the shard ID that
    /// defines the file info.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> mdb_shard::error::Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.shard_manager
            .get_file_reconstruction_info(file_hash)
            .await
    }
}

#[async_trait]
impl RegistrationClient for LocalShardClient {
    async fn register_shard_v1(&self, prefix: &str, hash: &MerkleHash, _force: bool) -> Result<()> {
        // Dump the shard from the CAS to the shard directory.  Go through the local client to unpack this.

        let shard_data = self.cas.get(prefix, hash).await.map_err(|e| {
            ShardClientError::Other(format!(
                "Error retrieving shard content from cas for local registration: {e:?}."
            ))
        })?;

        let shard = MDBShardFile::write_out_from_reader(
            &self.shard_directory,
            &mut Cursor::new(shard_data),
        )?;

        self.shard_manager.register_shards(&[shard], true).await?;

        Ok(())
    }

    async fn register_shard_with_salt(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()> {
        self.register_shard_v1(prefix, hash, force).await?;

        let Some(shard) = self.shard_manager.get_shard_handle(hash, false).await else {
            return Err(MDBShardError::ShardNotFound(*hash).into());
        };

        let mut shard_reader = shard.get_reader()?;

        let chunk_hashes = MDBShardInfo::read_cas_chunks_for_global_dedup(&mut shard_reader)?;

        self.global_dedup
            .batch_add(&chunk_hashes, hash, prefix, salt)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl ShardDedupProber for LocalShardClient {
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> mdb_shard::error::Result<Vec<MerkleHash>> {
        let salted_chunk_hash = chunk_hash
            .iter()
            .filter_map(|chunk| with_salt(chunk, salt).ok())
            .collect_vec();
        Ok(self.global_dedup.query(&salted_chunk_hash, prefix).await)
    }
}

impl ShardClientInterface for LocalShardClient {}
