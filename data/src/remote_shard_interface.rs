use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas_client::ShardClientInterface;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::error::MDBShardError;
use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::shard_file_manager::ShardFileManager;
use mdb_shard::MDBShardFile;
use merklehash::MerkleHash;
use parutils::tokio_par_for_each;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use xet_threadpool::ThreadPool;

use super::configurations::{FileQueryPolicy, StorageConfig};
use super::errors::{DataProcessingError, Result};
use super::shard_interface::{create_shard_client, create_shard_manager};
use crate::cas_interface::Client;
use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::repo_salt::RepoSalt;

pub struct RemoteShardInterface {
    pub file_query_policy: FileQueryPolicy,
    pub shard_prefix: String,
    pub shard_cache_directory: Option<PathBuf>,
    pub shard_session_directory: Option<PathBuf>,

    pub repo_salt: Option<RepoSalt>,

    pub cas: Option<Arc<dyn Client + Send + Sync>>,
    pub shard_manager: Option<Arc<ShardFileManager>>,
    pub shard_client: Option<Arc<dyn ShardClientInterface>>,
    pub threadpool: Arc<ThreadPool>,
}

impl RemoteShardInterface {
    /// Set up a lightweight version of this that can only use operations that query the remote server;
    /// anything that tries to download or upload shards will cause a runtime error.
    pub async fn new_query_only(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
        threadpool: Arc<ThreadPool>,
    ) -> Result<Arc<Self>> {
        Self::new(file_query_policy, shard_storage_config, None, None, None, threadpool, true).await
    }

    pub async fn new(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
        shard_manager: Option<Arc<ShardFileManager>>,
        cas: Option<Arc<dyn Client + Send + Sync>>,
        repo_salt: Option<RepoSalt>,
        threadpool: Arc<ThreadPool>,
        download_only: bool,
    ) -> Result<Arc<Self>> {
        let shard_client = {
            if file_query_policy != FileQueryPolicy::LocalOnly {
                debug!("data_processing: Setting up file reconstructor to query shard server.");
                create_shard_client(shard_storage_config).await.ok()
            } else {
                None
            }
        };

        let shard_manager = if file_query_policy != FileQueryPolicy::ServerOnly && shard_manager.is_none() {
            Some(create_shard_manager(shard_storage_config, download_only).await?)
        } else {
            shard_manager
        };

        Ok(Arc::new(Self {
            file_query_policy,
            shard_prefix: shard_storage_config.prefix.clone(),
            shard_cache_directory: shard_storage_config.cache_config.as_ref().map(|cf| cf.cache_directory.clone()),
            shard_session_directory: shard_storage_config.staging_directory.clone(),
            repo_salt,
            shard_manager,
            shard_client,
            cas,
            threadpool,
        }))
    }

    fn shard_client(&self) -> Result<Arc<dyn ShardClientInterface>> {
        let Some(shard_client) = self.shard_client.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::FileQueryPolicyError(format!(
                "tried to contact Shard service but FileQueryPolicy was set to {:?}",
                self.file_query_policy
            )));
        };

        Ok(shard_client)
    }

    fn shard_manager(&self) -> Result<Arc<ShardFileManager>> {
        let Some(shard_manager) = self.shard_manager.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::FileQueryPolicyError(format!(
                "tried to use local Shards but FileQueryPolicy was set to {:?}",
                self.file_query_policy
            )));
        };

        Ok(shard_manager)
    }

    fn repo_salt(&self) -> Result<RepoSalt> {
        // repo salt is optional for dedup
        Ok(self.repo_salt.unwrap_or_default())
    }

    fn shard_cache_directory(&self) -> Result<PathBuf> {
        let Some(cache_dir) = self.shard_cache_directory.clone() else {
            return Err(DataProcessingError::ShardConfigError("cache directory not configured".to_owned()));
        };

        Ok(cache_dir)
    }

    fn shard_session_directory(&self) -> Result<PathBuf> {
        let Some(session_dir) = self.shard_session_directory.clone() else {
            return Err(DataProcessingError::ShardConfigError("staging directory not configured".to_owned()));
        };

        Ok(session_dir)
    }

    /// Probes which shards provides dedup information for a chunk.
    /// Returns a list of shard hashes with key under 'prefix',
    /// Err(_) if an error occured.
    pub async fn query_dedup_shard_by_chunk(
        &self,
        chunk_hash: &MerkleHash,
        salt: &RepoSalt,
    ) -> Result<Option<PathBuf>> {
        if let Some(shard_client) = self.shard_client.as_ref() {
            debug!("get_dedup_shards: querying for shards with chunk {chunk_hash:?}");
            Ok(shard_client
                .query_for_global_dedup_shard(&self.shard_prefix, chunk_hash, salt)
                .await?)
        } else {
            Ok(None)
        }
    }

    pub fn merge_shards(&self) -> Result<JoinHandle<std::result::Result<Vec<Arc<MDBShardFile>>, MDBShardError>>> {
        let session_dir = self.shard_session_directory()?;

        let merged_shards_jh = self
            .threadpool
            .spawn(async move { consolidate_shards_in_directory(&session_dir, MDB_SHARD_MIN_TARGET_SIZE) });

        Ok(merged_shards_jh)
    }

    pub async fn upload_and_register_shards(&self, shards: Vec<Arc<MDBShardFile>>) -> Result<()> {
        if shards.is_empty() {
            return Ok(());
        }

        let salt = self.repo_salt()?;
        let shard_client = self.shard_client()?;
        let shard_client_ref = &shard_client;
        let shard_prefix = self.shard_prefix.clone();
        let shard_prefix_ref = &shard_prefix;

        tokio_par_for_each(shards, *MAX_CONCURRENT_XORB_UPLOADS, |si, _| async move {
            // For each shard:
            // 1. Upload directly to CAS.
            // 2. Sync to server.

            debug!("Uploading shard {shard_prefix_ref}/{:?} from staging area to CAS.", &si.shard_hash);
            let data = std::fs::read(&si.path)?;

            // Upload the shard.
            shard_client_ref
                .upload_shard(&self.shard_prefix, &si.shard_hash, false, &data, &salt)
                .await?;

            info!("Shard {shard_prefix_ref}/{:?} upload + sync completed successfully.", &si.shard_hash);

            Ok(())
        })
        .await
        .map_err(|e| match e {
            parutils::ParallelError::JoinError => DataProcessingError::InternalError("Join Error".into()),
            parutils::ParallelError::TaskError(e) => e,
        })?;

        Ok(())
    }

    pub async fn move_session_shards_to_local_cache(&self) -> Result<()> {
        let cache_dir = self.shard_cache_directory()?;
        let session_dir = self.shard_session_directory()?;

        let dir_walker = std::fs::read_dir(session_dir)?;

        for file in dir_walker.flatten() {
            let file_type = file.file_type()?;
            let file_path = file.path();
            if !file_type.is_file() || !is_shard_file(&file_path) {
                continue;
            }
            let dest_shard_name = cache_dir.join(file_path.file_name().unwrap());

            std::fs::rename(&file_path, &dest_shard_name)?;

            // Register this in any existing shard manager
            ShardFileManager::register_shard_in_existing_managers(&dest_shard_name).await?;
        }

        Ok(())
    }
}

/// Construct a file name for a MDBShard stored under cache and session dir.
fn local_shard_name(hash: &MerkleHash) -> PathBuf {
    PathBuf::from(hash.to_string()).with_extension("mdb")
}

/// Quickly validate the shard extension
fn is_shard_file(path: &Path) -> bool {
    path.extension().and_then(OsStr::to_str) == Some("mdb")
}
