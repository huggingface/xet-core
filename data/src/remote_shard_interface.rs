use super::configurations::{FileQueryPolicy, StorageConfig};
use super::errors::{DataProcessingError, Result};
use super::shard_interface::{create_shard_client, create_shard_manager};
use crate::cas_interface::Client;
use crate::constants::{FILE_RECONSTRUCTION_CACHE_SIZE, MAX_CONCURRENT_UPLOADS};
use crate::repo_salt::RepoSalt;
use lru::LruCache;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::{
    error::MDBShardError, file_structs::MDBFileInfo, shard_file_manager::ShardFileManager,
    shard_file_reconstructor::FileReconstructor, MDBShardFile,
};
use merklehash::MerkleHash;
use parutils::tokio_par_for_each;
use shard_client::ShardClientInterface;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub struct RemoteShardInterface {
    pub file_query_policy: FileQueryPolicy,
    pub shard_prefix: String,
    pub shard_cache_directory: Option<PathBuf>,
    pub shard_session_directory: Option<PathBuf>,

    pub repo_salt: Option<RepoSalt>,

    pub cas: Option<Arc<dyn Client + Send + Sync>>,
    pub shard_manager: Option<Arc<ShardFileManager>>,
    pub shard_client: Option<Arc<dyn ShardClientInterface>>,
    pub reconstruction_cache:
        Mutex<LruCache<merklehash::MerkleHash, (MDBFileInfo, Option<MerkleHash>)>>,
}

impl RemoteShardInterface {
    /// Set up a lightweight version of this that can only use operations that query the remote server;
    /// anything that tries to download or upload shards will cause a runtime error.
    pub async fn new_query_only(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
    ) -> Result<Arc<Self>> {
        Self::new(file_query_policy, shard_storage_config, None, None, None).await
    }

    pub async fn new(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
        shard_manager: Option<Arc<ShardFileManager>>,
        cas: Option<Arc<dyn Client + Send + Sync>>,
        repo_salt: Option<RepoSalt>,
    ) -> Result<Arc<Self>> {
        let shard_client = {
            if file_query_policy != FileQueryPolicy::LocalOnly {
                debug!("data_processing: Setting up file reconstructor to query shard server.");
                create_shard_client(shard_storage_config).await.ok()
            } else {
                None
            }
        };

        let shard_manager =
            if file_query_policy != FileQueryPolicy::ServerOnly && shard_manager.is_none() {
                Some(Arc::new(create_shard_manager(shard_storage_config).await?))
            } else {
                shard_manager
            };

        Ok(Arc::new(Self {
            file_query_policy,
            shard_prefix: shard_storage_config.prefix.clone(),
            shard_cache_directory: shard_storage_config
                .cache_config
                .as_ref()
                .map(|cf| cf.cache_directory.clone()),
            shard_session_directory: shard_storage_config.staging_directory.clone(),
            repo_salt,
            shard_manager,
            shard_client,
            reconstruction_cache: Mutex::new(LruCache::new(
                std::num::NonZero::new(FILE_RECONSTRUCTION_CACHE_SIZE).unwrap(),
            )),
            cas,
        }))
    }

    fn cas(&self) -> Result<Arc<dyn Client + Send + Sync>> {
        let Some(cas) = self.cas.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::CASConfigError(
                "tried to contact CAS service but cas client was not configured".to_owned(),
            ))?;
        };

        Ok(cas)
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
            return Err(DataProcessingError::ShardConfigError(
                "cache directory not configured".to_owned(),
            ));
        };

        Ok(cache_dir)
    }

    fn shard_session_directory(&self) -> Result<PathBuf> {
        let Some(session_dir) = self.shard_session_directory.clone() else {
            return Err(DataProcessingError::ShardConfigError(
                "staging directory not configured".to_owned(),
            ));
        };

        Ok(session_dir)
    }

    async fn query_server_for_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        // In this case, no remote to query
        if self.file_query_policy == FileQueryPolicy::LocalOnly {
            return Ok(None);
        }

        Ok(self
            .shard_client()?
            .get_file_reconstruction_info(file_hash)
            .await?)
    }

    async fn get_file_reconstruction_info_impl(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        match self.file_query_policy {
            FileQueryPolicy::LocalFirst => {
                let local_info = self
                    .shard_manager
                    .as_ref()
                    .ok_or_else(|| {
                        MDBShardError::SmudgeQueryPolicyError(
                        "Require ShardFileManager for smudge query policy other than 'server_only'"
                            .to_owned(),
                    )
                    })?
                    .get_file_reconstruction_info(file_hash)
                    .await?;

                if local_info.is_some() {
                    Ok(local_info)
                } else {
                    Ok(self
                        .query_server_for_file_reconstruction_info(file_hash)
                        .await?)
                }
            }
            FileQueryPolicy::ServerOnly => {
                self.query_server_for_file_reconstruction_info(file_hash)
                    .await
            }
            FileQueryPolicy::LocalOnly => Ok(self
                .shard_manager
                .as_ref()
                .ok_or_else(|| {
                    MDBShardError::SmudgeQueryPolicyError(
                        "Require ShardFileManager for smudge query policy other than 'server_only'"
                            .to_owned(),
                    )
                })?
                .get_file_reconstruction_info(file_hash)
                .await?),
        }
    }

    pub async fn get_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        {
            let mut reader = self.reconstruction_cache.lock().unwrap();
            if let Some(res) = reader.get(file_hash) {
                return Ok(Some(res.clone()));
            }
        }
        let response = self.get_file_reconstruction_info_impl(file_hash).await;
        match response {
            Ok(None) => Ok(None),
            Ok(Some(contents)) => {
                // we only cache real stuff
                self.reconstruction_cache
                    .lock()
                    .unwrap()
                    .put(*file_hash, contents.clone());
                Ok(Some(contents))
            }
            Err(e) => Err(e),
        }
    }

    /// Probes which shards provides dedup information for a chunk.
    /// Returns a list of shard hashes with key under 'prefix',
    /// Err(_) if an error occured.
    async fn get_dedup_shards(
        &self,
        chunk_hash: &[MerkleHash],
        salt: &RepoSalt,
    ) -> Result<Vec<MerkleHash>> {
        if chunk_hash.is_empty() {
            return Ok(vec![]);
        }

        if let Some(shard_client) = self.shard_client.as_ref() {
            debug!(
                "get_dedup_shards: querying for shards with chunk {:?}",
                chunk_hash[0]
            );
            Ok(shard_client
                .get_dedup_shards(&self.shard_prefix, chunk_hash, salt)
                .await?)
        } else {
            Ok(vec![])
        }
    }

    /// Convenience wrapper of above for single chunk query
    pub async fn query_dedup_shard_by_chunk(
        &self,
        chunk_hash: &MerkleHash,
        salt: &RepoSalt,
    ) -> Result<Option<MerkleHash>> {
        Ok(self.get_dedup_shards(&[*chunk_hash], salt).await?.pop())
    }

    pub async fn register_local_shard(&self, shard_hash: &MerkleHash) -> Result<()> {
        let shard_manager = self.shard_manager()?;
        let cache_dir = self.shard_cache_directory()?;

        // Shard is expired, we need to evit the previous registration.
        if shard_manager.shard_is_registered(shard_hash).await {
            info!("register_local_shard: re-register {shard_hash:?}.");
            todo!()
        }

        let shard_file = cache_dir.join(local_shard_name(shard_hash));

        shard_manager
            .register_shards_by_path(&[shard_file], true)
            .await?;

        Ok(())
    }

    pub fn merge_shards(
        &self,
    ) -> Result<JoinHandle<std::result::Result<Vec<MDBShardFile>, MDBShardError>>> {
        let session_dir = self.shard_session_directory()?;

        let merged_shards_jh = tokio::spawn(async move {
            consolidate_shards_in_directory(&session_dir, MDB_SHARD_MIN_TARGET_SIZE)
        });

        Ok(merged_shards_jh)
    }

    pub async fn upload_and_register_shards(&self, shards: Vec<MDBShardFile>) -> Result<()> {
        if shards.is_empty() {
            return Ok(());
        }

        let salt = self.repo_salt()?;
        let shard_client = self.shard_client()?;
        let shard_client_ref = &shard_client;
        let shard_prefix = self.shard_prefix.clone();
        let shard_prefix_ref = &shard_prefix;

        tokio_par_for_each(shards, *MAX_CONCURRENT_UPLOADS, |si, _| async move {
            // For each shard:
            // 1. Upload directly to CAS.
            // 2. Sync to server.

            debug!(
                "Uploading shard {shard_prefix_ref}/{:?} from staging area to CAS.",
                &si.shard_hash
            );
            let data = std::fs::read(&si.path)?;

            // Upload the shard.
            shard_client_ref
                .upload_shard(&self.shard_prefix, &si.shard_hash, false, &data, &salt)
                .await?;

            info!(
                "Shard {shard_prefix_ref}/{:?} upload + sync completed successfully.",
                &si.shard_hash
            );

            Ok(())
        })
        .await
        .map_err(|e| match e {
            parutils::ParallelError::JoinError => {
                DataProcessingError::InternalError("Join Error".into())
            }
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

            std::fs::rename(&file_path, cache_dir.join(file_path.file_name().unwrap()))?;
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
