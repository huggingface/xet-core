use std::sync::Arc;

use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::ShardFileManager;
use tempfile::TempDir;

use super::configurations::StorageConfig;
use super::errors::Result;
use crate::configurations::TranslatorConfig;
use crate::constants::MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS;

pub struct SessionShardInterface {
    session_shard_manager: Arc<ShardFileManager>,
    cache_shard_manager: Arc<ShardFileManager>,

    client: Arc<dyn Client + Send + Sync>,
    config: Arc<TranslatorConfig>,

    _shard_session_dir: TempDir,
}

impl SessionShardInterface {
    pub async fn new(config: Arc<TranslatorConfig>, client: Arc<dyn Client + Send + Sync>) -> Result<Self> {
        // Create a temporary session directory where we hold all the shards before upload.
        std::fs::create_dir_all(&config.shard_config.session_directory)?;
        let shard_session_tempdir = TempDir::new_in(&config.shard_config.session_directory)?;

        // Create the shard session manager.
        let session_dir = shard_session_tempdir.path();
        let session_shard_manager = ShardFileManager::new_in_session_directory(shard_session_dir).await?;

        // Make the cache directory.
        let cache_dir = &config.shard_config.cache_directory;
        std::fs::create_dir_all(cache_dir)?;
        let cache_shard_manager = ShardFileManager::new_in_cache_directory(cache_dir).await?;

        Ok(Self {
            session_shard_manager,
            cache_shard_manager,
            client,
            config,
            _shard_session_dir: shard_session_tempdir,
        })
    }

    /// Queries the client for global deduplication metrics
    pub async fn query_dedup_shard_by_chunk(&self, chunk_hash: &MerkleHash, salt: &RepoSalt) -> Result<bool> {
        let Ok(query_result) = self
            .client
            .query_dedup_shard_by_chunk(&chunk_hash, &repo_salt)
            .await
            .map_err(|e| {
                debug!("Error encountered attempting to query global dedup table: {e:?}; ignoring.");
                e
            })
        else {
            return Ok(false);
        };

        let Some(new_shard_file) = query_result else {
            debug!("Queried shard for global dedup with hash {query_chunk:?}; nothing found.");
            return Ok(false);
        };

        // The above process found something and downloaded it; it should now be in the cache directory and valid
        // for deduplication.  Register it and restart the dedup process at the start of this chunk.
        debug!("global dedup: {file_name:?} deduplicated by shard {new_shard_file:?}; registering.");
        self.cache_shard_manager.register_shards_by_path(&[new_shard_file]).await?;

        debug!("global dedup: New shard {new_shard_file:?} can be used for deduplication of {file_name:?}; reprocessing file.");

        Ok(true)
    }

    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        // First check for a deduplication hit in the session directory, then in the common cache directory.
        let res = self.session_shard_manager.chunk_hash_dedup_query(query_hashes).await?;

        if res.is_some() {
            return Ok(res);
        }

        // Now query in the cache shard manager.
        self.cache_shard_manager.chunk_hash_dedup_query(query_hashes).await
    }

    // Consumes everything
    pub async fn upload_and_register_current_shards(self) -> Result<()> {
        // First, flush everything to disk.
        self.session_shard_manager.flush().await?;

        // First, scan, merge, and fill out any shards in the session directory
        let shard_list =
            consolidate_shards_in_directory(&self.session_shard_manager.shard_directory(), MDB_SHARD_MIN_TARGET_SIZE)?;

        // Upload all the shards and move each to the common directory.
        let mut shard_uploads = JoinSet::<()>::new();

        for si in shard_list {
            let salt = self.config.shard_config.repo_salt;
            let shard_client = self.client.clone();
            let shard_prefix = self.config.shard_config.prefix.clone();
            let cache_shard_manager = self.cache_shard_manager.clone();

            shard_uploads.spawn(async move {
                debug!("Uploading shard {shard_prefix}/{:?} from staging area to CAS.", &si.shard_hash);
                let data = std::fs::read(&si.path)?;

                // Upload the shard.
                shard_client
                    .upload_shard(&shard_prefix, &si.shard_hash, false, &data, &salt)
                    .await?;

                info!("Shard {shard_prefix}/{:?} upload + sync completed successfully.", &si.shard_hash);

                // Now that the upload succeeded, move that shard to the cache directory, adding in an expiration time.
                let new_shard_path = si.export_with_expiration(
                    cache_shard_manager.shard_directory(),
                    MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS,
                )?;

                // Register that new shard in the cache shard manager
                cache_shard_manager.register_shards_by_path(&[new_shard_path]).await?;

                Ok(())
            });
        }

        // Now, let them all complete in parallel
        let _ = shard_uploads.join_all().await?;
    }
}
