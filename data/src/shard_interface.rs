use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cas_client::Client;
use error_printer::ErrorPrinter;
use mdb_shard::cas_structs::MDBCASInfo;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tempfile::TempDir;
use tokio::task::JoinSet;
use tracing::{debug, info, info_span, Instrument};

use crate::configurations::TranslatorConfig;
use crate::constants::MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS;
use crate::errors::Result;
use crate::file_upload_session::acquire_upload_permit;
use crate::repo_salt::RepoSalt;

pub struct SessionShardInterface {
    session_shard_manager: Arc<ShardFileManager>,
    cache_shard_manager: Arc<ShardFileManager>,

    client: Arc<dyn Client + Send + Sync>,
    config: Arc<TranslatorConfig>,

    dry_run: bool,

    _shard_session_dir: TempDir,
}

impl SessionShardInterface {
    pub async fn new(
        config: Arc<TranslatorConfig>,
        client: Arc<dyn Client + Send + Sync>,
        dry_run: bool,
    ) -> Result<Self> {
        // Create a temporary session directory where we hold all the shards before upload.
        std::fs::create_dir_all(&config.shard_config.session_directory)?;
        let shard_session_tempdir = TempDir::new_in(&config.shard_config.session_directory)?;

        // Create the shard session manager.
        let session_dir = shard_session_tempdir.path();
        let session_shard_manager = ShardFileManager::new_in_session_directory(session_dir).await?;

        // Make the cache directory.
        let cache_dir = &config.shard_config.cache_directory;
        std::fs::create_dir_all(cache_dir)?;
        let cache_shard_manager = ShardFileManager::new_in_cache_directory(cache_dir).await?;

        Ok(Self {
            session_shard_manager,
            cache_shard_manager,
            client,
            config,
            dry_run,
            _shard_session_dir: shard_session_tempdir,
        })
    }

    /// Queries the client for global deduplication metrics
    pub async fn query_dedup_shard_by_chunk(&self, chunk_hash: &MerkleHash, repo_salt: &RepoSalt) -> Result<bool> {
        let Ok(Some(new_shard_file)) = self
            .client
            .query_for_global_dedup_shard(&self.config.shard_config.prefix, chunk_hash, repo_salt)
            .await
            .info_error("Error attempting to query global dedup lookup.")
        else {
            return Ok(false);
        };

        // The above process found something and downloaded it; it should now be in the cache directory and valid
        // for deduplication.  Register it and restart the dedup process at the start of this chunk.
        self.cache_shard_manager.register_shards_by_path(&[new_shard_file]).await?;

        Ok(true)
    }

    /// Returns the number of chunks and xorb to dedup against, as well as whether it's already known to be uploaded.
    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry, bool)>> {
        // First check for a deduplication hit in the session directory, then in the common cache directory.
        let res = self.session_shard_manager.chunk_hash_dedup_query(query_hashes).await?;

        if let Some((n_entries, fse)) = res {
            return Ok(Some((n_entries, fse, false)));
        }

        // Now query in the cache shard manager; these shards have already been uploaded.
        if let Some((n_entries, fse)) = self.cache_shard_manager.chunk_hash_dedup_query(query_hashes).await? {
            Ok(Some((n_entries, fse, true)))
        } else {
            Ok(None)
        }
    }

    // Add the cas information to the session shard manager
    pub async fn add_cas_block(&self, cas_block_contents: MDBCASInfo) -> Result<()> {
        Ok(self.session_shard_manager.add_cas_block(cas_block_contents).await?)
    }

    // Add the file reconstruction information to the session shard manager
    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        self.session_shard_manager.add_file_reconstruction_info(file_info).await?;

        Ok(())
    }

    /// Returns a list of all file info currently in the session directory.  Must be called before
    /// upload_and_register_session_shards.
    pub async fn session_file_info_list(&self) -> Result<Vec<MDBFileInfo>> {
        Ok(self.session_shard_manager.all_file_info().await?)
    }

    /// Uploads everything in the current session directory.  This must be called after all xorbs
    /// have completed their upload.
    pub async fn upload_and_register_session_shards(&self) -> Result<usize> {
        // First, flush everything to disk.
        self.session_shard_manager.flush().await?;

        // First, scan, merge, and fill out any shards in the session directory
        let shard_list =
            consolidate_shards_in_directory(self.session_shard_manager.shard_directory(), *MDB_SHARD_MIN_TARGET_SIZE)?;

        // Upload all the shards and move each to the common directory.
        let mut shard_uploads = JoinSet::<Result<()>>::new();

        let shard_bytes_uploaded = Arc::new(AtomicUsize::new(0));

        for si in shard_list {
            let salt = self.config.shard_config.repo_salt;
            let shard_client = self.client.clone();
            let shard_prefix = self.config.shard_config.prefix.clone();
            let cache_shard_manager = self.cache_shard_manager.clone();
            let shard_bytes_uploaded = shard_bytes_uploaded.clone();
            let dry_run = self.dry_run;

            // Acquire a permit for uploading before we spawn the task; the acquired permit is dropped after the task
            // completes. The chosen Semaphore is fair, meaning xorbs added first will be scheduled to upload first.
            //
            // It's also important to acquire the permit before the task is launched; otherwise, we may spawn an
            // unlimited number of tasks that end up using up a ton of memory; this forces the pipeline to
            // block here while the upload is happening.
            let upload_permit = acquire_upload_permit().await?;

            shard_uploads.spawn(
                async move {
                    debug!("Uploading shard {shard_prefix}/{:?} from staging area to CAS.", &si.shard_hash);
                    let data = std::fs::read(&si.path)?;

                    shard_bytes_uploaded.fetch_add(data.len(), Ordering::Relaxed);

                    if dry_run {
                        // In dry run mode, don't upload the shards or move them to the cache.
                        return Ok(());
                    }

                    // Upload the shard.
                    shard_client
                        .upload_shard(&shard_prefix, &si.shard_hash, false, &data, &salt)
                        .await?;

                    // Done with the upload, drop the permit.
                    drop(upload_permit);

                    info!("Shard {shard_prefix}/{:?} upload + sync completed successfully.", &si.shard_hash);

                    // Now that the upload succeeded, move that shard to the cache directory, adding in an expiration
                    // time.
                    let new_shard_path = si.export_with_expiration(
                        cache_shard_manager.shard_directory(),
                        Duration::from_secs(*MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS),
                    )?;

                    // Register that new shard in the cache shard manager
                    cache_shard_manager.register_shards(&[new_shard_path]).await?;

                    Ok(())
                }
                .instrument(info_span!("shard_session::upload_shard_task")),
            );
        }

        // Now, let them all complete in parallel
        while let Some(jh) = shard_uploads.join_next().await {
            jh??;
        }

        Ok(shard_bytes_uploaded.load(Ordering::Relaxed))
    }
}
