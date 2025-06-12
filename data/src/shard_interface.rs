use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cas_client::Client;
use error_printer::ErrorPrinter;
use mdb_shard::cas_structs::MDBCASInfo;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use mdb_shard::session_directory::{consolidate_shards_in_directory, merge_shards_background, ShardMergeResult};
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info, info_span, Instrument};

use crate::configurations::TranslatorConfig;
use crate::constants::{
    MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS, SESSION_XORB_METADATA_FLUSH_INTERVAL_SECS,
    SESSION_XORB_METADATA_FLUSH_MAX_COUNT,
};
use crate::errors::Result;
use crate::file_upload_session::acquire_upload_permit;
use crate::repo_salt::RepoSalt;

pub struct SessionShardInterface {
    session_shard_manager: Arc<ShardFileManager>,
    cache_shard_manager: Arc<ShardFileManager>,

    client: Arc<dyn Client + Send + Sync>,
    config: Arc<TranslatorConfig>,

    dry_run: bool,

    // A place to write out shards that can help a future session resume.
    xorb_metadata_staging_dir: PathBuf,

    // If a previous session has been resumed, then we can query against that.  However, this has to
    // be handled differently than the regular session as these xorbs have already been uploaded and are thus
    // tracked differently by the cempletion tracking.
    resumed_session_shard_manager: Option<Arc<ShardFileManager>>,

    // We can remove these shards on final upload success.
    staged_shards_to_remove_on_success: Vec<PathBuf>,

    // The last time we flushed xorb metadata to disk and the current state of xorb metadata.
    xorb_metadata_staging: Mutex<(SystemTime, MDBInMemoryShard)>,

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

        let session_dir = shard_session_tempdir.path().to_owned();

        // Set up the cache dir.
        let cache_dir = &config.shard_config.cache_directory;
        std::fs::create_dir_all(cache_dir)?;

        // Set up the shard session directory.
        let xorb_metadata_staging_dir = config.shard_config.session_directory.join("xorb_metadata");
        std::fs::create_dir_all(&xorb_metadata_staging_dir)?;

        // To allow resume from previous session attempts, merge and copy all the valid shards in the xorb metadata
        // directory into the current session directory. The originals will remain until all the current senssion xorbs
        // have been uploaded successfully.  (Also, don't do this on a dry run, as it could screw up non-dry runs).
        let shard_merge_jh = {
            if !dry_run {
                Some(merge_shards_background(
                    &xorb_metadata_staging_dir,
                    &session_dir,
                    *MDB_SHARD_MIN_TARGET_SIZE,
                    true,
                ))
            } else {
                None
            }
        };

        // Load the cache and session shard managers.
        let cache_shard_manager = ShardFileManager::new_in_cache_directory(cache_dir).await?;
        let session_shard_manager = ShardFileManager::new_in_session_directory(&session_dir, false).await?;

        // Get the new merged shard handles here.
        let shard_merge_result = {
            if let Some(jh) = shard_merge_jh {
                jh.await??
            } else {
                ShardMergeResult::default()
            }
        };

        // If there are shards from a resumed session, load them.
        let resumed_session_shard_manager = {
            if !shard_merge_result.merged_shards.is_empty() {
                // Create a new shard manager to just hold the resumed session shards
                let resumed_session_shard_manager =
                    ShardFileManager::new_in_session_directory(&session_dir, false).await?;

                resumed_session_shard_manager
                    .register_shards(&shard_merge_result.merged_shards)
                    .await?;

                Some(resumed_session_shard_manager)
            } else {
                None
            }
        };

        let staged_shards_to_remove_on_success =
            shard_merge_result.obsolete_shards.iter().map(|sfi| sfi.path.clone()).collect();

        Ok(Self {
            session_shard_manager,
            cache_shard_manager,
            xorb_metadata_staging_dir,
            staged_shards_to_remove_on_success,
            xorb_metadata_staging: Mutex::new((SystemTime::now(), MDBInMemoryShard::default())),
            resumed_session_shard_manager,
            config,
            dry_run,
            _shard_session_dir: shard_session_tempdir,
            client,
        })
    }

    /// Queries the client for global deduplication metrics.
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
        // First, see if there's something in the resumed session.
        if let Some(resumed_session_sfm) = &self.resumed_session_shard_manager {
            if let Some((n_entries, fse)) = resumed_session_sfm.chunk_hash_dedup_query(query_hashes).await? {
                // Return true, as the data here is already known to have been uploaded.
                return Ok(Some((n_entries, fse, true)));
            }
        }

        // Now, check the local session directory.
        let res = self.session_shard_manager.chunk_hash_dedup_query(query_hashes).await?;

        if let Some((n_entries, fse)) = res {
            // These reference xorbs known only to this session.
            return Ok(Some((n_entries, fse, false)));
        }

        // Finally, query in the cache shard manager.
        if let Some((n_entries, fse)) = self.cache_shard_manager.chunk_hash_dedup_query(query_hashes).await? {
            Ok(Some((n_entries, fse, true)))
        } else {
            Ok(None)
        }
    }

    // Add the cas information to the session shard manager and the shard manager for the staged xorbs.
    pub async fn add_cas_block(&self, cas_block_contents: Arc<MDBCASInfo>) -> Result<()> {
        self.session_shard_manager.add_cas_block(cas_block_contents).await?;

        Ok(())
    }

    // Add in uploaded cas information that has been known to be uploaded successfully.
    pub async fn add_uploaded_cas_block(&self, cas_block_contents: Arc<MDBCASInfo>) -> Result<()> {
        // Ignore this part of a dry run
        if self.dry_run {
            return Ok(());
        }

        let mut lg = self.xorb_metadata_staging.lock().await;
        let (last_flush, xorb_shard) = &mut *lg;

        xorb_shard.add_cas_block(cas_block_contents)?;

        let time_now = SystemTime::now();
        let flush_interval = Duration::from_secs(*SESSION_XORB_METADATA_FLUSH_INTERVAL_SECS);

        // Flush if it's time or we've hit enough new shards that we should do the flush
        if *last_flush + flush_interval < time_now
            || xorb_shard.num_cas_entries() >= *SESSION_XORB_METADATA_FLUSH_MAX_COUNT
        {
            xorb_shard.write_to_directory(
                &self.xorb_metadata_staging_dir,
                Some(Duration::from_secs(*MDB_SHARD_LOCAL_CACHE_EXPIRATION_SECS)),
            )?;

            *last_flush = time_now + flush_interval;
            *xorb_shard = MDBInMemoryShard::default();
        }

        Ok(())
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
    pub async fn upload_and_register_session_shards(&self) -> Result<u64> {
        // First, flush everything to disk.
        self.session_shard_manager.flush().await?;

        // First, scan, merge, and fill out any shards in the session directory
        let shard_list = consolidate_shards_in_directory(
            self.session_shard_manager.shard_directory(),
            *MDB_SHARD_MIN_TARGET_SIZE,
            // Here, we want to error out if some of the information isn't present or corrupt, so set skip_on_error to
            // false.
            false,
        )?;

        // Upload all the shards and move each to the common directory.
        let mut shard_uploads = JoinSet::<Result<()>>::new();

        let shard_bytes_uploaded = Arc::new(AtomicU64::new(0));

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

                    shard_bytes_uploaded.fetch_add(data.len() as u64, Ordering::Relaxed);

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

        // Now that everything is complete, attempt to remove all the files in the staging
        // directory that are now correctly uploaded.
        for obsolete_shard in self.staged_shards_to_remove_on_success.iter() {
            // This is a best effort; no real harm in keeping these, so ignore errors.
            let _ = std::fs::remove_file(obsolete_shard);
        }

        Ok(shard_bytes_uploaded.load(Ordering::Relaxed))
    }
}
