use crate::cas_interface::{create_cas_client, data_from_chunks_to_writer};
use crate::clean::Cleaner;
use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_UPLOADS;
use crate::errors::*;
use crate::metrics::FILTER_CAS_BYTES_PRODUCED;
use crate::remote_shard_interface::RemoteShardInterface;
use crate::shard_interface::create_shard_manager;
use crate::PointerFile;

use cas_client::{CASAPIClient, Staging};
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merkledb::aggregate_hashes::cas_node_hash;
use merkledb::ObjectRange;
use merklehash::MerkleHash;
use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

#[derive(Default, Debug)]
pub struct CASDataAggregator {
    pub data: Vec<u8>,
    pub chunks: Vec<(MerkleHash, (usize, usize))>,
    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the default hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified) and the divisions in the chunks corresponding
    // to this file.
    pub pending_file_info: Vec<(MDBFileInfo, Vec<usize>)>,
}

impl CASDataAggregator {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.chunks.is_empty() && self.pending_file_info.is_empty()
    }
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslator {
    /* ----- Configurations ----- */
    config: TranslatorConfig,

    /* ----- Utils ----- */
    shard_manager: Arc<ShardFileManager>,
    remote_shards: Arc<RemoteShardInterface>,
    cas: Arc<dyn Staging + Send + Sync>,

    /* ----- Deduped data shared across files ----- */
    global_cas_data: Arc<Mutex<CASDataAggregator>>,
}

// Constructors
impl PointerFileTranslator {
    pub async fn new(config: TranslatorConfig) -> Result<PointerFileTranslator> {
        let cas_client = create_cas_client(&config.cas_storage_config, &config.repo_info).await?;

        let shard_manager = Arc::new(create_shard_manager(&config.shard_storage_config).await?);

        let remote_shards = {
            if let Some(dedup) = &config.dedup_config {
                RemoteShardInterface::new(
                    config.file_query_policy,
                    &config.shard_storage_config,
                    Some(shard_manager.clone()),
                    Some(cas_client.clone()),
                    dedup.repo_salt,
                )
                .await?
            } else {
                RemoteShardInterface::new_query_only(
                    config.file_query_policy,
                    &config.shard_storage_config,
                )
                .await?
            }
        };

        Ok(Self {
            config,
            shard_manager,
            remote_shards,
            cas: cas_client,
            global_cas_data: Default::default(),
        })
    }
}

/// Clean operations
impl PointerFileTranslator {
    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    pub async fn start_clean(
        &self,
        buffer_size: usize,
        file_name: Option<&Path>,
    ) -> Result<Arc<Cleaner>> {
        let Some(ref dedup) = self.config.dedup_config else {
            return Err(DataProcessingError::DedupConfigError(
                "empty dedup config".to_owned(),
            ));
        };

        Cleaner::new(
            dedup.small_file_threshold,
            matches!(dedup.global_dedup_policy, GlobalDedupPolicy::Always),
            self.config.cas_storage_config.prefix.clone(),
            dedup.repo_salt,
            self.shard_manager.clone(),
            self.remote_shards.clone(),
            self.cas.clone(),
            self.global_cas_data.clone(),
            buffer_size,
            file_name,
        )
        .await
    }

    pub async fn finalize_cleaning(&self) -> Result<()> {
        // flush accumulated CAS data.
        let mut cas_data_accumulator = self.global_cas_data.lock().await;
        let mut new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        if !new_cas_data.is_empty() {
            register_new_cas_block(
                &mut new_cas_data,
                &self.shard_manager,
                &self.cas,
                &self.config.cas_storage_config.prefix,
            )
            .await?;
        }

        debug_assert!(new_cas_data.is_empty());

        self.cas.flush().await?;

        // flush accumulated memory shard.
        self.shard_manager.flush().await?;

        self.upload().await?;

        Ok(())
    }

    async fn upload(&self) -> Result<()> {
        // First, get all the shards prepared and load them.
        let merged_shards_jh = self.remote_shards.merge_shards()?;

        // Make sure that all the uploads and everything are in a good state before proceeding with
        // anything changing the remote repository.
        //
        // Waiting until the CAS uploads finish avoids the following scenario:
        // 1. user 1 commit file A and push, but network drops after
        // sync_notes_to_remote before uploading cas finishes.
        // 2. user 2 tries to git add the same file A, which on filter pulls in
        // the new notes, and file A is 100% deduped so no CAS blocks will be created,
        // and push.
        //
        // This results in a bad repo state.
        self.upload_cas().await?;

        // Get a list of all the merged shards in order to upload them.
        let merged_shards = merged_shards_jh.await??;

        // Now, these need to be sent to the remote.
        self.remote_shards
            .upload_and_register_shards(merged_shards)
            .await?;

        // Finally, we can move all the mdb shards from the session directory, which is used
        // by the upload_shard task, to the cache.
        self.remote_shards
            .move_session_shards_to_local_cache()
            .await?;

        Ok(())
    }

    async fn upload_cas(&self) -> Result<()> {
        self.cas
            .upload_all_staged(*MAX_CONCURRENT_UPLOADS, false)
            .await?;

        Ok(())
    }
}

/// Clean operation helpers
pub(crate) async fn register_new_cas_block(
    cas_data: &mut CASDataAggregator,
    shard_manager: &Arc<ShardFileManager>,
    cas: &Arc<dyn Staging + Send + Sync>,
    cas_prefix: &str,
) -> Result<MerkleHash> {
    let cas_hash = cas_node_hash(&cas_data.chunks[..]);

    let raw_bytes_len = cas_data.data.len();
    // We now assume that the server will compress Xorbs using lz4,
    // without actually compressing the data client-side.
    // The accounting logic will be moved to server-side in the future.
    let compressed_bytes_len = lz4::block::compress(
        &cas_data.data,
        Some(lz4::block::CompressionMode::DEFAULT),
        false,
    )
    .map(|out| out.len())
    .unwrap_or(raw_bytes_len)
    .min(raw_bytes_len);

    let metadata = CASChunkSequenceHeader::new_with_compression(
        cas_hash,
        cas_data.chunks.len(),
        raw_bytes_len,
        compressed_bytes_len,
    );

    let mut pos = 0;
    let chunks: Vec<_> = cas_data
        .chunks
        .iter()
        .map(|(h, (bytes_lb, bytes_ub))| {
            let size = bytes_ub - bytes_lb;
            let result = CASChunkSequenceEntry::new(*h, size, pos);
            pos += size;
            result
        })
        .collect();

    let cas_info = MDBCASInfo { metadata, chunks };

    let mut chunk_boundaries: Vec<u64> = Vec::with_capacity(cas_data.chunks.len());
    let mut running_sum = 0;

    for (_, s) in cas_data.chunks.iter() {
        running_sum += s.1 - s.0;
        chunk_boundaries.push(running_sum as u64);
    }

    if !cas_info.chunks.is_empty() {
        shard_manager.add_cas_block(cas_info).await?;

        cas.put(
            cas_prefix,
            &cas_hash,
            take(&mut cas_data.data),
            chunk_boundaries,
        )
        .await?;
    } else {
        debug_assert_eq!(cas_hash, MerkleHash::default());
    }

    // Now register any new files as needed.
    for (mut fi, chunk_hash_indices) in take(&mut cas_data.pending_file_info) {
        for i in chunk_hash_indices {
            debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
            fi.segments[i].cas_hash = cas_hash;
        }

        shard_manager.add_file_reconstruction_info(fi).await?;
    }

    FILTER_CAS_BYTES_PRODUCED.inc_by(compressed_bytes_len as u64);

    cas_data.data.clear();
    cas_data.chunks.clear();
    cas_data.pending_file_info.clear();

    Ok(cas_hash)
}

/// Smudge operations
impl PointerFileTranslator {
    pub async fn derive_blocks(&self, hash: &MerkleHash) -> Result<Vec<ObjectRange>> {
        if let Some((file_info, _shard_hash)) = self
            .remote_shards
            .get_file_reconstruction_info(hash)
            .await?
        {
            Ok(file_info
                .segments
                .into_iter()
                .map(|s| ObjectRange {
                    hash: s.cas_hash,
                    start: s.chunk_byte_range_start as usize,
                    end: s.chunk_byte_range_end as usize,
                })
                .collect())
        } else {
            error!("File Reconstruction info for hash {hash:?} not found.");
            Err(DataProcessingError::HashNotFound)
        }
    }

    pub async fn smudge_file_from_pointer(
        &self,
        pointer: &PointerFile,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        self.smudge_file_from_hash(&pointer.hash()?, writer, range)
            .await
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        writer: &mut impl std::io::Write,
        _range: Option<(usize, usize)>,
    ) -> Result<()> {
        let endpoint = match &self.config.cas_storage_config.endpoint {
            Endpoint::Server(endpoint) => endpoint.clone(),
            Endpoint::FileSystem(_) => panic!("aaaaaaaa no server"),
        };

        let rc = CASAPIClient::new(&endpoint);

        rc.write_file(file_id, writer).await?;

        // let blocks = self
        //     .derive_blocks(file_id)
        //     .instrument(info_span!("derive_blocks"))
        //     .await?;

        // let ranged_blocks = match range {
        //     Some((start, end)) => {
        //         // we expect callers to validate the range, but just in case, check it anyway.
        //         if end < start {
        //             let msg = format!(
        //                 "End range value requested ({end}) is less than start range value ({start})"
        //             );
        //             error!(msg);
        //             return Err(DataProcessingError::ParameterError(msg));
        //         }
        //         slice_object_range(&blocks, start, end - start)
        //     }
        //     None => blocks,
        // };

        // self.data_from_chunks_to_writer(ranged_blocks, writer)
        //     .await?;

        Ok(())
    }

    async fn data_from_chunks_to_writer(
        &self,
        chunks: Vec<ObjectRange>,
        writer: &mut impl std::io::Write,
    ) -> Result<()> {
        data_from_chunks_to_writer(
            &self.cas,
            self.config.cas_storage_config.prefix.clone(),
            chunks,
            writer,
        )
        .await
    }
}
