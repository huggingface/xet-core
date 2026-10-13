//! Persistent repository output. Reads may use remote dedup; writes never leave the repository.
//!
//! Sessions and linked worktrees share content-addressed objects. SafeFileCreator publishes each
//! object atomically; concurrent writes of the same hash are interchangeable. Its temporary files
//! do not end in `.mdb`, so discovery never loads incomplete shards. No cross-process lock is needed.
//! A future push classifies xorb dependencies from the staged shards, not from file presence. A
//! session's shard lists only the xorbs that session wrote, so the union of all staged xorb tables
//! is the set of local dependencies; each must exist under `xorbs/`, and a missing one is a local
//! error. File records may also reference xorbs absent from every table; those came from remote
//! dedup data and are left to the server. Staging must not be deleted while a clean or push uses it.
use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use xet_client::cas_client::adaptive_concurrency::ConnectionPermit;
use xet_client::cas_client::{Client, MemoryClient, ProgressCallback, ShardUploadProgressCallback, URLProvider};
use xet_client::cas_types::{
    BatchQueryReconstructionResponse, FileChunkHashesResponse, FileRange, QueryReconstructionResponseV2,
};
use xet_client::error::{ClientError, Result};
use xet_core_structures::merklehash::{HashedWrite, MerkleHash};
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
use xet_core_structures::metadata_shard::{MDBShardFile, ShardFileManager};
use xet_core_structures::xorb_object::SerializedXorbObject;
use xet_data::processing::DeduplicationSource;
use xet_runtime::core::XetContext;
use xet_runtime::file_utils::SafeFileCreator;

/// Read-only index of published repository shards; never subject to remote cache eviction.
pub(crate) struct StagedDedupSource {
    manager: Arc<ShardFileManager>,
    registered: tokio::sync::Mutex<HashSet<PathBuf>>,
}

impl StagedDedupSource {
    pub(crate) async fn new(ctx: &XetContext, directory: &Path) -> xet_data::Result<Arc<Self>> {
        std::fs::create_dir_all(directory)?;
        let directory = std::fs::canonicalize(directory)?;
        Ok(Arc::new(Self {
            manager: ShardFileManager::new_in_session_directory(ctx, directory, false).await?,
            registered: tokio::sync::Mutex::new(HashSet::new()),
        }))
    }

    pub(crate) async fn refresh(&self) -> xet_data::Result<()> {
        let mut registered = self.registered.lock().await;
        for entry in std::fs::read_dir(self.manager.shard_directory())? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "mdb") && !registered.contains(&path) {
                // Staging has no expiration timestamp; do not use the cache's validity filter.
                let shard = MDBShardFile::load_from_file(&path, self.manager.shard_file_cache())?;
                self.manager.register_shards(&[shard]).await?;
                registered.insert(path);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DeduplicationSource for StagedDedupSource {
    async fn query(&self, hashes: &[MerkleHash]) -> xet_data::Result<Option<(usize, FileDataSequenceEntry)>> {
        Ok(self.manager.chunk_hash_dedup_query(hashes).await?)
    }
}

pub(crate) struct StagingClient {
    root: PathBuf,
    local: Arc<dyn Client>,
    remote: Option<Arc<dyn Client>>,
    offline: AtomicBool,
}

impl StagingClient {
    pub(crate) fn new(ctx: XetContext, root: &Path, remote: Option<Arc<dyn Client>>) -> Result<Arc<Self>> {
        std::fs::create_dir_all(root.join("xorbs"))?;
        std::fs::create_dir_all(root.join("shards"))?;
        Ok(Arc::new(Self {
            root: root.to_owned(),
            local: MemoryClient::new(ctx),
            remote,
            offline: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl Client for StagingClient {
    async fn query_for_global_dedup_shard(&self, prefix: &str, hash: &MerkleHash) -> Result<Option<Bytes>> {
        let Some(remote) = &self.remote else { return Ok(None) };
        if self.offline.load(Ordering::Relaxed) {
            return Ok(None);
        }
        match tokio::time::timeout(Duration::from_secs(2), remote.query_for_global_dedup_shard(prefix, hash)).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                self.offline.store(true, Ordering::Relaxed);
                Ok(None)
            },
        }
    }

    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        self.local.acquire_upload_permit().await
    }

    async fn upload_xorb(
        &self,
        _prefix: &str,
        object: SerializedXorbObject,
        progress: Option<ProgressCallback>,
        _permit: ConnectionPermit,
    ) -> Result<u64> {
        let size = object.serialized_data.len() as u64;
        let mut file = SafeFileCreator::new(self.root.join("xorbs").join(format!("{}.xorb", object.hash.hex())))?;
        if object.footer_start.is_some() {
            file.write_all(&object.serialized_data)?;
        } else {
            let (_, hash) =
                xet_core_structures::xorb_object::reconstruct_xorb_with_footer(&mut file, &object.serialized_data)?;
            if hash != object.hash {
                return Err(ClientError::Other("Staged xorb hash mismatch".into()));
            }
        }
        file.close()?;
        if let Some(progress) = progress {
            progress(size, size, size);
        }
        Ok(size)
    }

    async fn upload_shard(
        &self,
        data: Bytes,
        _permit: ConnectionPermit,
        _progress: Option<ShardUploadProgressCallback>,
    ) -> Result<()> {
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(data), true, true)?;
        let mut bytes = Vec::new();
        let mut writer = HashedWrite::new(&mut bytes);
        let verification = (0..shard.num_files()).all(|i| shard.file(i).is_some_and(|f| f.contains_verification()));
        shard.serialize(&mut writer, verification, None)?;
        writer.flush()?;
        let hash = writer.hash();
        let mut file = SafeFileCreator::new(self.root.join("shards").join(format!("{}.mdb", hash.hex())))?;
        file.write_all(&bytes)?;
        file.close()?;
        Ok(())
    }

    async fn get_file_reconstruction_info(
        &self,
        hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.local.get_file_reconstruction_info(hash).await
    }
    async fn get_reconstruction(
        &self,
        hash: &MerkleHash,
        range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.local.get_reconstruction(hash, range).await
    }
    async fn batch_get_reconstruction(&self, hashes: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        self.local.batch_get_reconstruction(hashes).await
    }
    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        self.local.acquire_download_permit().await
    }
    async fn get_file_term_data(
        &self,
        url: Box<dyn URLProvider>,
        permit: ConnectionPermit,
        progress: Option<ProgressCallback>,
        size: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.local.get_file_term_data(url, permit, progress, size).await
    }
    async fn get_file_chunk_hashes(
        &self,
        hash: &MerkleHash,
        ranges: Vec<FileRange>,
    ) -> Result<FileChunkHashesResponse> {
        self.local.get_file_chunk_hashes(hash, ranges).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct DedupStub {
        calls: AtomicUsize,
        invalid: bool,
    }

    #[async_trait]
    impl Client for DedupStub {
        async fn query_for_global_dedup_shard(&self, _prefix: &str, _hash: &MerkleHash) -> Result<Option<Bytes>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.invalid {
                return Err(ClientError::InvalidResponse("malformed dedup response".into()));
            }
            std::future::pending().await
        }
        async fn get_file_reconstruction_info(
            &self,
            _file_hash: &MerkleHash,
        ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
            panic!("remote get_file_reconstruction_info must not be called while staging")
        }
        async fn get_reconstruction(
            &self,
            _file_id: &MerkleHash,
            _bytes_range: Option<FileRange>,
        ) -> Result<Option<QueryReconstructionResponseV2>> {
            panic!("remote get_reconstruction must not be called while staging")
        }
        async fn batch_get_reconstruction(&self, _file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
            panic!("remote batch_get_reconstruction must not be called while staging")
        }
        async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
            panic!("remote acquire_download_permit must not be called while staging")
        }
        async fn get_file_term_data(
            &self,
            _url_info: Box<dyn URLProvider>,
            _download_permit: ConnectionPermit,
            _progress_callback: Option<ProgressCallback>,
            _uncompressed_size_if_known: Option<usize>,
        ) -> Result<(Bytes, Vec<u32>)> {
            panic!("remote get_file_term_data must not be called while staging")
        }
        async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
            panic!("remote acquire_upload_permit must not be called while staging")
        }
        async fn upload_shard(
            &self,
            _shard_data: bytes::Bytes,
            _upload_permit: ConnectionPermit,
            _progress_callback: Option<ShardUploadProgressCallback>,
        ) -> Result<()> {
            panic!("remote upload_shard must not be called while staging")
        }
        async fn upload_xorb(
            &self,
            _prefix: &str,
            _serialized_xorb_object: SerializedXorbObject,
            _progress_callback: Option<ProgressCallback>,
            _upload_permit: ConnectionPermit,
        ) -> Result<u64> {
            panic!("remote upload_xorb must not be called while staging")
        }
        async fn get_file_chunk_hashes(
            &self,
            _file_id: &MerkleHash,
            _dirty_ranges: Vec<FileRange>,
        ) -> Result<FileChunkHashesResponse> {
            panic!("remote get_file_chunk_hashes must not be called while staging")
        }
    }

    #[tokio::test]
    async fn session_writes_only_to_staging() -> anyhow::Result<()> {
        use xet_data::processing::configurations::TranslatorConfig;
        use xet_data::processing::{FileUploadSession, Sha256Policy, UploadShardCachePolicy};

        let ctx = XetContext::default()?;
        let dir = tempfile::tempdir()?;
        let remote = Arc::new(DedupStub {
            calls: AtomicUsize::new(0),
            invalid: false,
        });
        let client = StagingClient::new(ctx.clone(), dir.path(), Some(remote))?;
        let config = TranslatorConfig::memory_config(&ctx, dir.path().join("pending"))?;
        let cache = config.shard_cache_directory.clone();
        let session =
            FileUploadSession::new_with_client(Arc::new(config), client, None, UploadShardCachePolicy::Skip).await?;
        let (_, mut cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
        cleaner.add_data(&vec![0; 100001]).await?;
        cleaner.finish().await?;
        session.finalize().await?;
        assert!(std::fs::read_dir(dir.path().join("xorbs"))?.next().is_some());
        assert!(std::fs::read_dir(dir.path().join("shards"))?.next().is_some());
        assert_eq!(std::fs::read_dir(cache)?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn staged_dependencies_stay_local_but_remote_cache_is_shared() -> anyhow::Result<()> {
        use xet_core_structures::metadata_shard::{
            MDB_SHARD_LOCAL_CACHE_EXPIRATION, MDBShardFile, new_shard_file_cache,
        };
        use xet_data::processing::configurations::TranslatorConfig;
        use xet_data::processing::{FileUploadSession, Sha256Policy, UploadShardCachePolicy};

        let ctx = XetContext::default()?;
        let dir = tempfile::tempdir()?;
        let cache = dir.path().join("global-cache");
        let data = vec![0; 100001];
        let mut published: Vec<PathBuf> = Vec::new();
        // Two sessions in one repository share staging; another repository shares only the cache.
        for (i, repository) in ["repo-a", "repo-a", "repo-b", "repo-c"].iter().enumerate() {
            let root = dir.path().join(repository);
            let client = StagingClient::new(ctx.clone(), &root, None)?;
            let mut config = TranslatorConfig::memory_config(&ctx, dir.path().join(format!("session-{i}")))?;
            config.shard_cache_directory = cache.clone();
            let source = StagedDedupSource::new(&ctx, &root.join("shards")).await?;
            let session = FileUploadSession::new_with_client(
                Arc::new(config),
                client,
                Some(source.clone()),
                UploadShardCachePolicy::Skip,
            )
            .await?;
            if i == 1 {
                source.refresh().await?;
                // No xorb should be written when reusing repo-a's staged data.
                std::fs::rename(root.join("xorbs"), root.join("saved-xorbs"))?;
                std::fs::write(root.join("xorbs"), b"blocked")?;
            }
            if i == 3 {
                // Model a shard known to be remotely available in the shared cache.
                let shard = MDBShardFile::load_from_file(&published[0], &new_shard_file_cache())?;
                shard.export_with_expiration(&cache, *MDB_SHARD_LOCAL_CACHE_EXPIRATION, &new_shard_file_cache())?;
                // A fresh session loads the newly populated cache.
                drop(session);
                let mut config = TranslatorConfig::memory_config(&ctx, dir.path().join("cached-session"))?;
                config.shard_cache_directory = cache.clone();
                let client = StagingClient::new(ctx.clone(), &root, None)?;
                let source = StagedDedupSource::new(&ctx, &root.join("shards")).await?;
                let session = FileUploadSession::new_with_client(
                    Arc::new(config),
                    client,
                    Some(source.clone()),
                    UploadShardCachePolicy::Skip,
                )
                .await?;
                let (_, mut cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
                cleaner.add_data(&data).await?;
                cleaner.finish().await?;
                session.finalize().await?;
                assert_eq!(std::fs::read_dir(root.join("xorbs"))?.count(), 0);
                assert_eq!(std::fs::read_dir(cache)?.count(), 1);
                break;
            }
            let (_, mut cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
            cleaner.add_data(&data).await?;
            cleaner.finish().await?;
            session.finalize().await?;
            assert_eq!(std::fs::read_dir(&cache)?.count(), 0);
            if i == 0 {
                published = std::fs::read_dir(root.join("shards"))?
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<std::io::Result<Vec<_>>>()?;
            }
            if i == 2 {
                // Repo-b cannot refer to repo-a's unuploaded xorb.
                assert_eq!(std::fs::read_dir(root.join("xorbs"))?.count(), 1);
            }
        }
        Ok(())
    }

    // Push will rely on this: a staged xorb table lists exactly the xorbs written locally,
    // never xorbs referenced through remote dedup.
    #[tokio::test]
    async fn staged_xorb_tables_list_only_local_xorbs() -> anyhow::Result<()> {
        use std::collections::{HashMap, HashSet};

        use xet_client::cas_client::DirectAccessClient;
        use xet_data::processing::configurations::TranslatorConfig;
        use xet_data::processing::{FileUploadSession, Sha256Policy, UploadShardCachePolicy};

        async fn clean(session: &Arc<FileUploadSession>, data: &[u8]) -> anyhow::Result<MerkleHash> {
            let (_, mut cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
            cleaner.add_data(data).await?;
            let (info, _) = cleaner.finish().await?;
            Ok(MerkleHash::from_hex(&info.hash)?)
        }
        let random = |mut state: u64| {
            (0..300_000)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect::<Vec<_>>()
        };
        let (remote_file, local_file) = (random(1), random(2));
        let ctx = XetContext::default()?;
        let dir = tempfile::tempdir()?;

        // Make one file globally dedupable on the remote.
        let remote = MemoryClient::new(ctx.clone());
        remote.set_global_dedup_shard_expiration(Some(Duration::from_secs(300)));
        let config = TranslatorConfig::memory_config(&ctx, dir.path().join("remote-session"))?;
        let session =
            FileUploadSession::new_with_client(Arc::new(config), remote.clone(), None, UploadShardCachePolicy::Skip)
                .await?;
        clean(&session, &remote_file).await?;
        session.finalize().await?;

        let root = dir.path().join("staging");
        let client = StagingClient::new(ctx.clone(), &root, Some(remote))?;
        let config = TranslatorConfig::memory_config(&ctx, dir.path().join("staging-session"))?;
        let session =
            FileUploadSession::new_with_client(Arc::new(config), client, None, UploadShardCachePolicy::Skip).await?;
        let remote_hash = clean(&session, &remote_file).await?;
        let local_hash = clean(&session, &local_file).await?;
        session.finalize().await?;

        let mut table = HashSet::new();
        let mut references = HashMap::<MerkleHash, Vec<MerkleHash>>::new();
        for entry in std::fs::read_dir(root.join("shards"))? {
            let shard = MDBMinimalShard::from_reader(&mut Cursor::new(std::fs::read(entry?.path())?), true, true)?;
            table.extend((0..shard.num_xorb()).map(|i| shard.xorb(i).unwrap().xorb_hash()));
            for i in 0..shard.num_files() {
                let file = shard.file(i).unwrap();
                let xorbs = (0..file.num_entries()).map(|j| file.entry(j).xorb_hash);
                references.entry(file.file_hash()).or_default().extend(xorbs);
            }
        }
        let on_disk = std::fs::read_dir(root.join("xorbs"))?
            .map(|entry| Ok(MerkleHash::from_hex(entry?.path().file_stem().unwrap().to_str().unwrap())?))
            .collect::<anyhow::Result<HashSet<_>>>()?;
        assert_eq!(table, on_disk);
        assert!(references[&local_hash].iter().all(|xorb| table.contains(xorb)));
        // The remote file depends on a xorb that staging neither wrote nor listed.
        assert!(references[&remote_hash].iter().any(|xorb| !table.contains(xorb)));
        Ok(())
    }

    #[tokio::test]
    async fn offline_lookup_is_bounded_and_disabled() -> Result<()> {
        let ctx = XetContext::default().unwrap();
        let dir = tempfile::tempdir()?;
        let remote = Arc::new(DedupStub {
            calls: AtomicUsize::new(0),
            invalid: false,
        });
        let client = StagingClient::new(ctx, dir.path(), Some(remote.clone()))?;
        for _ in 0..2 {
            let value = tokio::time::timeout(
                Duration::from_secs(5),
                client.query_for_global_dedup_shard("default", &MerkleHash::default()),
            )
            .await
            .unwrap()?;
            assert!(value.is_none());
        }
        assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn client_errors_disable_remote_dedup() -> Result<()> {
        let ctx = XetContext::default().unwrap();
        let dir = tempfile::tempdir()?;
        let remote = Arc::new(DedupStub {
            calls: AtomicUsize::new(0),
            invalid: true,
        });
        let client = StagingClient::new(ctx, dir.path(), Some(remote))?;
        assert!(
            client
                .query_for_global_dedup_shard("default", &MerkleHash::default())
                .await?
                .is_none()
        );
        assert!(client.offline.load(Ordering::Relaxed));
        Ok(())
    }
}
