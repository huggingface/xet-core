use std::fs::{metadata, File};
use std::io::{BufReader, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::{CasObject, SerializedCasObject};
use cas_types::{FileRange, Key};
use file_utils::SafeFileCreator;
use heed::types::*;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use mdb_shard::{MDBShardFile, MDBShardInfo, ShardFileManager};
use merkledb::aggregate_hashes::with_salt;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

use crate::error::{CasClientError, Result};
use crate::interface::{OutputProvider, ShardDedupProber, UploadClient};
use crate::{Client, ReconstructionClient, RegistrationClient, ShardClientInterface};

pub struct LocalClient {
    tmp_dir: Option<TempDir>, // To hold directory to use for local testing
    base_dir: PathBuf,
    xorb_dir: PathBuf,
    shard_dir: PathBuf,
    shard_manager: Arc<ShardFileManager>,
    global_dedup_db_env: heed::Env,
    global_dedup_table: heed::Database<OwnedType<MerkleHash>, OwnedType<MerkleHash>>,

    shard_cache_dir: Option<PathBuf>,
}

impl LocalClient {
    pub fn temporary() -> Result<Self> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_owned();
        let mut s = Self::new(path, None)?;

        s.tmp_dir = Some(tmp_dir);
        Ok(s)
    }

    pub fn temporary_with_global_dedup(shard_cache_dir: PathBuf) -> Result<Self> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_owned();
        let mut s = Self::new(path, Some(shard_cache_dir))?;

        s.tmp_dir = Some(tmp_dir);
        Ok(s)
    }

    pub fn new(path: impl AsRef<Path>, shard_cache_dir: Option<PathBuf>) -> Result<Self> {
        let base_dir = std::path::absolute(path)?;
        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        let shard_dir = base_dir.join("shards");
        if !shard_dir.exists() {
            std::fs::create_dir_all(&shard_dir)?;
        }

        let xorb_dir = base_dir.join("xorbs");
        if !xorb_dir.exists() {
            std::fs::create_dir_all(&xorb_dir)?;
        }

        let global_dedup_dir = base_dir.join("global_dedup_lookup.db");
        if !global_dedup_dir.exists() {
            std::fs::create_dir_all(&global_dedup_dir)?;
        }

        // Open / setup the global dedup lookup
        let global_dedup_db_env = heed::EnvOpenOptions::new()
            .max_dbs(32)
            .max_readers(32)
            .open(&global_dedup_dir)
            .map_err(|e| CasClientError::Other(format!("Error opening db at {global_dedup_dir:?}: {e}")))?;

        let global_dedup_table = global_dedup_db_env
            .create_database(None)
            .map_err(|e| CasClientError::Other(format!("Error opening heed table: {e}")))?;

        // Open / setup the shard lookup
        let shard_directory_ = shard_dir.clone();
        let shard_manager = tokio::task::block_in_place(|| {
            Handle::current()
                .block_on(async move { ShardFileManager::new_in_session_directory(shard_directory_, true).await })
        })?;

        Ok(Self {
            tmp_dir: None,
            base_dir,
            shard_dir,
            xorb_dir,
            shard_manager,
            global_dedup_db_env,
            global_dedup_table,
            shard_cache_dir,
        })
    }

    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, hash: &MerkleHash) -> PathBuf {
        self.xorb_dir.join(format!("default.{hash:?}"))
    }

    /// Returns all entries in the local client
    pub fn get_all_entries(&self) -> Result<Vec<Key>> {
        let mut ret: Vec<_> = Vec::new();

        // loop through the directory
        self.xorb_dir
            .read_dir()
            .map_err(CasClientError::internal)?
            // take only entries which are ok
            .filter_map(|x| x.ok())
            // take only entries whose filenames convert into strings
            .filter_map(|x| x.file_name().into_string().ok())
            .for_each(|x| {
                let mut is_okay = false;

                // try to split the string with the path format [prefix].[hash]
                if let Some(pos) = x.rfind('.') {
                    let prefix = &x[..pos];
                    let hash = &x[(pos + 1)..];

                    if let Ok(hash) = MerkleHash::from_hex(hash) {
                        ret.push(Key {
                            prefix: prefix.into(),
                            hash,
                        });
                        is_okay = true;
                    }
                }
                if !is_okay {
                    debug!("File '{x:?}' in staging area not in valid format, ignoring.");
                }
            });
        Ok(ret)
    }

    /// Deletes an entry
    pub fn delete(&self, hash: &MerkleHash) {
        let file_path = self.get_path_for_entry(hash);

        // unset read-only for Windows to delete
        #[cfg(windows)]
        {
            if let Ok(metadata) = std::fs::metadata(&file_path) {
                let mut permissions = metadata.permissions();
                #[allow(clippy::permissions_set_readonly_false)]
                permissions.set_readonly(false);
                let _ = std::fs::set_permissions(&file_path, permissions);
            }
        }

        let _ = std::fs::remove_file(file_path);
    }

    pub fn get(&self, hash: &MerkleHash) -> Result<Vec<u8>> {
        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;
        let result = cas.get_all_bytes(&mut reader)?;
        Ok(result)
    }

    /// Get uncompressed bytes from a CAS object within chunk ranges.
    /// Each tuple in chunk_ranges represents a chunk index range [a, b)
    fn get_object_range(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Vec<u8>>> {
        // Handle the case where we aren't asked for any real data.
        if chunk_ranges.is_empty() {
            return Ok(vec![vec![]]);
        }

        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;

        let mut ret: Vec<Vec<u8>> = Vec::new();
        for r in chunk_ranges {
            if r.0 >= r.1 {
                ret.push(vec![]);
                continue;
            }

            let data = cas.get_bytes_by_chunk_range(&mut reader, r.0, r.1)?;
            ret.push(data);
        }
        Ok(ret)
    }

    fn get_length(&self, hash: &MerkleHash) -> Result<u32> {
        let file_path = self.get_path_for_entry(hash);
        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let cas = CasObject::deserialize(&mut reader)?;
                let length = cas.get_all_bytes(&mut reader)?.len();
                Ok(length as u32)
            },
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }
}

/// LocalClient is responsible for writing/reading Xorbs on local disk.
#[async_trait]
impl UploadClient for LocalClient {
    async fn upload_xorb(
        &self,
        _prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
    ) -> Result<u64> {
        // moved hash validation into [CasObject::serialize], so removed from here.
        let hash = &serialized_cas_object.hash;
        if self.exists("", hash).await? {
            info!("object {hash:?} already exists in Local CAS; returning.");
            return Ok(0);
        }

        let file_path = self.get_path_for_entry(hash);
        info!("Writing XORB {hash:?} to local path {file_path:?}");

        let mut file = SafeFileCreator::new(&file_path)?;

        // Do a partial work of this to test the upload_tracker.

        for i in 0..10 {
            let start = (i * serialized_cas_object.serialized_data.len()) / 10;
            let end = ((i + 1) * serialized_cas_object.serialized_data.len()) / 10;

            file.write_all(&serialized_cas_object.serialized_data[start..end])?;

            if let Some(upload_tracker) = &upload_tracker {
                // Do this carefully so that we don't perpetually underflow
                let adjusted_byte_start = (i * serialized_cas_object.raw_num_bytes as usize) / 10;
                let adjusted_byte_end = ((i + 1) * serialized_cas_object.raw_num_bytes as usize) / 10;

                let adjusted_progress = adjusted_byte_end - adjusted_byte_start;

                // Now report the progress.
                upload_tracker
                    .register_xorb_upload_progress(*hash, adjusted_progress as u64)
                    .await;
            }
        }

        let bytes_written = serialized_cas_object.serialized_data.len();
        file.close()?;

        // attempt to set to readonly on unix.
        // On windows, this may pose issues if a xorb has recently
        // been deleted and `exists` returns false, but the FS
        // still has the metadata (and previous xorb was read-only).
        #[cfg(unix)]
        if let Ok(metadata) = metadata(&file_path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(true);
            let _ = std::fs::set_permissions(&file_path, permissions);
        }

        info!("{file_path:?} successfully written with {bytes_written} bytes.");

        Ok(bytes_written as u64)
    }

    async fn exists(&self, _prefix: &str, hash: &MerkleHash) -> Result<bool> {
        let file_path = self.get_path_for_entry(hash);

        let res = metadata(&file_path);

        if res.is_err() {
            return Ok(false);
        }

        if !res.unwrap().is_file() {
            return Err(CasClientError::internal(format!(
                "Attempting to write to {file_path:?}, but it is not a file"
            )));
        };

        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                CasObject::deserialize(&mut reader)?;
                Ok(true)
            },
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }

    fn use_xorb_footer(&self) -> bool {
        true
    }
}

#[async_trait]
impl RegistrationClient for LocalClient {
    async fn upload_shard(
        &self,
        _prefix: &str, // Prefix not used in current implementation
        shard_hash: &MerkleHash,
        _force_sync: bool,
        shard_data: &[u8],
        salt: &[u8; 32],
    ) -> Result<bool> {
        // Write out the shard to the shard directory.
        let shard = MDBShardFile::write_out_from_reader(&self.shard_dir, &mut Cursor::new(shard_data))?;

        self.shard_manager.register_shards(&[shard]).await?;

        // Add dedup info to the global dedup table.
        let mut shard_reader = Cursor::new(shard_data);

        let chunk_hashes = MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut shard_reader)?;

        let mut write_txn = self.global_dedup_db_env.write_txn().map_err(map_heed_db_error)?;

        for chunk in chunk_hashes {
            if let Ok(salted_chunk_hash) = with_salt(&chunk, salt) {
                self.global_dedup_table
                    .put(&mut write_txn, &salted_chunk_hash, shard_hash)
                    .map_err(map_heed_db_error)?;
            }
        }

        write_txn.commit().map_err(map_heed_db_error)?;

        Ok(true)
    }
}

#[async_trait]
impl FileReconstructor<CasClientError> for LocalClient {
    /// Query the shard server for the file reconstruction info.
    /// Returns the FileInfo for reconstructing the file and the shard ID that
    /// defines the file info.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        Ok(self.shard_manager.get_file_reconstruction_info(file_hash).await?)
    }
}

#[async_trait]
impl ShardDedupProber for LocalClient {
    async fn query_for_global_dedup_shard(
        &self,
        _prefix: &str,
        chunk_hash: &MerkleHash,
        salt: &[u8; 32],
    ) -> Result<Option<PathBuf>> {
        let read_txn = self.global_dedup_db_env.read_txn().map_err(map_heed_db_error)?;

        let Some(shard_cache_dir) = self.shard_cache_dir.as_ref() else {
            return Err(CasClientError::Other("Shard cache directory not set for get_dedup_shards.".to_owned()));
        };

        if let Ok(sh) = with_salt(chunk_hash, salt) {
            if let Some(shard) = self.global_dedup_table.get(&read_txn, &sh).map_err(map_heed_db_error)? {
                let filename = shard_file_name(&shard);
                let dest = shard_cache_dir.join(&filename);
                std::fs::copy(self.shard_dir.join(&filename), &dest)?;
                return Ok(Some(dest));
            }
        }
        Ok(None)
    }
}

impl ShardClientInterface for LocalClient {}

#[async_trait]
impl ReconstructionClient for LocalClient {
    async fn get_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        _progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let Some((file_info, _)) = self
            .shard_manager
            .get_file_reconstruction_info(hash)
            .await
            .map_err(|e| anyhow!("{e}"))?
        else {
            return Err(CasClientError::FileNotFound(*hash));
        };
        let mut writer = output_provider.get_writer_at(0)?;

        // This is just used for testing, so inefficient is fine.
        let mut file_vec = Vec::new();
        for entry in &file_info.segments {
            let mut entry_bytes = self
                .get_object_range(&entry.cas_hash, vec![(entry.chunk_index_start, entry.chunk_index_end)])?
                .pop()
                .unwrap();
            file_vec.append(&mut entry_bytes);
        }

        let start = byte_range.as_ref().map(|range| range.start as usize).unwrap_or(0);
        let end = byte_range
            .as_ref()
            .map(|range| range.end as usize)
            .unwrap_or(file_vec.len())
            .min(file_vec.len());

        writer.write_all(&file_vec[start..end])?;

        Ok((end - start) as u64)
    }
}

impl Client for LocalClient {}

fn map_heed_db_error(e: heed::Error) -> CasClientError {
    let msg = format!("Global shard dedup database error: {e:?}");
    warn!("{msg}");
    CasClientError::Other(msg)
}

#[cfg(test)]
mod tests {
    use cas_object::test_utils::*;
    use cas_object::CompressionScheme::LZ4;
    use deduplication::test_utils::raw_xorb_to_vec;
    use mdb_shard::utils::parse_shard_filename;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_basic_put_get() {
        let xorb = build_raw_xorb(1, ChunkSize::Fixed(2048));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().unwrap();
        assert!(client.upload_xorb("key", cas_object, None).await.is_ok());

        let returned_data = client.get(&hash).unwrap();
        assert_eq!(data, returned_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_basic_put_get_random_medium() {
        let xorb = build_raw_xorb(44, ChunkSize::Random(512, 15633));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().unwrap();
        assert!(client.upload_xorb("", cas_object, None).await.is_ok());

        let returned_data = client.get(&hash).unwrap();
        assert_eq!(data, returned_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_basic_put_get_range_random_small() {
        let xorb = build_raw_xorb(3, ChunkSize::Random(512, 15633));
        let data = raw_xorb_to_vec(&xorb);
        let chunk_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().unwrap();
        assert!(client.upload_xorb("", cas_object, None).await.is_ok());

        let ranges: Vec<(u32, u32)> = vec![(0, 1), (2, 3)];
        let returned_ranges = client.get_object_range(&hash, ranges).unwrap();

        let expected = [
            data[0..chunk_and_boundaries[0].1 as usize].to_vec(),
            data[chunk_and_boundaries[1].1 as usize..chunk_and_boundaries[2].1 as usize].to_vec(),
        ];

        for idx in 0..returned_ranges.len() {
            assert_eq!(expected[idx], returned_ranges[idx]);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_basic_length() {
        let xorb = build_raw_xorb(1, ChunkSize::Fixed(2048));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        let gen_length = data.len();

        // Act
        let client = LocalClient::temporary().unwrap();
        assert!(client.upload_xorb("", cas_object, None).await.is_ok());
        let len = client.get_length(&hash).unwrap();

        // Assert
        assert_eq!(len as usize, gen_length);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_missing_xorb() {
        // Arrange
        let hash = MerkleHash::from_hex("d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da").unwrap();

        // Act & Assert
        let client = LocalClient::temporary().unwrap();
        let result = client.get(&hash);
        assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_failures() {
        let hello = "hello world".as_bytes().to_vec();

        let hello_hash = merklehash::compute_data_hash(&hello[..]);

        let cas_object = serialized_cas_object_from_components(
            &hello_hash,
            hello.clone(),
            vec![(hello_hash, hello.len() as u32)],
            None,
        )
        .unwrap();

        // write "hello world"
        let client = LocalClient::temporary().unwrap();
        client.upload_xorb("default", cas_object, None).await.unwrap();

        let cas_object = serialized_cas_object_from_components(
            &hello_hash,
            hello.clone(),
            vec![(hello_hash, hello.len() as u32)],
            None,
        )
        .unwrap();

        // put the same value a second time. This should be ok.
        client.upload_xorb("default", cas_object, None).await.unwrap();

        // we can list all entries
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r,
            vec![Key {
                prefix: "default".into(),
                hash: hello_hash
            }]
        );

        // compute a hash of something we do not have in the store
        let world = "world".as_bytes().to_vec();
        let world_hash = merklehash::compute_data_hash(&world[..]);

        // get length of non-existant object should fail with XORBNotFound
        assert_eq!(CasClientError::XORBNotFound(world_hash), client.get_length(&world_hash).unwrap_err());

        // read of non-existant object should fail with XORBNotFound
        assert!(client.get(&world_hash).is_err());
        // read range of non-existant object should fail with XORBNotFound
        assert!(client.get_object_range(&world_hash, vec![(0, 5)]).is_err());

        // we can delete non-existant things
        client.delete(&world_hash);

        // delete the entry we inserted
        client.delete(&hello_hash);
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 0);

        // now every read of that key should fail
        assert_eq!(CasClientError::XORBNotFound(hello_hash), client.get_length(&hello_hash).unwrap_err());
        assert_eq!(CasClientError::XORBNotFound(hello_hash), client.get(&hello_hash).unwrap_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_hashing() {
        // hand construct a tree of 2 chunks
        let hello = "hello".as_bytes().to_vec();
        let world = "world".as_bytes().to_vec();
        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        let world_hash = merklehash::compute_data_hash(&world[..]);

        let hellonode = merkledb::MerkleNode::new(0, hello_hash, 5, vec![]);
        let worldnode = merkledb::MerkleNode::new(1, world_hash, 5, vec![]);

        let final_hash = merkledb::detail::hash_node_sequence(&[hellonode, worldnode]);

        // insert should succeed
        let client = LocalClient::temporary().unwrap();
        client
            .upload_xorb(
                "key",
                serialized_cas_object_from_components(
                    &final_hash,
                    "helloworld".as_bytes().to_vec(),
                    vec![(hello_hash, 5), (world_hash, 10)],
                    None,
                )
                .unwrap(),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_global_dedup() {
        let tmp_dir = TempDir::new().unwrap();
        let shard_dir_1 = tmp_dir.path().join("shard_1");
        std::fs::create_dir_all(&shard_dir_1).unwrap();
        let shard_dir_2 = tmp_dir.path().join("shard_2");
        std::fs::create_dir_all(&shard_dir_2).unwrap();

        let shard_in = mdb_shard::shard_format::test_routines::gen_random_shard_with_cas_references(
            0, &[16; 8], &[2; 20], true, true,
        )
        .unwrap();

        let new_shard_path = shard_in.write_to_directory(&shard_dir_1, None).unwrap();

        let shard_hash = parse_shard_filename(&new_shard_path).unwrap();

        let client = LocalClient::temporary_with_global_dedup(shard_dir_2.clone()).unwrap();

        client
            .upload_shard("default", &shard_hash, true, &std::fs::read(&new_shard_path).unwrap(), &[1; 32])
            .await
            .unwrap();

        let dedup_hashes =
            MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut std::fs::File::open(&new_shard_path).unwrap())
                .unwrap();

        assert_ne!(dedup_hashes.len(), 0);

        // Now do the query...
        let new_shard = client
            .query_for_global_dedup_shard("default", &dedup_hashes[0], &[1; 32])
            .await
            .unwrap()
            .unwrap();

        assert_eq!(new_shard, shard_dir_2.join(shard_file_name(&shard_hash)));
    }
}
