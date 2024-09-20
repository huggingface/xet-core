use crate::error::{CasClientError, Result};
use crate::interface::Client;
use cas::key::Key;
use cas_object::CasObject;
use merkledb::prelude::*;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;
use std::fs::{metadata, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use anyhow::anyhow;
use async_trait::async_trait;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct LocalClient {
    // tempdir is created but never used. it is just RAII for directory deletion
    // of the temporary directory
    #[allow(dead_code)]
    tempdir: Option<TempDir>,
    pub path: PathBuf,
    pub silence_errors: bool,
}
impl Default for LocalClient {
    /// Creates a default local client that writes to a temporary directory
    /// which gets deleted when LocalClient object is destroyed.
    fn default() -> LocalClient {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().to_path_buf();
        LocalClient {
            tempdir: Some(tempdir),
            path,
            silence_errors: false,
        }
    }
}

impl LocalClient {
    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, prefix: &str, hash: &MerkleHash) -> PathBuf {
        self.path.join(format!("{}.{}", prefix, hash.hex()))
    }

    /// Creates a local client that writes to a particular specified path.
    /// Files preexisting in the path may be used to serve queries.
    pub fn new(path: &Path, silence_errors: bool) -> LocalClient {
        LocalClient {
            tempdir: None,
            path: path.to_path_buf(),
            silence_errors,
        }
    }

    /// Returns all entries in the local client
    pub fn get_all_entries(&self) -> Result<Vec<Key>> {
        let mut ret: Vec<_> = Vec::new();

        // loop through the directory
        self.path
            .read_dir()
            .map_err(|x| CasClientError::InternalError(x.into()))?
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

    /// A more complete get() which returns both the chunk boundaries as well
    /// as the raw data
    pub async fn get_detailed(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<(Vec<u64>, Vec<u8>)> {
        let file_path = self.get_path_for_entry(prefix, hash);

        let file = File::open(&file_path).map_err(|_| {
            if !self.silence_errors {
                error!("Unable to find file in local CAS {:?}", file_path);
            }
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;
        let (boundaries, data) = cas.get_detailed_bytes(&mut reader)?;

        Ok((boundaries.into_iter().map(|x| x as u64).collect(), data))
    }

    /// Deletes an entry
    pub fn delete(&self, prefix: &str, hash: &MerkleHash) {
        let file_path = self.get_path_for_entry(prefix, hash);

        // unset read-only for Windows to delete
        #[cfg(windows)]
        {
            if let Ok(metadata) = std::fs::metadata(&file_path) {
                let mut permissions = metadata.permissions();
                permissions.set_readonly(false);
                let _ = std::fs::set_permissions(&file_path, permissions);
            }
        }

        let _ = std::fs::remove_file(file_path);
    }

    fn validate_root_hash(data: &[u8], chunk_boundaries: &[u64], hash: &MerkleHash) -> bool {
        // at least 1 chunk, and last entry in chunk boundary must match the length
        if chunk_boundaries.is_empty()
            || chunk_boundaries[chunk_boundaries.len() - 1] as usize != data.len()
        {
            return false;
        }

        let mut chunks: Vec<Chunk> = Vec::new();
        let mut left_edge: usize = 0;
        for i in chunk_boundaries {
            let right_edge = *i as usize;
            let hash = merklehash::compute_data_hash(&data[left_edge..right_edge]);
            let length = right_edge - left_edge;
            chunks.push(Chunk { hash, length });
            left_edge = right_edge;
        }

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);
        *ret.hash() == *hash
    }
}

/// LocalClient is responsible for writing/reading Xorbs on local disk.
#[async_trait]
impl Client for LocalClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        // no empty writes
        if chunk_boundaries.is_empty() || data.is_empty() {
            return Err(CasClientError::InvalidArguments);
        }

        // last boundary must be end of data
        if !chunk_boundaries.is_empty()
            && chunk_boundaries[chunk_boundaries.len() - 1] as usize != data.len()
        {
            return Err(CasClientError::InvalidArguments);
        }

        // validate hash
        if !Self::validate_root_hash(&data, &chunk_boundaries, hash) {
            return Err(CasClientError::HashMismatch);
        }

        if let Ok(xorb_size) = self.get_length(prefix, hash).await {
            if xorb_size > 0 {
                info!("{prefix:?}/{hash:?} already exists in Local CAS; returning.");
                return Ok(());
            }
        }

        let file_path = self.get_path_for_entry(prefix, hash);
        info!("Writing XORB {prefix}/{hash:?} to local path {file_path:?}");

        if let Ok(metadata) = metadata(&file_path) {
            return if metadata.is_file() {
                info!("{file_path:?} already exists; returning.");
                // if its a file, its ok. we do not overwrite
                Ok(())
            } else {
                // if its not file we have a problem.
                Err(CasClientError::InternalError(anyhow!(
                    "Attempting to write to {:?}, but {:?} is not a file",
                    file_path,
                    file_path
                )))
            };
        }

        // we prefix with "[PID]." for now. We should be able to do a cleanup
        // in the future.
        let tempfile = tempfile::Builder::new()
            .prefix(&format!("{}.", std::process::id()))
            .suffix(".xorb")
            .tempfile_in(&self.path)
            .map_err(|e| {
                CasClientError::InternalError(anyhow!(
                    "Unable to create temporary file for staging Xorbs, got {e:?}"
                ))
            })?;

        let total_bytes_written;
        {
            let mut writer = BufWriter::new(&tempfile);
            let (_, bytes_written) = CasObject::serialize(
                &mut writer,
                hash,
                &data,
                &chunk_boundaries.into_iter().map(|x| x as u32).collect(),
                cas_object::CompressionScheme::None
            )?;
            // flush before persisting
            writer.flush()?;
            total_bytes_written = bytes_written;
        }

        tempfile.persist(&file_path)?;

        // attempt to set to readonly
        // its ok to fail.
        if let Ok(metadata) = std::fs::metadata(&file_path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(true);
            let _ = std::fs::set_permissions(&file_path, permissions);
        }

        info!("{file_path:?} successfully written with {total_bytes_written:?} bytes.");

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        let file_path = self.get_path_for_entry(prefix, hash);
        let file = File::open(&file_path).map_err(|_| {
            if !self.silence_errors {
                error!("Unable to find file in local CAS {:?}", file_path);
            }
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;
        let result = cas.get_all_bytes(&mut reader)?;
        Ok(result)
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        // Handle the case where we aren't asked for any real data.
        if ranges.len() == 1 && ranges[0].0 == ranges[0].1 {
            return Ok(vec![Vec::<u8>::new()]);
        }

        let file_path = self.get_path_for_entry(prefix, hash);
        let file = File::open(&file_path).map_err(|_| {
            if !self.silence_errors {
                error!("Unable to find file in local CAS {:?}", file_path);
            }
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;

        let mut ret: Vec<Vec<u8>> = Vec::new();
        for r in ranges {
            let data = cas.get_range(&mut reader, r.0 as u32, r.1 as u32)?;
            ret.push(data);
        }
        Ok(ret)
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        let file_path = self.get_path_for_entry(prefix, hash);
        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let cas = CasObject::deserialize(&mut reader)?;
                let length = cas.get_contents_length()?;
                Ok(length as u64)
            }
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use merklehash::{compute_data_hash, DataHash};
    use rand::Rng;

    #[tokio::test]
    async fn test_basic_put_get() {
        // Arrange
        let client = LocalClient::default();
        let data = gen_random_bytes(2048);
        let hash = compute_data_hash(&data[..]);
        let chunk_boundaries = vec![data.len() as u64];

        let data_again = data.clone();

        // Act & Assert
        assert!(client
            .put("key", &hash, data, chunk_boundaries)
            .await
            .is_ok());

        let returned_data = client.get("key", &hash).await.unwrap();
        assert_eq!(data_again, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_random_medium() {
        // Arrange
        let client = LocalClient::default();
        let (hash, data, chunk_boundaries) = gen_dummy_xorb(44, 15633, true);
        let data_again = data.clone();

        // Act & Assert
        assert!(client.put("", &hash, data, chunk_boundaries).await.is_ok());

        let returned_data = client.get("", &hash).await.unwrap();
        assert_eq!(data_again, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_range_random_small() {
        // Arrange
        let client = LocalClient::default();
        let (hash, data, chunk_boundaries) = gen_dummy_xorb(3, 2048, true);
        let data_again = data.clone();

        // Act & Assert
        assert!(client.put("", &hash, data, chunk_boundaries).await.is_ok());

        let ranges: Vec<(u64, u64)> = vec![(0, 100), (100, 1500)];
        let ranges_again = ranges.clone();
        let returned_ranges = client.get_object_range("", &hash, ranges).await.unwrap();

        for idx in 0..returned_ranges.len() {
            assert_eq!(
                data_again[ranges_again[idx].0 as usize..ranges_again[idx].1 as usize],
                returned_ranges[idx]
            );
        }
    }

    #[tokio::test]
    async fn test_basic_length() {
        // Arrange
        let client = LocalClient::default();
        let (hash, data, chunk_boundaries) = gen_dummy_xorb(1, 2048, false);
        let gen_length = data.len();

        // Act
        client.put("", &hash, data, chunk_boundaries).await.unwrap();
        let len = client.get_length("", &hash).await.unwrap();

        // Assert
        assert_eq!(len as usize, gen_length);
    }

    #[tokio::test]
    async fn test_missing_xorb() {
        // Arrange
        let client = LocalClient::default();
        let (hash, _, _) = gen_dummy_xorb(16, 2048, true);

        // Act & Assert
        let result = client.get("", &hash).await;
        assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));
    }

    #[tokio::test]
    async fn test_failures() {
        let client = LocalClient::default();
        let hello = "hello world".as_bytes().to_vec();

        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        // write "hello world"
        client
            .put("key", &hello_hash, hello.clone(), vec![hello.len() as u64])
            .await
            .unwrap();

        // put the same value a second time. This should be ok.
        client
            .put("key", &hello_hash, hello.clone(), vec![hello.len() as u64])
            .await
            .unwrap();

        // we can list all entries
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r,
            vec![Key {
                prefix: "key".into(),
                hash: hello_hash
            }]
        );

        // put the different value with the same hash
        // this should fail
        assert_eq!(
            CasClientError::HashMismatch,
            client
                .put(
                    "key",
                    &hello_hash,
                    "hellp world".as_bytes().to_vec(),
                    vec![hello.len() as u64],
                )
                .await
                .unwrap_err()
        );
        // content shorter than the chunk boundaries should fail
        assert_eq!(
            CasClientError::InvalidArguments,
            client
                .put(
                    "key",
                    &hello_hash,
                    "hellp wod".as_bytes().to_vec(),
                    vec![hello.len() as u64],
                )
                .await
                .unwrap_err()
        );

        // content longer than the chunk boundaries should fail
        assert_eq!(
            CasClientError::InvalidArguments,
            client
                .put(
                    "key",
                    &hello_hash,
                    "hello world again".as_bytes().to_vec(),
                    vec![hello.len() as u64],
                )
                .await
                .unwrap_err()
        );

        // empty writes should fail
        assert_eq!(
            CasClientError::InvalidArguments,
            client
                .put("key", &hello_hash, vec![], vec![],)
                .await
                .unwrap_err()
        );

        // compute a hash of something we do not have in the store
        let world = "world".as_bytes().to_vec();
        let world_hash = merklehash::compute_data_hash(&world[..]);

        // get length of non-existant object should fail with XORBNotFound
        assert_eq!(
            CasClientError::XORBNotFound(world_hash),
            client.get_length("key", &world_hash).await.unwrap_err()
        );

        // read of non-existant object should fail with XORBNotFound
        assert!(client.get("key", &world_hash).await.is_err());
        // read range of non-existant object should fail with XORBNotFound
        assert!(client
            .get_object_range("key", &world_hash, vec![(0, 5)])
            .await
            .is_err());

        // we can delete non-existant things
        client.delete("key", &world_hash);

        // delete the entry we inserted
        client.delete("key", &hello_hash);
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 0);

        // now every read of that key should fail
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client.get_length("key", &hello_hash).await.unwrap_err()
        );
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client.get("key", &hello_hash).await.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_hashing() {
        let client = LocalClient::default();
        // hand construct a tree of 2 chunks
        let hello = "hello".as_bytes().to_vec();
        let world = "world".as_bytes().to_vec();
        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        let world_hash = merklehash::compute_data_hash(&world[..]);

        let hellonode = merkledb::MerkleNode::new(0, hello_hash, 5, vec![]);
        let worldnode = merkledb::MerkleNode::new(1, world_hash, 5, vec![]);

        let final_hash = merkledb::detail::hash_node_sequence(&[hellonode, worldnode]);

        // insert should succeed
        client
            .put(
                "key",
                &final_hash,
                "helloworld".as_bytes().to_vec(),
                vec![5, 10],
            )
            .await
            .unwrap();
    }

    fn gen_dummy_xorb(
        num_chunks: u32,
        uncompressed_chunk_size: u32,
        randomize_chunk_sizes: bool,
    ) -> (DataHash, Vec<u8>, Vec<u64>) {
        let mut contents = Vec::new();
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut chunk_boundaries = Vec::with_capacity(num_chunks as usize);
        for _idx in 0..num_chunks {
            let chunk_size: u32 = if randomize_chunk_sizes {
                let mut rng = rand::thread_rng();
                rng.gen_range(1024..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);

            chunks.push(Chunk {
                hash: merklehash::compute_data_hash(&bytes),
                length: bytes.len(),
            });

            contents.extend(bytes);
            chunk_boundaries.push(contents.len() as u64);
        }

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);
        let hash = *ret.hash();

        (hash, contents, chunk_boundaries)
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }
}
