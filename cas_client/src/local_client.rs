use crate::error::{CasClientError, Result};
use crate::interface::UploadClient;
use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::CasObject;
use cas_types::Key;
use merklehash::MerkleHash;
use std::fs::{metadata, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tracing::{debug, info};

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
}

/// LocalClient is responsible for writing/reading Xorbs on local disk.
#[async_trait]
impl UploadClient for LocalClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<()> {
        // no empty writes
        if chunk_and_boundaries.is_empty() || data.is_empty() {
            return Err(CasClientError::InvalidArguments);
        }

        // last boundary must be end of data
        if chunk_and_boundaries.last().unwrap().1 as usize != data.len() {
            return Err(CasClientError::InvalidArguments);
        }

        // moved hash validation into [CasObject::serialize], so removed from here.

        if self.exists(prefix, hash).await? {
            info!("{prefix:?}/{hash:?} already exists in Local CAS; returning.");
            return Ok(());
        }

        let file_path = self.get_path_for_entry(prefix, hash);
        info!("Writing XORB {prefix}/{hash:?} to local path {file_path:?}");

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
                &chunk_and_boundaries,
                cas_object::CompressionScheme::None,
            )?;
            // flush before persisting
            writer.flush()?;
            total_bytes_written = bytes_written;
        }

        tempfile.persist(&file_path).map_err(|e| e.error)?;

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

    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool> {
        let file_path = self.get_path_for_entry(prefix, hash);

        let res = metadata(&file_path);

        if res.is_err() {
            return Ok(false);
        }

        if !res.unwrap().is_file() {
            return Err(CasClientError::InternalError(anyhow!(
                "Attempting to write to {:?}, but it is not a file",
                file_path
            )));
        };

        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                CasObject::deserialize(&mut reader)?;
                Ok(true)
            }
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests_utils {
    use super::LocalClient;
    use crate::{error::Result, CasClientError};
    use cas_object::CasObject;
    use merklehash::MerkleHash;
    use std::{fs::File, io::BufReader};
    use tracing::error;

    pub trait TestUtils {
        fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>>;
        fn get_object_range(
            &self,
            prefix: &str,
            hash: &MerkleHash,
            ranges: Vec<(u32, u32)>,
        ) -> Result<Vec<Vec<u8>>>;
        fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u32>;
    }

    impl TestUtils for LocalClient {
        fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
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

        fn get_object_range(
            &self,
            prefix: &str,
            hash: &MerkleHash,
            ranges: Vec<(u32, u32)>,
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
            let all_uncompressed_bytes = cas.get_all_bytes(&mut reader)?;
            for r in ranges {
                let data = all_uncompressed_bytes[r.0 as usize..r.1 as usize].to_vec();
                ret.push(data);
            }
            Ok(ret)
        }

        fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u32> {
            let file_path = self.get_path_for_entry(prefix, hash);
            match File::open(file_path) {
                Ok(file) => {
                    let mut reader = BufReader::new(file);
                    let cas = CasObject::deserialize(&mut reader)?;
                    let length = cas.get_all_bytes(&mut reader)?.len();
                    Ok(length as u32)
                }
                Err(_) => Err(CasClientError::XORBNotFound(*hash)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tests_utils::TestUtils;
    use super::*;
    use cas_object::test_utils::*;
    use cas_object::CompressionScheme::LZ4;
    use merklehash::compute_data_hash;

    #[tokio::test]
    async fn test_basic_put_get() {
        // Arrange
        let client = LocalClient::default();
        let data = gen_random_bytes(2048);
        let hash = compute_data_hash(&data[..]);
        let chunk_boundaries = data.len() as u32;

        let data_again = data.clone();

        // Act & Assert
        assert!(client
            .put("key", &hash, data, vec![(hash, chunk_boundaries)])
            .await
            .is_ok());

        let returned_data = client.get("key", &hash).unwrap();
        assert_eq!(data_again, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_random_medium() {
        // Arrange
        let client = LocalClient::default();
        let (c, _, data, chunk_boundaries) =
            build_cas_object(44, ChunkSize::Random(512, 15633), LZ4);
        let data_again = data.clone();

        // Act & Assert
        assert!(client
            .put("", &c.info.cashash, data, chunk_boundaries)
            .await
            .is_ok());

        let returned_data = client.get("", &c.info.cashash).unwrap();
        assert_eq!(data_again, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_range_random_small() {
        // Arrange
        let client = LocalClient::default();
        let (c, _, data, chunk_boundaries) = build_cas_object(3, ChunkSize::Random(512, 2048), LZ4);
        let data_again = data.clone();

        // Act & Assert
        assert!(client
            .put("", &c.info.cashash, data, chunk_boundaries)
            .await
            .is_ok());

        let ranges: Vec<(u32, u32)> = vec![(0, 100), (100, 1500)];
        let ranges_again = ranges.clone();
        let returned_ranges = client
            .get_object_range("", &c.info.cashash, ranges)
            .unwrap();

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
        let (c, _, data, chunk_boundaries) = build_cas_object(1, ChunkSize::Fixed(2048), LZ4);
        let gen_length = data.len();

        // Act
        client
            .put("", &c.info.cashash, data, chunk_boundaries)
            .await
            .unwrap();
        let len = client.get_length("", &c.info.cashash).unwrap();

        // Assert
        assert_eq!(len as usize, gen_length);
    }

    #[tokio::test]
    async fn test_missing_xorb() {
        // Arrange
        let client = LocalClient::default();
        let hash = MerkleHash::from_hex(
            "d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da",
        )
        .unwrap();

        // Act & Assert
        let result = client.get("", &hash);
        assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));
    }

    #[tokio::test]
    async fn test_failures() {
        let client = LocalClient::default();
        let hello = "hello world".as_bytes().to_vec();

        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        // write "hello world"
        client
            .put(
                "key",
                &hello_hash,
                hello.clone(),
                vec![(hello_hash, hello.len() as u32)],
            )
            .await
            .unwrap();

        // put the same value a second time. This should be ok.
        client
            .put(
                "key",
                &hello_hash,
                hello.clone(),
                vec![(hello_hash, hello.len() as u32)],
            )
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

        // content shorter than the chunk boundaries should fail
        assert_eq!(
            CasClientError::InvalidArguments,
            client
                .put(
                    "hellp2",
                    &hello_hash,
                    "hellp wod".as_bytes().to_vec(),
                    vec![(hello_hash, hello.len() as u32)],
                )
                .await
                .unwrap_err()
        );

        // content longer than the chunk boundaries should fail
        assert_eq!(
            CasClientError::InvalidArguments,
            client
                .put(
                    "again",
                    &hello_hash,
                    "hello world again".as_bytes().to_vec(),
                    vec![(hello_hash, hello.len() as u32)],
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
            client.get_length("key", &world_hash).unwrap_err()
        );

        // read of non-existant object should fail with XORBNotFound
        assert!(client.get("key", &world_hash).is_err());
        // read range of non-existant object should fail with XORBNotFound
        assert!(client
            .get_object_range("key", &world_hash, vec![(0, 5)])
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
            client.get_length("key", &hello_hash).unwrap_err()
        );
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client.get("key", &hello_hash).unwrap_err()
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
                vec![(hello_hash, 5), (world_hash, 10)],
            )
            .await
            .unwrap();
    }
}
