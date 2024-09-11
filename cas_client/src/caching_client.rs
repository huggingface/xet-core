use crate::error::Result;
use crate::interface::Client;
use crate::{client_adapter::ClientRemoteAdapter, error::CasClientError};
use async_trait::async_trait;
use cache::{Remote, XorbCache};
use cas::key::Key;
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

pub const DEFAULT_BLOCK_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct CachingClient<T: Client + Debug + Sync + Send + 'static> {
    client: Arc<T>,
    cache: Arc<dyn XorbCache>,
    xorb_lengths: Arc<Mutex<HashMap<MerkleHash, u64>>>,
}

impl<T: Client + Debug + Sync + Send + 'static> CachingClient<T> {
    /// Create a new caching client.
    /// client: This is the client object used to satisfy requests
    pub fn new(
        client: T,
        cache_path: &Path,
        capacity_bytes: u64,
        block_size: u64,
    ) -> Result<CachingClient<T>> {
        // convert Path to String
        let canonical_path = cache_path.canonicalize().map_err(|e| {
            CasClientError::ConfigurationError(format!("Error specifying cache path: {e}"))
        })?;

        let canonical_string_path = canonical_path.to_str().ok_or_else(|| {
            CasClientError::ConfigurationError("Error parsing cache path to UTF-8 path.".to_owned())
        })?;

        let arcclient = Arc::new(client);
        let client_remote_arc: Arc<dyn Remote> =
            Arc::new(ClientRemoteAdapter::new(arcclient.clone()));

        let cache = cache::from_config::<CasClientError>(
            cache::CacheConfig {
                cache_dir: canonical_string_path.to_string(),
                capacity: capacity_bytes,
                block_size,
            },
            client_remote_arc,
        )?;

        info!(
            "Creating CachingClient, path={:?}, byte capacity={}, blocksize={:?}",
            cache_path, capacity_bytes, block_size
        );

        Ok(CachingClient {
            client: arcclient,
            cache,
            xorb_lengths: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl<T: Client + Debug + Sync + Send> Client for CachingClient<T> {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        // puts write through
        debug!(
            "CachingClient put to {}/{} of length {} bytes",
            prefix,
            hash,
            data.len()
        );
        Ok(self
            .client
            .put(prefix, hash, data, chunk_boundaries)
            .await?)
    }

    async fn flush(&self) -> Result<()> {
        // forward flush to the underlying client
        Ok(self.client.flush().await?)
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        // get the length, reduce to range read of the entire length.
        debug!("CachingClient Get of {}/{}", prefix, hash);
        let xorb_size = self
            .get_length(prefix, hash)
            .await
            .debug_error("CachingClient Get: get_length reported error")?;

        debug!("CachingClient Get: get_length call succeeded with value {xorb_size}.");

        self.get_object_range(prefix, hash, vec![(0, xorb_size)])
            .await
            .map(|mut v| v.swap_remove(0))
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        debug!(
            "CachingClient GetObjectRange of {}/{}: {:?}",
            prefix, hash, ranges
        );
        let mut ret: Vec<Vec<u8>> = Vec::new();
        for (start, end) in ranges {
            let prefix_str = prefix.to_string();
            ret.push(
                self.cache
                    .fetch_xorb_range(
                        &Key {
                            prefix: prefix_str,
                            hash: *hash,
                        },
                        Range { start, end },
                        None,
                    )
                    .await
                    .warn_error(format!(
                        "CachingClient Error on GetObjectRange of {}/{}",
                        prefix, hash
                    ))?,
            )
        }
        Ok(ret)
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        debug!("CachingClient GetLength of {}/{}", prefix, hash);
        {
            // check the xorb length cache
            let xorb_lengths = self.xorb_lengths.lock().await;
            if let Some(l) = xorb_lengths.get(hash) {
                return Ok(*l);
            }
            // release lock here since get_length may take a while
        }
        let ret = self.client.get_length(prefix, hash).await;

        if let Ok(l) = ret {
            // insert it into the xorb length cache
            let mut xorb_lengths = self.xorb_lengths.lock().await;
            xorb_lengths.insert(*hash, l);
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_BLOCK_SIZE;
    use crate::*;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn path_has_files(path: &Path) -> bool {
        fs::read_dir(path).unwrap().count() > 0
    }

    #[tokio::test]
    async fn test_basic_read_write() {
        let client = Arc::new(LocalClient::default());
        let cachedir = TempDir::new().unwrap();
        assert!(!path_has_files(cachedir.path()));

        let client = CachingClient::new(client, cachedir.path(), 100, DEFAULT_BLOCK_SIZE).unwrap();

        // the root hash of a single chunk is just the hash of the data
        let hello = "hello world".as_bytes().to_vec();
        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        // write "hello world"
        client
            .put("key", &hello_hash, hello.clone(), vec![hello.len() as u64])
            .await
            .unwrap();

        // get length "hello world"
        assert_eq!(11, client.get_length("key", &hello_hash).await.unwrap());

        // read "hello world"
        assert_eq!(hello, client.get("key", &hello_hash).await.unwrap());

        // read range "hello" and "world"
        let ranges_to_read: Vec<(u64, u64)> = vec![(0, 5), (6, 11)];
        let expected: Vec<Vec<u8>> = vec!["hello".as_bytes().to_vec(), "world".as_bytes().to_vec()];
        assert_eq!(
            expected,
            client
                .get_object_range("key", &hello_hash, ranges_to_read)
                .await
                .unwrap()
        );
        // read range "hello" and "world", with truncation for larger offsets
        let ranges_to_read: Vec<(u64, u64)> = vec![(0, 5), (6, 20)];
        let expected: Vec<Vec<u8>> = vec!["hello".as_bytes().to_vec(), "world".as_bytes().to_vec()];
        assert_eq!(
            expected,
            client
                .get_object_range("key", &hello_hash, ranges_to_read)
                .await
                .unwrap()
        );
        // empty read
        let ranges_to_read: Vec<(u64, u64)> = vec![(0, 5), (6, 6)];
        let expected: Vec<Vec<u8>> = vec!["hello".as_bytes().to_vec(), "".as_bytes().to_vec()];
        assert_eq!(
            expected,
            client
                .get_object_range("key", &hello_hash, ranges_to_read)
                .await
                .unwrap()
        );
        assert!(path_has_files(cachedir.path()));
    }

    #[tokio::test]
    async fn test_failures() {
        let client = Arc::new(LocalClient::default());
        let cachedir = TempDir::new().unwrap();
        assert!(!path_has_files(cachedir.path()));

        let client = CachingClient::new(client, cachedir.path(), 100, DEFAULT_BLOCK_SIZE).unwrap();

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
        assert_eq!(
            CasClientError::XORBNotFound(world_hash),
            client.get("key", &world_hash).await.unwrap_err()
        );
        // read range of non-existant object should fail with XORBNotFound
        assert!(client
            .get_object_range("key", &world_hash, vec![(0, 5)])
            .await
            .is_err());
    }
}
