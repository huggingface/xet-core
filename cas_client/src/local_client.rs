use crate::error::{CasClientError, Result};
use crate::interface::Client;
use anyhow::anyhow;
use async_trait::async_trait;
use cas::key::Key;
use merkledb::prelude::*;
use merkledb::{Chunk, MerkleMemDB};
use merklehash::MerkleHash;
use std::fs::{metadata, File};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
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

fn read_io_to_cas_err(path: &PathBuf, e: std::io::Error) -> CasClientError {
    CasClientError::InternalError(anyhow!("Unable to read contents of {:?}. {:?}", path, e))
}
fn write_io_to_cas_err(path: &PathBuf, e: std::io::Error) -> CasClientError {
    CasClientError::InternalError(anyhow!("Unable to write contents of {:?}. {:?}", path, e))
}

impl LocalClient {
    /// Creates a local client that writes to a particular specified path.
    /// Files preexisting in the path may be used to serve queries.
    pub fn new(path: &Path, silence_errors: bool) -> LocalClient {
        LocalClient {
            tempdir: None,
            path: path.to_path_buf(),
            silence_errors,
        }
    }

    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, prefix: &str, hash: &MerkleHash) -> PathBuf {
        self.path.join(format!("{}.{}", prefix, hash.hex()))
    }

    /// File format handling Functions
    ///
    /// The local disk format for each Xorb is:
    ///  HEADER
    ///  - u64: Version Number
    ///  - u64: data len bytes
    ///  - u64: chunk boundary in bytes length
    ///
    ///  - all the data
    ///  - chunk_boundaries as bincode
    ///
    const HEADER_LEN: u64 = 24;
    const HEADER_VERSION: u64 = 0;

    /// Reads a u64 in little endian form from a file
    fn read_u64(file: &mut impl Read) -> std::io::Result<u64> {
        let mut val = [0u8; 8];
        file.read_exact(&mut val)?;
        Ok(u64::from_le_bytes(val))
    }

    /// Writes a u64 in little endian form to a file
    fn write_u64(file: &mut impl Write, val: u64) -> std::io::Result<()> {
        file.write_all(&val.to_le_bytes())
    }

    /// Returns the length and the size of the of the chunk boundary object
    fn read_header(file: &mut impl Read) -> std::io::Result<(u64, u64)> {
        let version = LocalClient::read_u64(file)?;
        if version != LocalClient::HEADER_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid File Version",
            ));
        }
        let data_len = LocalClient::read_u64(file)?;
        let chunkboundary_len = LocalClient::read_u64(file)?;
        Ok((data_len, chunkboundary_len))
    }

    /// Returns the length and the size of the of the chunk boundary object
    fn write_header(
        file: &mut impl Write,
        data_len: u64,
        chunkboundary_len: u64,
    ) -> std::io::Result<()> {
        LocalClient::write_u64(file, LocalClient::HEADER_VERSION)?;
        LocalClient::write_u64(file, data_len)?;
        LocalClient::write_u64(file, chunkboundary_len)?;
        Ok(())
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

        // read the data length and the chunk boundary length
        let (data_len, chunkboundary_len) =
            LocalClient::read_header(&mut reader).map_err(|x| read_io_to_cas_err(&file_path, x))?;

        // deserialize the chunk boundary
        let mut chunk_boundary_buf = vec![0u8; chunkboundary_len as usize];
        reader
            .read_exact(&mut chunk_boundary_buf)
            .map_err(|x| read_io_to_cas_err(&file_path, x))?;
        let chunk_boundaries: Vec<u64> =
            bincode::deserialize(&chunk_boundary_buf).map_err(|_| {
                CasClientError::InternalError(anyhow!("Invalid deserialization {:?}", file_path))
            })?;

        let mut data = vec![0u8; data_len as usize];
        reader
            .read_exact(&mut data)
            .map_err(|x| read_io_to_cas_err(&file_path, x))?;
        Ok((chunk_boundaries, data))
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

/// The local client stores Xorbs on local disk.
#[async_trait]
impl Client for LocalClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        let file_path = self.get_path_for_entry(prefix, hash);

        info!("Writing XORB {prefix}/{hash:?} to local path {file_path:?}");
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
        if !validate_root_hash(&data, &chunk_boundaries, hash) {
            return Err(CasClientError::HashMismatch);
        }
        if let Ok(xorb_size) = self.get_length(prefix, hash).await {
            if xorb_size > 0 {
                info!("{prefix:?}/{hash:?} already exists in Local CAS; returning.");
                return Ok(());
            }
        }
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

        let chunk_boundaries_bytes: Vec<u8> = bincode::serialize(&chunk_boundaries)?;

        {
            let mut writer = BufWriter::new(&tempfile);
            LocalClient::write_header(
                &mut writer,
                data.len() as u64,
                chunk_boundaries_bytes.len() as u64,
            )
            .map_err(|x| write_io_to_cas_err(&file_path, x))?;

            // write out chunk boundaries then bytes
            writer
                .write_all(&chunk_boundaries_bytes)
                .map_err(|x| write_io_to_cas_err(&file_path, x))?;

            writer
                .write_all(&data[..])
                .map_err(|x| write_io_to_cas_err(&file_path, x))?;

            // make sure we flush before persisting
            writer
                .flush()
                .map_err(|x| write_io_to_cas_err(&file_path, x))?;
        }

        tempfile.persist(&file_path)?;

        // attempt to set to readonly
        // its ok to fail.
        if let Ok(metadata) = std::fs::metadata(&file_path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(true);
            let _ = std::fs::set_permissions(&file_path, permissions);
        }

        info!("{file_path:?} successfully written.");

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // this client does not background so no flush is needed
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        Ok(self.get_detailed(prefix, hash).await?.1)
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

        let mut file = File::open(&file_path).map_err(|_| {
            if !self.silence_errors {
                error!("Unable to find file in local CAS {:?}", file_path);
            }
            CasClientError::XORBNotFound(*hash)
        })?;

        // read the data length and the chunk boundary length
        let (data_len, chunkboundary_len) =
            LocalClient::read_header(&mut file).map_err(|x| read_io_to_cas_err(&file_path, x))?;

        // calculate where the data starts:
        // Its just the header + chunkboundary bytes
        let starting_offset = LocalClient::HEADER_LEN + chunkboundary_len;

        let mut ret: Vec<Vec<u8>> = Vec::new();
        for r in ranges {
            let mut start = r.0;
            let mut end = r.1;
            if start >= data_len {
                start = data_len
            }
            if end > data_len {
                end = data_len
            }
            // end before start, or any position is outside of array
            if end < start {
                return Err(CasClientError::InvalidRange);
            }
            let mut data = vec![0u8; (end - start) as usize];
            if end - start > 0 {
                file.seek(std::io::SeekFrom::Start(starting_offset + start))
                    .map_err(|x| read_io_to_cas_err(&file_path, x))?;

                file.read_exact(&mut data)
                    .map_err(|x| read_io_to_cas_err(&file_path, x))?;
            }
            ret.push(data);
        }
        Ok(ret)
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        let file_path = self.get_path_for_entry(prefix, hash);
        match File::open(&file_path) {
            Ok(mut file) => {
                let (len, _) = LocalClient::read_header(&mut file)
                    .map_err(|x| read_io_to_cas_err(&file_path, x))?;
                Ok(len)
            }
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cas::key::Key;
    use merkledb::detail::hash_node_sequence;
    use merkledb::MerkleNode;

    #[tokio::test]
    async fn test_basic_read_write() {
        let client = LocalClient::default();
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

        let hellonode = MerkleNode::new(0, hello_hash, 5, vec![]);
        let worldnode = MerkleNode::new(1, world_hash, 5, vec![]);

        let final_hash = hash_node_sequence(&[hellonode, worldnode]);

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
}
