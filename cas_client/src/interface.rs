use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cas_object::SerializedCasObject;
use cas_types::{FileRange, QueryReconstructionResponse};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;

use crate::error::Result;
use crate::CasClientError;

/// A Client to the CAS (Content Addressed Storage) service to allow storage and
/// management of XORBs (Xet Object Remote Block). A XORB represents a collection
/// of arbitrary bytes. These bytes are hashed according to a Xet Merkle Hash
/// producing a Merkle Tree. XORBs in the CAS are identified by a combination of
/// a prefix namespacing the XORB and the hash at the root of the Merkle Tree.
#[async_trait]
pub trait UploadClient {
    /// Insert a serialized XORB into the CAS, returning the number of bytes read.  See   
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
    ) -> Result<u64>;

    /// Check if a XORB already exists.
    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_xorb_footer(&self) -> bool;
}

/// A Client to the CAS (Content Addressed Storage) service to allow reconstructing a
/// pointer file based on FileID (MerkleHash).
///
/// To simplify this crate, it is intentional that the client does not create its own http_client or
/// spawn its own threads. Instead, it is expected to be given the parallelism harness/threadpool/queue
/// on which it is expected to run. This allows the caller to better optimize overall system utilization
/// by controlling the number of concurrent requests.
#[async_trait]
pub trait ReconstructionClient {
    /// Get an entire file by file hash with an optional bytes range.
    ///
    /// The http_client passed in is a non-authenticated client. This is used to directly communicate
    /// with the backing store (S3) to retrieve xorbs.
    async fn get_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    async fn batch_get_file(&self, files: HashMap<MerkleHash, &OutputProvider>) -> Result<u64> {
        let mut n_bytes = 0;
        // Provide the basic naive implementation as a default.
        for (h, w) in files {
            n_bytes += self.get_file(&h, None, w, None).await?;
        }
        Ok(n_bytes)
    }
}

/// Enum of different output formats to write reconstructed files.
#[derive(Debug, Clone)]
pub enum OutputProvider {
    File(FileProvider),
    #[cfg(test)]
    Buffer(buffer::BufferProvider),
}

impl OutputProvider {
    /// Create a new writer to start writing at the indicated start location.
    pub(crate) fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        match self {
            OutputProvider::File(fp) => fp.get_writer_at(start),
            #[cfg(test)]
            OutputProvider::Buffer(bp) => bp.get_writer_at(start),
        }
    }
}

/// Provides new Writers to a file located at a particular location
#[derive(Debug, Clone)]
pub struct FileProvider {
    filename: PathBuf,
}

impl FileProvider {
    pub fn new(filename: PathBuf) -> Self {
        Self { filename }
    }

    fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .create(true)
            .open(&self.filename)?;
        file.seek(SeekFrom::Start(start))?;
        Ok(Box::new(file))
    }
}

/// A Client to the CAS (Content Addressed Storage) service that is able to obtain
/// the reconstruction info of a file by FileID (MerkleHash). Return
/// - Ok(Some(response)) if the query succeeded,
/// - Ok(None) if the specified range can't be satisfied,
/// - Err(e) for other errors.
#[async_trait]
pub trait Reconstructable {
    async fn get_reconstruction(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>>;
}

/// Probes for shards that provide dedup information for a chunk, and, if
/// any are found, writes them to disk and returns the path.
#[async_trait]
pub trait ShardDedupProber {
    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &MerkleHash,
        salt: &[u8; 32],
    ) -> Result<Option<PathBuf>>;
}

#[async_trait]
pub trait RegistrationClient {
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: &[u8],
        salt: &[u8; 32],
    ) -> Result<bool>;
}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
pub trait ShardClientInterface:
    RegistrationClient + FileReconstructor<CasClientError> + ShardDedupProber + Send + Sync
{
}

pub trait Client: UploadClient + ReconstructionClient + ShardClientInterface {}

#[cfg(test)]
pub mod buffer {
    use std::io::Cursor;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default, Clone)]
    pub struct BufferProvider {
        pub buf: ThreadSafeBuffer,
    }

    impl BufferProvider {
        pub fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
            let mut buffer = self.buf.clone();
            buffer.idx = start;
            Ok(Box::new(buffer))
        }
    }

    #[derive(Debug, Default, Clone)]
    /// Thread-safe in-memory buffer that implements [Write](Write) trait at some position
    /// within an underlying buffer and allows access to inner buffer.
    /// Thread-safe in-memory buffer that implements [Write](Write) trait and allows
    /// access to inner buffer
    pub struct ThreadSafeBuffer {
        idx: u64,
        inner: Arc<Mutex<Cursor<Vec<u8>>>>,
    }
    impl ThreadSafeBuffer {
        pub fn value(&self) -> Vec<u8> {
            self.inner.lock().unwrap().get_ref().clone()
        }
    }

    impl Write for ThreadSafeBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.inner.lock().map_err(|e| std::io::Error::other(format!("{e}")))?;
            guard.set_position(self.idx);
            let num_written = guard.write(buf)?;
            self.idx = guard.position();
            Ok(num_written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
