use anyhow::anyhow;
use async_trait::async_trait;
use merklehash::MerkleHash;
use parutils::{tokio_par_for_each, ParallelError};
use progress_reporting::DataProgressReporter;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::CasClientError;
use crate::interface::Client;
use crate::local_client::LocalClient;
use crate::staging_trait::*;
use crate::PassthroughStagingClient;

#[derive(Debug)]
pub struct StagingClient {
    client: Arc<dyn Client + Sync + Send>,
    staging_client: LocalClient,
    progressbar: bool,
}

impl StagingClient {
    /// Create a new staging client which wraps a remote client.
    ///
    /// stage_path is the staging directory.
    ///
    /// Reads will check both the staging environment as well as the
    /// the remote client. Puts will write to only staging environment
    /// until upload `upload_all_staged()` is called.
    ///
    /// Staging environment is fully persistent and resilient to restarts.
    pub fn new(client: Arc<dyn Client + Sync + Send>, stage_path: &Path) -> StagingClient {
        StagingClient {
            client,
            staging_client: LocalClient::new(stage_path, true), // silence warnings=true
            progressbar: false,
        }
    }

    /// Create a new staging client which wraps a remote client.
    ///
    /// stage_path is the staging directory.
    ///
    /// Reads will check both the staging environment as well as the
    /// the remote client. Puts will write to only staging environment
    /// until upload `upload_all_staged()` is called.
    ///
    /// Staging environment is fully persistent and resilient to restarts.
    /// This version of the constructor will display a progressbar to stderr
    /// when `upload_all_staged()` is called
    pub fn new_with_progressbar(
        client: Arc<dyn Client + Sync + Send>,
        stage_path: &Path,
    ) -> StagingClient {
        StagingClient {
            client,
            staging_client: LocalClient::new(stage_path, true), // silence warnings=true
            progressbar: true,
        }
    }
}

fn cas_staging_bypass_is_set() -> bool {
    // Returns true if XET_CAS_BYPASS_STAGING is set to something besides "0"
    std::env::var_os("XET_CAS_BYPASS_STAGING")
        .filter(|v| v != "0")
        .is_some()
}

/// Creates a new staging client wraping a staging directory.
/// If a staging directory is provided, it will be used for staging.
/// Otherwise all queries are passed through to the remote directly
/// using the PassthroughStagingClient.
pub fn new_staging_client<T: Client + Debug + Sync + Send + 'static>(
    client: T,
    stage_path: Option<&Path>,
) -> Arc<dyn Staging + Send + Sync> {
    if let (false, Some(path)) = (cas_staging_bypass_is_set(), stage_path) {
        Arc::new(StagingClient::new(Arc::new(client), path))
    } else {
        Arc::new(PassthroughStagingClient::new(Arc::new(client)))
    }
}

/// Creates a new staging client wraping a staging directory.
/// If a staging directory is provided, it will be used for staging.
/// Otherwise all queries are passed through to the remote directly
/// using the PassthroughStagingClient.
pub fn new_staging_client_with_progressbar<T: Client + Debug + Sync + Send + 'static>(
    client: T,
    stage_path: Option<&Path>,
) -> Arc<dyn Staging + Send + Sync> {
    if let (false, Some(path)) = (cas_staging_bypass_is_set(), stage_path) {
        Arc::new(StagingClient::new_with_progressbar(Arc::new(client), path))
    } else {
        Arc::new(PassthroughStagingClient::new(Arc::new(client)))
    }
}

impl Staging for StagingClient {}

#[async_trait]
impl StagingUpload for StagingClient {
    /// Upload all staged will upload everything to the remote client.
    /// TODO : Caller may need to be wary of a HashMismatch error which will
    /// indicate that the local staging environment has been corrupted somehow.
    async fn upload_all_staged(
        &self,
        max_concurrent: usize,
        retain: bool,
    ) -> Result<(), CasClientError> {
        let client = &self.client;
        let stage = &self.staging_client;
        let entries = stage.get_all_entries()?;
        info!(
            "XET StagingClient: {} entries to upload to remote.",
            entries.len()
        );

        let pb = if self.progressbar && !entries.is_empty() {
            let pb =
                DataProgressReporter::new("Xet: Uploading data blocks", Some(entries.len()), None);

            pb.register_progress(Some(0), Some(0)); // draw the bar immediately

            Some(Arc::new(Mutex::new(pb)))
        } else {
            None
        };
        let cur_span = info_span!("staging_client.upload_all_staged");
        let ctx = cur_span.context();
        // TODO: This can probably be re-written cleaner with futures::stream
        // ex: https://patshaughnessy.net/2020/1/20/downloading-100000-files-using-async-rust
        tokio_par_for_each(entries, max_concurrent, |entry, _| {
            let pb = pb.clone();
            let span = info_span!("upload_staged_xorb");
            span.set_parent(ctx.clone());
            async move {
                // if remote does not have the object
                // read the object from staging
                // and write the object out to remote
                let (cb, val) = stage
                    .get_detailed(&entry.prefix, &entry.hash)
                    .instrument(info_span!("read_staged"))
                    .await?;
                let xorb_length = val.len();
                info!(
                    "Uploading XORB {}/{} of length {}.",
                    &entry.prefix,
                    &entry.hash,
                    val.len()
                );
                client.put(&entry.prefix, &entry.hash, val, cb).await?;

                if !retain {
                    info!(
                        "Clearing XORB {}/{} from staging area.",
                        &entry.prefix, &entry.hash,
                    );
                    // Delete it from staging
                    stage.delete(&entry.prefix, &entry.hash);
                }
                if let Some(bar) = &pb {
                    bar.lock()
                        .await
                        .register_progress(Some(1), Some(xorb_length));
                }
                Ok(())
            }
            .instrument(span)
        })
        .instrument(cur_span)
        .await
        .map_err(|e| match e {
            ParallelError::JoinError => CasClientError::InternalError(anyhow!("Join Error")),
            ParallelError::TaskError(e) => e,
        })?;
        self.client.flush().await?;

        if let Some(bar) = &pb {
            bar.lock().await.finalize();
        }

        Ok(())
    }
}

#[async_trait]
impl StagingBypassable for StagingClient {
    async fn put_bypass_stage(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError> {
        self.client.put(prefix, hash, data, chunk_boundaries).await
    }
}

#[async_trait]
impl StagingInspect for StagingClient {
    async fn list_all_staged(&self) -> Result<Vec<String>, CasClientError> {
        let stage = &self.staging_client;
        let items = stage
            .get_all_entries()?
            .iter()
            .map(|item: &cas::key::Key| item.to_string())
            .collect();

        Ok(items)
    }

    async fn get_length_staged(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError> {
        let stage = &self.staging_client;
        let item = stage.get_detailed(prefix, hash).await?;

        Ok(item.1.len())
    }

    async fn get_length_remote(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError> {
        let item = self.client.get_length(prefix, hash).await?;

        Ok(item as usize)
    }

    fn get_staging_path(&self) -> PathBuf {
        self.staging_client.path.clone()
    }

    fn get_staging_size(&self) -> Result<usize, CasClientError> {
        self.staging_client
            .path
            .read_dir()
            .map_err(|x| CasClientError::InternalError(x.into()))?
            // take only entries which are ok
            .filter_map(|x| x.ok())
            // take only entries whose filenames convert into strings
            .filter(|x| {
                let mut is_ok = false;
                if let Ok(name) = x.file_name().into_string() {
                    if let Some(pos) = name.rfind('.') {
                        is_ok = MerkleHash::from_hex(&name[(pos + 1)..]).is_ok()
                    }
                }

                is_ok
            })
            .try_fold(0, |acc, file| {
                let file = file;
                let size = match file.metadata() {
                    Ok(data) => data.len() as usize,
                    _ => 0,
                };
                Ok(acc + size)
            })
    }
}

#[async_trait]
impl Client for StagingClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError> {
        self.staging_client
            .put(prefix, hash, data, chunk_boundaries)
            .instrument(info_span!("staging_client.put"))
            .await
    }

    async fn flush(&self) -> Result<(), CasClientError> {
        // forward flush to the underlying clients
        self.staging_client.flush().await?;
        self.client.flush().await
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>, CasClientError> {
        match self
            .staging_client
            .get(prefix, hash)
            .instrument(info_span!("staging_client.get"))
            .await
        {
            Err(CasClientError::XORBNotFound(_)) => self.client.get(prefix, hash).await,
            x => x,
        }
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>, CasClientError> {
        match self
            .staging_client
            .get_object_range(prefix, hash, ranges.clone())
            .instrument(info_span!("staging_client.get_range"))
            .await
        {
            Err(CasClientError::XORBNotFound(_)) => {
                self.client.get_object_range(prefix, hash, ranges).await
            }
            x => x,
        }
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64, CasClientError> {
        match self
            .staging_client
            .get_length(prefix, hash)
            .instrument(info_span!("staging_client.get_length"))
            .await
        {
            Err(CasClientError::XORBNotFound(_)) => self.client.get_length(prefix, hash).await,
            x => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::staging_client::{StagingClient, StagingUpload};
    use crate::*;

    fn make_staging_client(_client_path: &Path, stage_path: &Path) -> StagingClient {
        let client = LocalClient::default();
        StagingClient::new(Arc::new(client), stage_path)
    }

    #[tokio::test]
    async fn test_general_basic_read_write() {
        let localdir = TempDir::new().unwrap();
        let stagedir = TempDir::new().unwrap();
        let client = make_staging_client(localdir.path(), stagedir.path());

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
    async fn test_general_failures() {
        let localdir = TempDir::new().unwrap();
        let stagedir = TempDir::new().unwrap();
        let client = make_staging_client(localdir.path(), stagedir.path());

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
        assert!(client.get_length("key", &world_hash).await.is_err());

        // read of non-existant object should fail with XORBNotFound
        assert!(client.get("key", &world_hash).await.is_err());
        // read range of non-existant object should fail with XORBNotFound
        assert_eq!(
            CasClientError::XORBNotFound(world_hash),
            client
                .get_object_range("key", &world_hash, vec![(0, 5)])
                .await
                .unwrap_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_staged_read_write() {
        let localdir = TempDir::new().unwrap();
        let stagedir = TempDir::new().unwrap();
        let client = make_staging_client(localdir.path(), stagedir.path());

        // put an object in and make sure it is there

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

        // check that the underlying client does not actually have it
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client.client.get("key", &hello_hash).await.unwrap_err()
        );

        // upload staged
        client.upload_all_staged(1, false).await.unwrap();

        // we can still read it
        // get length "hello world"
        assert_eq!(11, client.get_length("key", &hello_hash).await.unwrap());
        // read "hello world"
        assert_eq!(hello, client.get("key", &hello_hash).await.unwrap());

        // underlying client has it now
        assert_eq!(hello, client.client.get("key", &hello_hash).await.unwrap());

        // staging client does not
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client
                .staging_client
                .get("key", &hello_hash)
                .await
                .unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_passthrough() {
        let localdir = TempDir::new().unwrap();
        let local = LocalClient::new(localdir.path(), true);
        // no staging directory
        let client = new_staging_client(local, None);

        // put an object in and make sure it is there

        // the root hash of a single chunk is just the hash of the data
        let hello = "hello world".as_bytes().to_vec();
        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        // write "hello world"
        client
            .put("key", &hello_hash, hello.clone(), vec![hello.len() as u64])
            .await
            .unwrap();
        client.flush().await.unwrap();
        // get length "hello world"
        assert_eq!(11, client.get_length("key", &hello_hash).await.unwrap());
        // read "hello world"
        assert_eq!(hello, client.get("key", &hello_hash).await.unwrap());

        // since there is no stage. get_length_staged should fail.
        assert_eq!(
            CasClientError::XORBNotFound(hello_hash),
            client
                .get_length_staged("key", &hello_hash)
                .await
                .unwrap_err()
        );

        // check that the underlying client has it!
        // (this is a passthrough!)
        // but we can't get it from the stage object (it is now a Box)
        // so we make a new local client at the same directory
        let local2 = LocalClient::new(localdir.path(), true);
        assert_eq!(hello, local2.get("key", &hello_hash).await.unwrap());
    }
}
