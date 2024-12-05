use std::sync::Arc;

use cas_client::Client;
use futures::StreamExt;
use mdb_shard::{
    cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo},
    ShardFileManager,
};
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;
use tokio::task::JoinHandle;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot, Mutex,
    },
};
use utils::ThreadPool;

use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::data_processing::CASDataAggregator;
use crate::errors::{DataProcessingError::*, *};

pub enum QueueItem<T: Send, S: Send> {
    Value(T),
    Flush(S),
}
type XorbUploadValueType = (MerkleHash, Vec<u8>, Vec<(MerkleHash, usize)>);
type XorbUploadSignalType = oneshot::Sender<()>;
type XorbUploadInputType = QueueItem<XorbUploadValueType, XorbUploadSignalType>;
type XorbUploadOutputType = QueueItem<(), XorbUploadSignalType>;

/// Helper to parallelize xorb upload and registration.
/// Calls to registering xorbs return immediately after computing a xorb hash so callers
/// can continue with other work, and xorb data is queued internally to be uploaded and registered.
///
/// It is critical to call [`flush`] before `ParallelXorbUploader` is dropped. Though
/// dropping will attempt to wait for all transfers to finish, any errors
/// that happen in the process of dropping will be ignored.
pub(crate) struct ParallelXorbUploader {
    // Configurations
    cas_prefix: String,

    // Utils
    shard_manager: Arc<ShardFileManager>,
    cas: Arc<dyn Client + Send + Sync>,

    // Internal worker
    xorb_data_queue: Mutex<Sender<XorbUploadInputType>>,
    upload_worker: Mutex<Option<JoinHandle<()>>>,

    // Theadpool
    threadpool: Arc<ThreadPool>,
}

impl ParallelXorbUploader {
    pub async fn new(
        cas_prefix: &str,
        shard_manager: Arc<ShardFileManager>,
        cas: Arc<dyn Client + Send + Sync>,
        threadpool: Arc<ThreadPool>,
        buffer_size: usize,
    ) -> Arc<Self> {
        let (xorb_data_p, xorb_data_c) = mpsc::channel::<XorbUploadInputType>(buffer_size);

        let uploader = Arc::new(ParallelXorbUploader {
            cas_prefix: cas_prefix.to_owned(),
            shard_manager,
            cas,
            xorb_data_queue: Mutex::new(xorb_data_p),
            upload_worker: Mutex::new(None),
            threadpool,
        });

        Self::run(uploader.clone(), xorb_data_c).await;

        uploader
    }

    async fn run(uploader: Arc<Self>, xorbs: Receiver<XorbUploadInputType>) {
        let shard_manager = uploader.shard_manager.clone();
        let cas = uploader.cas.clone();
        let cas_prefix = uploader.cas_prefix.clone();
        let upload_task = uploader.threadpool.spawn(async move {
            let stream_of_xorbs = tokio_stream::wrappers::ReceiverStream::new(xorbs);
            let mut buffered_upload = stream_of_xorbs
                .map(|item| process_xorb_data_queue_item(item, &shard_manager, &cas, &cas_prefix))
                .buffered(*MAX_CONCURRENT_XORB_UPLOADS);

            while let Some(ret) = buffered_upload.next().await {
                match ret {
                    // All xorbs added to the queue before this signal were all uploaded successfully,
                    // send out this signal.
                    Ok(QueueItem::Flush(signal)) => signal.send(()).expect("Upload flush signal error"),
                    Err(e) => {
                        panic!("Uploading and registering Xorb failed with {e}");
                    },
                    // Uploaded a xorb successfully.
                    _ => (),
                }
            }
        });
        let mut worker = uploader.upload_worker.lock().await;
        *worker = Some(upload_task);
    }

    pub async fn register_new_cas_block(&self, cas_data: CASDataAggregator) -> Result<MerkleHash> {
        self.task_is_running().await?;

        let cas_hash = cas_node_hash(&cas_data.chunks[..]);

        let sender = self.xorb_data_queue.lock().await;

        sender
            .send(QueueItem::Value((cas_hash, cas_data.data, cas_data.chunks)))
            .await
            .map_err(|e| InternalError(format!("{e}")))?;

        // Now register any new files as needed.
        for (mut fi, chunk_hash_indices) in cas_data.pending_file_info {
            for i in chunk_hash_indices {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
                fi.segments[i].cas_hash = cas_hash;
            }

            self.shard_manager.add_file_reconstruction_info(fi).await?;
        }

        Ok(cas_hash)
    }

    /// Flush makes sure all xorbs added to queue before this call are sent successfully
    /// to remote. This function can be called multiple times and should be called at
    /// least once before `ParallelXorbUploader` is dropped.
    pub async fn flush(&self) -> Result<()> {
        // no need to flush if task already finished
        if self.task_is_running().await.is_err() {
            return Ok(());
        }

        let sender = self.xorb_data_queue.lock().await;
        let (signal_tx, signal_rx) = oneshot::channel();
        sender
            .send(QueueItem::Flush(signal_tx))
            .await
            .map_err(|e| InternalError(format!("{e}")))?;

        signal_rx.await.map_err(|e| InternalError(format!("{e}")))?;

        Ok(())
    }

    async fn task_is_running(&self) -> Result<()> {
        let uploader_worker = self.upload_worker.lock().await;

        if uploader_worker.is_none() {
            return Err(UploadTaskError("no active upload task".to_owned()));
        }

        Ok(())
    }
}

impl Drop for ParallelXorbUploader {
    fn drop(&mut self) {
        let _ = tokio::task::block_in_place(|| Handle::current().block_on(async move { self.flush().await }));
    }
}

async fn process_xorb_data_queue_item(
    item: XorbUploadInputType,
    shard_manager: &Arc<ShardFileManager>,
    cas: &Arc<dyn Client + Send + Sync>,
    cas_prefix: &str,
) -> Result<XorbUploadOutputType> {
    let (cas_hash, data, chunks) = match item {
        QueueItem::Value(tuple) => tuple,
        QueueItem::Flush(signal) => return Ok(QueueItem::Flush(signal)),
    };

    let raw_bytes_len = data.len();
    // upload xorb
    {
        let mut pos = 0;
        let chunk_and_boundaries = chunks
            .iter()
            .map(|(hash, len)| {
                pos += *len;
                (*hash, pos as u32)
            })
            .collect();
        cas.put(cas_prefix, &cas_hash, data, chunk_and_boundaries).await?;
    }

    // register for dedup
    // This should happen after uploading xorb above succeeded so not to
    // leave invalid information in the local shard to dedup other xorbs.
    {
        let metadata = CASChunkSequenceHeader::new(cas_hash, chunks.len(), raw_bytes_len);

        let mut pos = 0;
        let chunks: Vec<_> = chunks
            .iter()
            .map(|(h, len)| {
                let result = CASChunkSequenceEntry::new(*h, *len, pos);
                pos += *len;
                result
            })
            .collect();
        let cas_info = MDBCASInfo { metadata, chunks };

        shard_manager.add_cas_block(cas_info).await?;
    }

    Ok(QueueItem::Value(()))
}
