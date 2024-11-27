use std::sync::Arc;

use cas_client::Client;
use futures::StreamExt;
use mdb_shard::{
    cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo},
    ShardFileManager,
};
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, Mutex,
};
use tokio::task::JoinHandle;
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
                .buffer_unordered(*MAX_CONCURRENT_XORB_UPLOADS);

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

    pub async fn flush(&self) -> Result<()> {
        let sender = self.xorb_data_queue.lock().await;
        let (signal_tx, signal_rx) = oneshot::channel();
        sender
            .send(QueueItem::Flush(signal_tx))
            .await
            .map_err(|e| InternalError(format!("{e}")))?;

        signal_rx.await.map_err(|e| InternalError(format!("{e}")))?;

        Ok(())
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
