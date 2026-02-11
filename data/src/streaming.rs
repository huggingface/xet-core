use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use cas_types::FileRange;
use file_reconstruction::DataOutput;
use futures::stream::Stream;
use progress_tracking::TrackingProgressUpdater;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use utils::auth::TokenRefresher;
use xet_runtime::xet_config;

use crate::configurations::TranslatorConfig;
use crate::data_client::default_config;
use crate::errors::DataProcessingError;
use crate::file_cleaner::SingleFileCleaner;
use crate::{FileDownloader, FileUploadSession, XetFileInfo, errors};

/// A client that holds shared configuration, providing [`read`](XetClient::read)
/// and [`write`](XetClient::write) methods that return [`XetReader`] and
/// [`XetWriter`] respectively.
///
/// Creating a single `XetClient` and reusing it for multiple file operations
/// avoids redundant config building.
pub struct XetClient {
    config: Arc<TranslatorConfig>,
}

impl XetClient {
    /// Creates a new client with the given connection parameters.
    pub fn new(
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        user_agent: String,
    ) -> errors::Result<Self> {
        let config: Arc<TranslatorConfig> = default_config(
            endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
            None,
            token_info,
            token_refresher,
            user_agent,
        )?
        .into();

        Ok(Self { config })
    }

    /// Creates a reader that will download a single file.
    ///
    /// Each call creates a fresh [`FileDownloader`] from the client's config.
    pub fn read(
        &self,
        file_info: XetFileInfo,
        file_range: Option<FileRange>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        stream_buffer_size: usize,
    ) -> errors::Result<XetReader> {
        XetReader::new(self.config.clone(), file_info, file_range, progress_updater, stream_buffer_size)
    }

    /// Creates a writer that will upload a single file.
    ///
    /// Each call creates a fresh [`FileUploadSession`] because sessions are
    /// consumed on finalization and cannot be reused.
    pub async fn write(
        &self,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        name: Option<Arc<str>>,
        size: u64,
    ) -> errors::Result<XetWriter> {
        XetWriter::new(self.config.clone(), progress_updater, name, size).await
    }
}

/// A single-file upload writer whose API aligns with the OpenDAL `Write` trait.
///
/// Created via [`XetWriter::new`]. Feed data with [`write`](XetWriter::write),
/// then call [`close`](XetWriter::close) to finalize the upload and obtain the
/// resulting [`XetFileInfo`]. If the upload should be abandoned, call
/// [`abort`](XetWriter::abort) instead.
pub struct XetWriter {
    session: Option<Arc<FileUploadSession>>,
    handle: Option<SingleFileCleaner>,
}

impl XetWriter {
    /// Creates a new writer that will upload a single file.
    ///
    /// `size` is the total number of bytes that will be written and is used for
    /// progress tracking. The caller must know the content length before writing.
    pub async fn new(
        config: Arc<TranslatorConfig>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        name: Option<Arc<str>>,
        size: u64,
    ) -> errors::Result<Self> {
        let session = FileUploadSession::new(config, progress_updater).await?;
        let handle = session.start_clean(name, size, None).await;
        Ok(Self {
            session: Some(session),
            handle: Some(handle),
        })
    }

    /// Write a chunk of bytes into the upload pipeline.
    pub async fn write(&mut self, data: Bytes) -> errors::Result<()> {
        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| DataProcessingError::InternalError("writer already closed".into()))?;
        handle.add_data(&data).await
    }

    /// Finalize the upload, flush all data to the remote, and return file info.
    pub async fn close(&mut self) -> errors::Result<XetFileInfo> {
        let handle = self
            .handle
            .take()
            .ok_or_else(|| DataProcessingError::InternalError("writer already closed".into()))?;
        let (file_info, _metrics) = handle.finish().await?;
        let session = self
            .session
            .take()
            .ok_or_else(|| DataProcessingError::InternalError("writer already closed".into()))?;
        session.finalize().await?;
        Ok(file_info)
    }

    /// Abort the upload, discarding any data written so far.
    pub async fn abort(&mut self) -> errors::Result<()> {
        self.handle.take();
        self.session.take();
        Ok(())
    }
}

/// A single-file download reader whose API aligns with the OpenDAL `Read` trait.
///
/// Created via [`XetReader::new`]. Implements [`Stream`] yielding `Bytes`
/// chunks. The download is lazy — it starts on the first poll.
/// `stream_buffer_size` controls how many chunks can be buffered before
/// backpressure is applied.
pub struct XetReader {
    buffer_size: usize,
    merkle_hash: merklehash::MerkleHash,
    file_hash: Arc<str>,
    file_range: Option<FileRange>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    state: ReaderState,
}

enum ReaderState {
    /// Download has not started yet; will be spawned on first poll.
    Init { config: Arc<TranslatorConfig> },
    /// Download task running, receiving chunks.
    Streaming {
        rx: mpsc::Receiver<Bytes>,
        handle: Option<JoinHandle<errors::Result<u64>>>,
    },
    /// Terminal state.
    Completed,
}

impl XetReader {
    /// Creates a new reader that will download a single file.
    ///
    /// The download is lazy — it starts only when the stream is first polled
    /// or when [`start`](XetReader::start) is called explicitly.
    /// `stream_buffer_size` controls how many chunks can be buffered before
    /// backpressure is applied.
    pub fn new(
        config: Arc<TranslatorConfig>,
        file_info: XetFileInfo,
        file_range: Option<FileRange>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        stream_buffer_size: usize,
    ) -> errors::Result<Self> {
        let merkle_hash = file_info.merkle_hash()?;
        let file_hash: Arc<str> = file_info.hash().into();

        Ok(Self {
            buffer_size: stream_buffer_size,
            merkle_hash,
            file_hash,
            file_range,
            progress_updater,
            state: ReaderState::Init { config },
        })
    }

    /// Eagerly start the download task.
    ///
    /// By default the download is lazy and begins on the first poll. Call this
    /// method to kick it off immediately so data is already buffering when you
    /// start consuming the stream.
    ///
    /// Calling `start` on an already-started or completed reader is a no-op.
    pub fn start(&mut self) {
        if let ReaderState::Init { config } = &self.state {
            let (output, rx) = DataOutput::byte_stream(self.buffer_size);
            let config = config.clone();
            let merkle_hash = self.merkle_hash;
            let file_hash = self.file_hash.clone();
            let file_range = self.file_range;
            let progress_updater = self.progress_updater.take();
            let handle = tokio::spawn(async move {
                let downloader = match FileDownloader::new(config).await {
                    Ok(d) => d,
                    Err(e) => return Err(e),
                };
                let progress_updater = progress_updater.map(ItemProgressUpdater::new);
                downloader
                    .smudge_file_from_hash(&merkle_hash, file_hash, output, file_range, progress_updater)
                    .await
            });
            self.state = ReaderState::Streaming {
                rx,
                handle: Some(handle),
            };
        }
    }
}

impl Stream for XetReader {
    type Item = Result<Bytes, DataProcessingError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match &mut this.state {
            ReaderState::Init { .. } => {
                this.start();
                Pin::new(this).poll_next(cx)
            },

            ReaderState::Streaming { rx, handle } => {
                // Check the task handle first for early error detection.
                if let Some(h) = handle
                    && let Poll::Ready(result) = Pin::new(h).poll(cx)
                {
                    let result = result.expect("download task panicked");
                    match result {
                        Ok(_) => *handle = None,
                        Err(e) => {
                            this.state = ReaderState::Completed;
                            return Poll::Ready(Some(Err(e)));
                        },
                    }
                }

                // Poll the channel for the next chunk.
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(chunk))),
                    Poll::Ready(None) => {
                        // Channel closed — check task one more time.
                        if let Some(h) = handle {
                            return match Pin::new(h).poll(cx) {
                                Poll::Ready(result) => {
                                    this.state = ReaderState::Completed;
                                    let result = result.expect("download task panicked");
                                    match result {
                                        Ok(_) => Poll::Ready(None),
                                        Err(e) => Poll::Ready(Some(Err(e))),
                                    }
                                },
                                Poll::Pending => Poll::Pending,
                            };
                        }
                        this.state = ReaderState::Completed;
                        Poll::Ready(None)
                    },
                    Poll::Pending => Poll::Pending,
                }
            },

            ReaderState::Completed => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, TryStreamExt};
    use tempfile::tempdir;

    use super::*;

    /// Upload `content` via [`XetWriter`] and download it back via [`XetReader`],
    /// asserting the round-tripped bytes match the original.
    async fn assert_roundtrip(client: &XetClient, content: &[u8]) {
        let mut writer = client.write(None, None, content.len() as u64).await.unwrap();
        for chunk in content.chunks(4096) {
            writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
        }
        let file_info = writer.close().await.unwrap();
        assert_eq!(file_info.file_size(), content.len() as u64);

        let reader = client.read(file_info, None, None, 64).unwrap();
        let chunks: Vec<Bytes> = reader.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), content);
    }

    /// Create a reader already in the streaming state for testing poll behavior.
    fn streaming_reader(rx: mpsc::Receiver<Bytes>, handle: JoinHandle<errors::Result<u64>>) -> XetReader {
        XetReader {
            buffer_size: 0,
            merkle_hash: Default::default(),
            file_hash: "".into(),
            file_range: None,
            progress_updater: None,
            state: ReaderState::Streaming {
                rx,
                handle: Some(handle),
            },
        }
    }

    #[tokio::test]
    async fn test_roundtrip() {
        let temp_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", temp_dir.path().display());
        let client = XetClient::new(Some(endpoint), None, None, "test".into()).unwrap();

        // Empty.
        assert_roundtrip(&client, &[]).await;
        // Small.
        assert_roundtrip(&client, b"Hello, World!").await;
        // Large (1 MB).
        let large: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        assert_roundtrip(&client, &large).await;
    }

    #[tokio::test]
    async fn test_partial_range() {
        let temp_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", temp_dir.path().display());
        let client = XetClient::new(Some(endpoint), None, None, "test".into()).unwrap();

        let content: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let mut writer = client.write(None, None, content.len() as u64).await.unwrap();
        for chunk in content.chunks(4096) {
            writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
        }
        let file_info = writer.close().await.unwrap();

        let range = FileRange::new(1000, 5000);
        let reader = client.read(file_info, Some(range), None, 64).unwrap();
        let chunks: Vec<Bytes> = reader.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), &content[1000..5000]);
    }

    #[tokio::test]
    async fn test_lazy_initialization() {
        let temp_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", temp_dir.path().display());
        let client = XetClient::new(Some(endpoint), None, None, "test".into()).unwrap();

        let mut writer = client.write(None, None, 5).await.unwrap();
        writer.write(Bytes::from_static(b"hello")).await.unwrap();
        let file_info = writer.close().await.unwrap();

        let mut reader = client.read(file_info, None, None, 64).unwrap();
        assert!(matches!(reader.state, ReaderState::Init { .. }));

        let _ = reader.next().await;
        assert!(!matches!(reader.state, ReaderState::Init { .. }));
    }

    #[tokio::test]
    async fn test_reader_error_propagation() {
        let (_tx, rx) = mpsc::channel(4);
        let handle = tokio::spawn(async { Err(DataProcessingError::InternalError("download failed".into())) });
        let mut reader = streaming_reader(rx, handle);

        let item = reader.next().await.unwrap();
        assert!(item.is_err());
        assert!(reader.next().await.is_none());
    }

    #[tokio::test]
    async fn test_reader_error_surfaces_early() {
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(async move {
            tx.send(Bytes::from("a")).await.unwrap();
            tx.send(Bytes::from("b")).await.unwrap();
            tx.send(Bytes::from("c")).await.unwrap();
            Err(DataProcessingError::InternalError("fail".into()))
        });
        let mut reader = streaming_reader(rx, handle);

        // Give the producer time to fill the buffer and return the error.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut saw_error = false;
        let mut chunks_before_error = 0;
        while let Some(item) = reader.next().await {
            match item {
                Ok(_) => chunks_before_error += 1,
                Err(_) => {
                    saw_error = true;
                    break;
                },
            }
        }
        assert!(saw_error);
        assert!(chunks_before_error < 3, "error should surface before all buffered chunks");
    }

    #[tokio::test]
    async fn test_writer_abort() {
        let temp_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", temp_dir.path().display());
        let client = XetClient::new(Some(endpoint), None, None, "test".into()).unwrap();

        let mut writer = client.write(None, None, 100).await.unwrap();
        writer.write(Bytes::from_static(b"some data")).await.unwrap();
        writer.abort().await.unwrap();

        assert!(writer.write(Bytes::from_static(b"more")).await.is_err());
        assert!(writer.close().await.is_err());
    }

    #[tokio::test]
    async fn test_sha256_returned() {
        let temp_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", temp_dir.path().display());
        let client = XetClient::new(Some(endpoint), None, None, "test".into()).unwrap();

        let content = b"Hello, World!";
        let mut writer = client.write(None, None, Some(content.len() as u64)).await.unwrap();
        writer.write(Bytes::from_static(content)).await.unwrap();
        let file_info = writer.close().await.unwrap();

        assert!(file_info.sha256().is_some(), "SHA256 hash should be present");
        let sha256 = file_info.sha256().unwrap();
        assert_eq!(sha256.len(), 64, "SHA256 should be 64 hex characters");
        assert!(sha256.chars().all(|c| c.is_ascii_hexdigit()), "SHA256 should be valid hex");
    }
}
