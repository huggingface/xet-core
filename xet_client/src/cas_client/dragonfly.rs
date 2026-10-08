//! Peer-to-peer xorb downloads through a local Dragonfly dfdaemon (<https://d7y.io>).
//!
//! When `HF_XET_CLIENT_DFDAEMON_SOCKET_PATH` is set, [`super::RemoteClient`] first asks the dfdaemon for the
//! byte ranges of each xorb. Any error here makes the caller use the direct HTTP download, so this path adds
//! a source and never removes one. There are two modes (`HF_XET_CLIENT_DFDAEMON_MODE`):
//!
//! - [`DragonflyMode::Cache`] (default): the dfdaemon is a pure peer-to-peer cache and never contacts the CDN. Each
//!   byte range is a persistent cache task whose ID comes from the xorb key and the range. On a miss, the caller
//!   downloads the range directly, as without Dragonfly, then imports it with [`DragonflyClient::import_range`] so that
//!   other nodes get it from this one. This works with today's signed xorb URLs, but the scheduler needs Redis.
//! - [`DragonflyMode::Source`]: one standard task per whole xorb, with an ID from the xorb key. The dfdaemon goes back
//!   to the signed URL when no peer has the pieces. This needs xorb URLs that accept any byte range, which is not the
//!   case today, but it shares a xorb between all the files that use any part of it.
//!
//! In both modes, the dfdaemon streams back whole pieces and this module cuts the range out of them.

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use dragonfly_api::common::v2::{Download, Piece, Range, SchedulingPolicy, TaskType};
use dragonfly_api::dfdaemon::v2::dfdaemon_download_client::DfdaemonDownloadClient;
use dragonfly_api::dfdaemon::v2::{
    DownloadPersistentCacheTaskRequest, DownloadPersistentCacheTaskResponse, DownloadTaskRequest, DownloadTaskResponse,
    UploadPersistentCacheTaskRequest, download_persistent_cache_task_response, download_task_response,
};
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;
use tokio::sync::OnceCell;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::cas_types::{HttpRange, Key};
use crate::error::{ClientError, Result};

/// Piece length of every xorb task. A task ID computed from content does not include the piece length, so
/// all peers must use the same value. 4 MiB is the Dragonfly minimum, and also the length that Dragonfly
/// picks itself for objects of xorb size (64 MiB at most).
const XORB_PIECE_LENGTH: u64 = 4 * 1024 * 1024;

/// Number of peers that keep an imported range, the same default as `dfcache import`.
const PERSISTENT_REPLICA_COUNT: u64 = 2;

/// How the dfdaemon gets the ranges that no peer has yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DragonflyMode {
    /// The dfdaemon never contacts the CDN; the client imports what it downloads directly.
    Cache,
    /// The dfdaemon downloads from the signed URL itself.
    Source,
}

impl DragonflyMode {
    /// Parses `HF_XET_CLIENT_DFDAEMON_MODE`. Returns `None` for an unknown value.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "cache" => Some(Self::Cache),
            "source" => Some(Self::Source),
            _ => None,
        }
    }
}

/// Client for the download gRPC service of a local dfdaemon, over its Unix socket.
pub(crate) struct DragonflyClient {
    mode: DragonflyMode,
    socket_path: String,
    /// Directory for the files that [`DragonflyMode::Cache`] imports. The dfdaemon reads them by path.
    import_dir: PathBuf,
    /// Maximum wait for each message of a download stream.
    read_timeout: Duration,
    /// Connected on first use. A failed connection leaves it empty, so a later call tries again.
    client: OnceCell<DfdaemonDownloadClient<Channel>>,
}

impl DragonflyClient {
    pub(crate) fn new(mode: DragonflyMode, socket_path: String, import_dir: PathBuf, read_timeout: Duration) -> Self {
        Self {
            mode,
            socket_path,
            import_dir,
            read_timeout,
            client: OnceCell::new(),
        }
    }

    pub(crate) fn mode(&self) -> DragonflyMode {
        self.mode
    }

    async fn client(&self) -> Result<DfdaemonDownloadClient<Channel>> {
        let client = self
            .client
            .get_or_try_init(|| async {
                let socket_path = self.socket_path.clone();
                // tonic needs a URI, but the connector below always dials the Unix socket.
                let channel = Endpoint::from_static("http://dfdaemon")
                    .connect_with_connector(service_fn(move |_: Uri| {
                        let socket_path = socket_path.clone();
                        async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(socket_path).await?)) }
                    }))
                    .await
                    .map_err(|e| ClientError::Other(format!("dfdaemon: connect to {}: {e}", self.socket_path)))?;
                // Each message carries one whole piece, larger than the 4 MiB tonic default.
                Ok::<_, ClientError>(DfdaemonDownloadClient::new(channel).max_decoding_message_size(usize::MAX))
            })
            .await?;
        Ok(client.clone())
    }

    /// Gets `range` of the xorb `key` through the dfdaemon and returns its bytes, still compressed.
    /// `url` is the signed URL; only [`DragonflyMode::Source`] gives it to the dfdaemon.
    pub(crate) async fn fetch_range(&self, key: &Key, url: &str, range: HttpRange) -> Result<Bytes> {
        let mut client = self.client().await?;
        match self.mode {
            DragonflyMode::Source => {
                let stream = client
                    .download_task(download_request(key, url, range))
                    .await
                    .map_err(|status| ClientError::Other(format!("dfdaemon: download {key}: {status}")))?
                    .into_inner();
                self.collect_range(key, stream, range, |message: DownloadTaskResponse| match message.response {
                    Some(download_task_response::Response::DownloadPieceFinishedResponse(finished)) => {
                        Some(finished.piece)
                    },
                    _ => None,
                })
                .await
            },
            DragonflyMode::Cache => {
                let stream = client
                    .download_persistent_cache_task(persistent_cache_download_request(key, range))
                    .await
                    .map_err(|status| ClientError::Other(format!("dfdaemon: cache lookup {key} {range}: {status}")))?
                    .into_inner();
                // A persistent cache task holds exactly the range, so its bytes start at 0.
                let whole = HttpRange::new(0, range.length() - 1);
                self.collect_range(key, stream, whole, |message: DownloadPersistentCacheTaskResponse| {
                    match message.response {
                        Some(download_persistent_cache_task_response::Response::DownloadPieceFinishedResponse(
                            finished,
                        )) => Some(finished.piece),
                        _ => None,
                    }
                })
                .await
            },
        }
    }

    /// Reads a download stream to its end and puts `range` together from the pieces in it.
    async fn collect_range<M>(
        &self,
        key: &Key,
        mut stream: Streaming<M>,
        range: HttpRange,
        piece_of: impl Fn(M) -> Option<Option<Piece>>,
    ) -> Result<Bytes> {
        let mut assembler = RangeAssembler::new(range);
        loop {
            let message = tokio::time::timeout(self.read_timeout, stream.message())
                .await
                .map_err(|_| {
                    ClientError::Other(format!("dfdaemon: download {key}: no message for {:?}", self.read_timeout))
                })?
                .map_err(|status| ClientError::Other(format!("dfdaemon: download {key}: {status}")))?;
            let Some(message) = message else { break };

            if let Some(piece) = piece_of(message) {
                let piece =
                    piece.ok_or_else(|| ClientError::Other(format!("dfdaemon: download {key}: piece is missing")))?;
                let content = piece.content.ok_or_else(|| {
                    ClientError::Other(format!("dfdaemon: download {key}: piece {} has no content", piece.number))
                })?;
                assembler.add_piece(piece.number, piece.offset, &content);
            }
        }
        assembler.finish()
    }

    /// Imports the compressed bytes of `range` of the xorb `key` as a persistent cache task, so that other
    /// peers can download it. The dfdaemon only reads files by path: the bytes go through a temporary file
    /// in the import directory, which the dfdaemon copies into its storage before it answers.
    pub(crate) async fn import_range(&self, key: &Key, range: HttpRange, data: &[u8]) -> Result<()> {
        let mut file = tempfile::Builder::new()
            .prefix("xet-xorb-")
            .tempfile_in(&self.import_dir)
            .map_err(|e| ClientError::Other(format!("dfdaemon: import {key} {range}: temporary file: {e}")))?;
        file.write_all(data)
            .and_then(|_| file.flush())
            .map_err(|e| ClientError::Other(format!("dfdaemon: import {key} {range}: write: {e}")))?;

        let request = persistent_cache_upload_request(key, range, file.path().to_string_lossy().into_owned());
        let mut client = self.client().await?;
        client
            .upload_persistent_cache_task(request)
            .await
            .map_err(|status| ClientError::Other(format!("dfdaemon: import {key} {range}: {status}")))?;
        Ok(())
    }
}

/// Content that the dfdaemon hashes (SHA-256) into the task ID of a whole xorb ([`DragonflyMode::Source`]).
pub(crate) fn task_id_content(key: &Key) -> String {
    format!("xet-xorb/{key}")
}

/// Content that the dfdaemon hashes into the task ID of one byte range of a xorb ([`DragonflyMode::Cache`]).
/// The range is part of the key because a persistent cache task cannot be read by range.
pub(crate) fn range_task_id_content(key: &Key, range: HttpRange) -> String {
    format!("xet-xorb-range/{key}/{}-{}", range.start, range.end)
}

/// The persistent cache task ID that the dfdaemon computes for `content`: its SHA-256, in hex. A download
/// takes the ID, not the content, so the client computes it the same way.
fn persistent_cache_task_id(content: &str) -> String {
    Sha256::digest(content.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

fn download_request(key: &Key, url: &str, range: HttpRange) -> DownloadTaskRequest {
    DownloadTaskRequest {
        download: Some(Download {
            url: url.to_owned(),
            range: Some(Range {
                start: range.start,
                length: range.length(),
            }),
            r#type: TaskType::Standard as i32,
            piece_length: Some(XORB_PIECE_LENGTH),
            content_for_calculating_task_id: Some(task_id_content(key)),
            need_piece_content: true,
            // With the default (AUTO), a range of 4 MiB or less skips the scheduler and goes to the
            // source directly, so no other peer could find it.
            scheduling_policy: SchedulingPolicy::Always as i32,
            ..Default::default()
        }),
    }
}

fn persistent_cache_download_request(key: &Key, range: HttpRange) -> DownloadPersistentCacheTaskRequest {
    DownloadPersistentCacheTaskRequest {
        task_id: persistent_cache_task_id(&range_task_id_content(key, range)),
        need_piece_content: true,
        ..Default::default()
    }
}

fn persistent_cache_upload_request(key: &Key, range: HttpRange, path: String) -> UploadPersistentCacheTaskRequest {
    UploadPersistentCacheTaskRequest {
        content_for_calculating_task_id: Some(range_task_id_content(key, range)),
        path,
        persistent_replica_count: PERSISTENT_REPLICA_COUNT,
        piece_length: Some(XORB_PIECE_LENGTH),
        // No TTL: the dfdaemon applies its `gc.policy.persistentCacheTaskTTL`, which the operator sets.
        ..Default::default()
    }
}

/// Builds a byte range out of the whole pieces that the dfdaemon streams back, in any order.
struct RangeAssembler {
    start: u64,
    buffer: Vec<u8>,
    filled: usize,
    pieces_seen: HashSet<u32>,
}

impl RangeAssembler {
    fn new(range: HttpRange) -> Self {
        Self {
            start: range.start,
            buffer: vec![0; range.length() as usize],
            filled: 0,
            pieces_seen: HashSet::new(),
        }
    }

    /// Copies the part of the piece that is inside the range. A piece that was seen before is ignored.
    fn add_piece(&mut self, number: u32, offset: u64, content: &[u8]) {
        if !self.pieces_seen.insert(number) {
            return;
        }
        let range_end = self.start + self.buffer.len() as u64;
        let low = offset.max(self.start);
        let high = (offset + content.len() as u64).min(range_end);
        if low >= high {
            return;
        }
        self.buffer[(low - self.start) as usize..(high - self.start) as usize]
            .copy_from_slice(&content[(low - offset) as usize..(high - offset) as usize]);
        self.filled += (high - low) as usize;
    }

    fn finish(self) -> Result<Bytes> {
        if self.filled != self.buffer.len() {
            return Err(ClientError::Other(format!(
                "dfdaemon: received {} of {} bytes of the range",
                self.filled,
                self.buffer.len()
            )));
        }
        Ok(Bytes::from(self.buffer))
    }
}

/// An in-process dfdaemon for tests: it serves persistent cache tasks from memory and imports files by path,
/// like the real one. Every other method answers `UNIMPLEMENTED`.
#[cfg(test)]
pub(crate) mod fake_dfdaemon {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use dragonfly_api::common::v2::{CacheTask, PersistentCacheTask, PersistentTask, Piece, Task};
    use dragonfly_api::dfdaemon::v2::dfdaemon_download_server::{DfdaemonDownload, DfdaemonDownloadServer};
    use dragonfly_api::dfdaemon::v2::*;
    use futures::stream::BoxStream;
    use tokio::net::UnixListener;
    use tonic::{Request, Response, Status};

    use super::persistent_cache_task_id;

    /// Size of the pieces that the fake streams back; small, so that tests cover several pieces.
    const PIECE_SIZE: usize = 7;

    #[derive(Default)]
    pub(crate) struct FakeDfdaemon {
        /// Persistent cache tasks by task ID.
        pub tasks: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[tonic::async_trait]
    impl DfdaemonDownload for FakeDfdaemon {
        type DownloadTaskStream = BoxStream<'static, Result<DownloadTaskResponse, Status>>;
        type DownloadCacheTaskStream = BoxStream<'static, Result<DownloadCacheTaskResponse, Status>>;
        type DownloadPersistentTaskStream = BoxStream<'static, Result<DownloadPersistentTaskResponse, Status>>;
        type DownloadPersistentCacheTaskStream =
            BoxStream<'static, Result<DownloadPersistentCacheTaskResponse, Status>>;

        async fn download_task(
            &self,
            _: Request<DownloadTaskRequest>,
        ) -> Result<Response<Self::DownloadTaskStream>, Status> {
            Err(Status::unimplemented("download_task"))
        }

        async fn stat_task(&self, _: Request<StatTaskRequest>) -> Result<Response<Task>, Status> {
            Err(Status::unimplemented("stat_task"))
        }

        async fn stat_local_task(
            &self,
            _: Request<StatLocalTaskRequest>,
        ) -> Result<Response<StatLocalTaskResponse>, Status> {
            Err(Status::unimplemented("stat_local_task"))
        }

        async fn list_local_tasks(
            &self,
            _: Request<ListLocalTasksRequest>,
        ) -> Result<Response<ListLocalTasksResponse>, Status> {
            Err(Status::unimplemented("list_local_tasks"))
        }

        async fn list_task_entries(
            &self,
            _: Request<ListTaskEntriesRequest>,
        ) -> Result<Response<ListTaskEntriesResponse>, Status> {
            Err(Status::unimplemented("list_task_entries"))
        }

        async fn delete_task(&self, _: Request<DeleteTaskRequest>) -> Result<Response<()>, Status> {
            Err(Status::unimplemented("delete_task"))
        }

        async fn delete_local_task(&self, _: Request<DeleteLocalTaskRequest>) -> Result<Response<()>, Status> {
            Err(Status::unimplemented("delete_local_task"))
        }

        async fn delete_host(&self, _: Request<()>) -> Result<Response<()>, Status> {
            Err(Status::unimplemented("delete_host"))
        }

        async fn download_cache_task(
            &self,
            _: Request<DownloadCacheTaskRequest>,
        ) -> Result<Response<Self::DownloadCacheTaskStream>, Status> {
            Err(Status::unimplemented("download_cache_task"))
        }

        async fn stat_cache_task(&self, _: Request<StatCacheTaskRequest>) -> Result<Response<CacheTask>, Status> {
            Err(Status::unimplemented("stat_cache_task"))
        }

        async fn delete_cache_task(&self, _: Request<DeleteCacheTaskRequest>) -> Result<Response<()>, Status> {
            Err(Status::unimplemented("delete_cache_task"))
        }

        async fn download_persistent_task(
            &self,
            _: Request<DownloadPersistentTaskRequest>,
        ) -> Result<Response<Self::DownloadPersistentTaskStream>, Status> {
            Err(Status::unimplemented("download_persistent_task"))
        }

        async fn upload_persistent_task(
            &self,
            _: Request<UploadPersistentTaskRequest>,
        ) -> Result<Response<PersistentTask>, Status> {
            Err(Status::unimplemented("upload_persistent_task"))
        }

        async fn stat_persistent_task(
            &self,
            _: Request<StatPersistentTaskRequest>,
        ) -> Result<Response<PersistentTask>, Status> {
            Err(Status::unimplemented("stat_persistent_task"))
        }

        async fn stat_local_persistent_task(
            &self,
            _: Request<StatLocalPersistentTaskRequest>,
        ) -> Result<Response<StatLocalPersistentTaskResponse>, Status> {
            Err(Status::unimplemented("stat_local_persistent_task"))
        }

        async fn list_local_persistent_tasks(
            &self,
            _: Request<ListLocalPersistentTasksRequest>,
        ) -> Result<Response<ListLocalPersistentTasksResponse>, Status> {
            Err(Status::unimplemented("list_local_persistent_tasks"))
        }

        async fn stat_persistent_cache_task(
            &self,
            _: Request<StatPersistentCacheTaskRequest>,
        ) -> Result<Response<PersistentCacheTask>, Status> {
            Err(Status::unimplemented("stat_persistent_cache_task"))
        }

        async fn stat_local_persistent_cache_task(
            &self,
            _: Request<StatLocalPersistentCacheTaskRequest>,
        ) -> Result<Response<StatLocalPersistentCacheTaskResponse>, Status> {
            Err(Status::unimplemented("stat_local_persistent_cache_task"))
        }

        async fn list_local_persistent_cache_tasks(
            &self,
            _: Request<ListLocalPersistentCacheTasksRequest>,
        ) -> Result<Response<ListLocalPersistentCacheTasksResponse>, Status> {
            Err(Status::unimplemented("list_local_persistent_cache_tasks"))
        }

        async fn download_persistent_cache_task(
            &self,
            request: Request<DownloadPersistentCacheTaskRequest>,
        ) -> Result<Response<Self::DownloadPersistentCacheTaskStream>, Status> {
            let task_id = request.into_inner().task_id;
            let Some(content) = self.tasks.lock().unwrap().get(&task_id).cloned() else {
                return Err(Status::not_found(format!("persistent cache task {task_id} not found")));
            };
            let started = DownloadPersistentCacheTaskResponse {
                response: Some(
                    download_persistent_cache_task_response::Response::DownloadPersistentCacheTaskStartedResponse(
                        DownloadPersistentCacheTaskStartedResponse {
                            content_length: content.len() as u64,
                        },
                    ),
                ),
                ..Default::default()
            };
            // Last piece first: the client must not rely on the order.
            let pieces = content.chunks(PIECE_SIZE).enumerate().rev().map(|(number, chunk)| {
                Ok(DownloadPersistentCacheTaskResponse {
                    response: Some(download_persistent_cache_task_response::Response::DownloadPieceFinishedResponse(
                        DownloadPieceFinishedResponse {
                            piece: Some(Piece {
                                number: number as u32,
                                offset: (number * PIECE_SIZE) as u64,
                                length: chunk.len() as u64,
                                content: Some(chunk.to_vec()),
                                ..Default::default()
                            }),
                        },
                    )),
                    ..Default::default()
                })
            });
            let messages: Vec<_> = std::iter::once(Ok(started)).chain(pieces).collect();
            Ok(Response::new(Box::pin(futures::stream::iter(messages))))
        }

        async fn upload_persistent_cache_task(
            &self,
            request: Request<UploadPersistentCacheTaskRequest>,
        ) -> Result<Response<PersistentCacheTask>, Status> {
            let request = request.into_inner();
            let content = std::fs::read(&request.path).map_err(|e| Status::internal(e.to_string()))?;
            let task_id = persistent_cache_task_id(
                &request
                    .content_for_calculating_task_id
                    .ok_or_else(|| Status::invalid_argument("content_for_calculating_task_id"))?,
            );
            self.tasks.lock().unwrap().insert(task_id, content);
            Ok(Response::new(PersistentCacheTask::default()))
        }
    }

    /// Serves a new fake on `<dir>/dfdaemon.sock` until the test runtime stops.
    pub(crate) fn start(dir: &Path) -> (Arc<FakeDfdaemon>, PathBuf) {
        let socket_path = dir.join("dfdaemon.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let fake = Arc::new(FakeDfdaemon::default());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let service = DfdaemonDownloadServer::from_arc(fake.clone());
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );
        (fake, socket_path)
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    fn key() -> Key {
        Key {
            prefix: "default".to_owned(),
            hash: MerkleHash::from_hex("02c57c0d7bc350d7f73e6b25ddd0093586b5bb394bff28420c5296376dd6db9e").unwrap(),
        }
    }

    #[test]
    fn test_mode_parse() {
        assert_eq!(DragonflyMode::parse("cache"), Some(DragonflyMode::Cache));
        assert_eq!(DragonflyMode::parse("source"), Some(DragonflyMode::Source));
        assert_eq!(DragonflyMode::parse("Cache"), None);
        assert_eq!(DragonflyMode::parse(""), None);
    }

    #[test]
    fn test_task_id_content_depends_only_on_the_xorb_key() {
        assert_eq!(
            task_id_content(&key()),
            "xet-xorb/default/02c57c0d7bc350d7f73e6b25ddd0093586b5bb394bff28420c5296376dd6db9e"
        );
    }

    #[test]
    fn test_range_task_id_content_includes_the_range() {
        assert_eq!(
            range_task_id_content(&key(), HttpRange::new(0, 55821581)),
            "xet-xorb-range/default/02c57c0d7bc350d7f73e6b25ddd0093586b5bb394bff28420c5296376dd6db9e/0-55821581"
        );
        assert_ne!(
            range_task_id_content(&key(), HttpRange::new(0, 99)),
            range_task_id_content(&key(), HttpRange::new(0, 100))
        );
    }

    #[test]
    fn test_persistent_cache_task_id_is_the_sha256_of_the_content() {
        // sha256("abc"), as computed by the dfdaemon's id generator.
        assert_eq!(persistent_cache_task_id("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    #[test]
    fn test_download_request_is_content_addressed_and_ranged() {
        let request = download_request(&key(), "https://cdn.example.com/xorb?Signature=abc", HttpRange::new(100, 199));
        let download = request.download.unwrap();
        assert_eq!(download.url, "https://cdn.example.com/xorb?Signature=abc");
        assert_eq!(download.content_for_calculating_task_id, Some(task_id_content(&key())));
        let range = download.range.unwrap();
        assert_eq!((range.start, range.length), (100, 100));
        assert_eq!(download.piece_length, Some(XORB_PIECE_LENGTH));
        assert!(download.need_piece_content);
        assert_eq!(download.scheduling_policy, SchedulingPolicy::Always as i32);
        assert!(!download.disable_back_to_source);
    }

    #[test]
    fn test_cache_download_and_upload_use_the_same_task() {
        let range = HttpRange::new(100, 199);
        let download = persistent_cache_download_request(&key(), range);
        let upload = persistent_cache_upload_request(&key(), range, "/tmp/x".to_owned());
        assert_eq!(
            download.task_id,
            persistent_cache_task_id(upload.content_for_calculating_task_id.as_ref().unwrap())
        );
        assert!(download.need_piece_content);
        assert_eq!(upload.path, "/tmp/x");
        assert_eq!(upload.persistent_replica_count, PERSISTENT_REPLICA_COUNT);
        assert_eq!(upload.piece_length, Some(XORB_PIECE_LENGTH));
        assert!(upload.ttl.is_none());
    }

    #[test]
    fn test_assembler_cuts_the_range_out_of_pieces_in_any_order() {
        // Object of 30 bytes in pieces of 10; the range is bytes 5..=24.
        let object: Vec<u8> = (0..30).collect();
        let mut assembler = RangeAssembler::new(HttpRange::new(5, 24));
        assembler.add_piece(2, 20, &object[20..30]);
        assembler.add_piece(0, 0, &object[0..10]);
        assembler.add_piece(0, 0, &object[0..10]);
        assembler.add_piece(1, 10, &object[10..20]);
        assert_eq!(assembler.finish().unwrap().as_ref(), &object[5..25]);
    }

    #[test]
    fn test_assembler_ignores_pieces_outside_the_range() {
        let object: Vec<u8> = (0..40).collect();
        let mut assembler = RangeAssembler::new(HttpRange::new(10, 19));
        assembler.add_piece(0, 0, &object[0..10]);
        assembler.add_piece(2, 20, &object[20..30]);
        assembler.add_piece(1, 10, &object[10..20]);
        assert_eq!(assembler.finish().unwrap().as_ref(), &object[10..20]);
    }

    #[test]
    fn test_assembler_rejects_a_missing_piece() {
        let object: Vec<u8> = (0..30).collect();
        let mut assembler = RangeAssembler::new(HttpRange::new(0, 29));
        assembler.add_piece(0, 0, &object[0..10]);
        assembler.add_piece(2, 20, &object[20..30]);
        assert!(assembler.finish().is_err());
    }

    #[tokio::test]
    async fn test_cache_mode_misses_then_serves_an_imported_range() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, socket_path) = fake_dfdaemon::start(dir.path());
        let client = DragonflyClient::new(
            DragonflyMode::Cache,
            socket_path.to_string_lossy().into_owned(),
            dir.path().to_path_buf(),
            Duration::from_secs(5),
        );
        let range = HttpRange::new(1000, 1029);
        let data: Vec<u8> = (0..30).collect();

        let err = client.fetch_range(&key(), "unused", range).await.unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");

        client.import_range(&key(), range, &data).await.unwrap();
        assert_eq!(fake.tasks.lock().unwrap().len(), 1);
        // The temporary file is removed once the dfdaemon has copied it.
        assert!(
            std::fs::read_dir(dir.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("xet-xorb-"))
        );

        assert_eq!(client.fetch_range(&key(), "unused", range).await.unwrap().as_ref(), data.as_slice());
        // Another range of the same xorb is another task.
        assert!(client.fetch_range(&key(), "unused", HttpRange::new(1000, 1028)).await.is_err());
    }

    #[tokio::test]
    async fn test_fetch_and_import_fail_when_the_socket_does_not_exist() {
        for mode in [DragonflyMode::Cache, DragonflyMode::Source] {
            let client = DragonflyClient::new(
                mode,
                "/nonexistent/dfdaemon.sock".to_owned(),
                std::env::temp_dir(),
                Duration::from_secs(1),
            );
            let err = client
                .fetch_range(&key(), "https://cdn.example.com/xorb", HttpRange::new(0, 9))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("connect"), "{err}");
        }

        let client = DragonflyClient::new(
            DragonflyMode::Cache,
            "/nonexistent/dfdaemon.sock".to_owned(),
            std::env::temp_dir(),
            Duration::from_secs(1),
        );
        let err = client.import_range(&key(), HttpRange::new(0, 2), b"abc").await.unwrap_err();
        assert!(err.to_string().contains("connect"), "{err}");
    }
}
