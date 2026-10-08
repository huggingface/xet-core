//! Peer-to-peer xorb downloads through a local Dragonfly dfdaemon (<https://d7y.io>).
//!
//! When `HF_XET_CLIENT_DFDAEMON_SOCKET_PATH` is set, [`super::RemoteClient`] first asks the dfdaemon for
//! the byte ranges of each xorb. The dfdaemon gets the pieces from other peers of the cluster, and goes back
//! to the signed URL only when no peer has them. Any error here makes the caller use the direct HTTP download,
//! so this path adds a source and never removes one.
//!
//! One Dragonfly task holds one whole xorb. The task ID comes from the xorb key, not from the signed URL, so
//! the task is the same for every file, repository and signature that references the xorb. The dfdaemon
//! downloads only the pieces that overlap the requested range and streams them back whole; this module cuts
//! the range out of them.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use dragonfly_api::common::v2::{Download, Range, SchedulingPolicy, TaskType};
use dragonfly_api::dfdaemon::v2::DownloadTaskRequest;
use dragonfly_api::dfdaemon::v2::dfdaemon_download_client::DfdaemonDownloadClient;
use dragonfly_api::dfdaemon::v2::download_task_response::Response as DownloadTaskEvent;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::cas_types::{HttpRange, Key};
use crate::error::{ClientError, Result};

/// Piece length of every xorb task. A task ID computed from content does not include the piece length, so
/// all peers must use the same value. 4 MiB is the Dragonfly minimum, and also the length that Dragonfly
/// picks itself for objects of xorb size (64 MiB at most).
const XORB_PIECE_LENGTH: u64 = 4 * 1024 * 1024;

/// Client for the download gRPC service of a local dfdaemon, over its Unix socket.
pub(crate) struct DragonflyClient {
    socket_path: String,
    /// Maximum wait for each message of a download stream.
    read_timeout: Duration,
    /// Connected on first use. A failed connection leaves it empty, so a later call tries again.
    client: OnceCell<DfdaemonDownloadClient<Channel>>,
}

impl DragonflyClient {
    pub(crate) fn new(socket_path: String, read_timeout: Duration) -> Self {
        Self {
            socket_path,
            read_timeout,
            client: OnceCell::new(),
        }
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

    /// Downloads `range` of the xorb `key` through the dfdaemon and returns its bytes, still compressed.
    /// `url` is the signed URL that the dfdaemon uses if it has to go back to the source.
    pub(crate) async fn fetch_range(&self, key: &Key, url: &str, range: HttpRange) -> Result<Bytes> {
        let mut client = self.client().await?;
        let mut stream = client
            .download_task(download_request(key, url, range))
            .await
            .map_err(|status| ClientError::Other(format!("dfdaemon: download {key}: {status}")))?
            .into_inner();

        let mut assembler = RangeAssembler::new(range);
        loop {
            let message = tokio::time::timeout(self.read_timeout, stream.message())
                .await
                .map_err(|_| {
                    ClientError::Other(format!("dfdaemon: download {key}: no message for {:?}", self.read_timeout))
                })?
                .map_err(|status| ClientError::Other(format!("dfdaemon: download {key}: {status}")))?;
            let Some(message) = message else { break };

            if let Some(DownloadTaskEvent::DownloadPieceFinishedResponse(finished)) = message.response {
                let piece = finished
                    .piece
                    .ok_or_else(|| ClientError::Other(format!("dfdaemon: download {key}: piece is missing")))?;
                let content = piece.content.ok_or_else(|| {
                    ClientError::Other(format!("dfdaemon: download {key}: piece {} has no content", piece.number))
                })?;
                assembler.add_piece(piece.number, piece.offset, &content);
            }
        }
        assembler.finish()
    }
}

/// Content that the dfdaemon hashes (SHA-256) into the task ID of a xorb.
pub(crate) fn task_id_content(key: &Key) -> String {
    format!("xet-xorb/{key}")
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
    fn test_task_id_content_depends_only_on_the_xorb_key() {
        assert_eq!(
            task_id_content(&key()),
            "xet-xorb/default/02c57c0d7bc350d7f73e6b25ddd0093586b5bb394bff28420c5296376dd6db9e"
        );
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
    async fn test_fetch_range_fails_when_the_socket_does_not_exist() {
        let client = DragonflyClient::new("/nonexistent/dfdaemon.sock".to_owned(), Duration::from_secs(1));
        let err = client
            .fetch_range(&key(), "https://cdn.example.com/xorb", HttpRange::new(0, 9))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("connect"), "{err}");
    }
}
