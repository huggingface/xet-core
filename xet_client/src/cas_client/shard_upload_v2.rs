use futures::TryStreamExt;
use reqwest::Response;

use super::interface::{ShardUploadProgressCallback, ShardUploadProgressType};
use crate::cas_types::{ShardUploadEvent, UploadShardResponseType};
use crate::error::{ClientError, Result};

/// Parse the `/v2/shards` NDJSON response stream and return whether the shard was newly inserted.
pub(crate) async fn read_shard_upload_ndjson(
    response: Response,
    progress_callback: Option<ShardUploadProgressCallback>,
) -> Result<bool> {
    let mut buffer = Vec::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.try_next().await? {
        buffer.extend_from_slice(&chunk);
        while let Some(newline_idx) = buffer.iter().position(|&b| b == b'\n') {
            let line = buffer.drain(..=newline_idx).collect::<Vec<u8>>();
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            if line.is_empty() {
                continue;
            }
            let event: ShardUploadEvent = serde_json::from_slice(line).map_err(|err| {
                ClientError::Other(format!(
                    "failed to parse shard upload progress frame: {err}; line={}",
                    String::from_utf8_lossy(line)
                ))
            })?;
            if let Some(was_new) = handle_shard_upload_event(event, progress_callback.as_ref())? {
                return Ok(was_new);
            }
        }
    }

    if !buffer.is_empty() {
        let event: ShardUploadEvent = serde_json::from_slice(&buffer).map_err(|err| {
            ClientError::Other(format!(
                "failed to parse trailing shard upload progress frame: {err}; line={}",
                String::from_utf8_lossy(&buffer)
            ))
        })?;
        if let Some(was_new) = handle_shard_upload_event(event, progress_callback.as_ref())? {
            return Ok(was_new);
        }
    }

    Err(ClientError::Other("shard upload stream ended without a terminal done or error frame".into()))
}

fn handle_shard_upload_event(
    event: ShardUploadEvent,
    progress_callback: Option<&ShardUploadProgressCallback>,
) -> Result<Option<bool>> {
    if let Some(callback) = progress_callback {
        callback(ShardUploadProgressType::Response(&event));
    }

    match event {
        ShardUploadEvent::Validating { .. } | ShardUploadEvent::Committing { .. } => Ok(None),
        ShardUploadEvent::Result { result } => Ok(Some(matches!(result, UploadShardResponseType::SyncPerformed))),
        ShardUploadEvent::Error { message } => Err(ClientError::Other(message)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::cas_types::{CommitStage, UploadShardResponseType};

    /// Builds a `reqwest::Response` that streams `chunks` as separate network reads, so tests
    /// can exercise `read_shard_upload_ndjson`'s buffering (e.g. a JSON frame split mid-line
    /// across two chunks) without spinning up a real HTTP server.
    fn response_from_chunks(chunks: Vec<&'static str>) -> reqwest::Response {
        let chunks: Vec<std::result::Result<bytes::Bytes, std::io::Error>> = chunks
            .into_iter()
            .map(|c| Ok(bytes::Bytes::from_static(c.as_bytes())))
            .collect();
        let body = reqwest::Body::wrap_stream(futures::stream::iter(chunks));
        http::Response::builder().status(200).body(body).unwrap().into()
    }

    // === handle_shard_upload_event ===

    #[test]
    fn test_handle_validating_event_is_not_terminal() {
        let result = handle_shard_upload_event(ShardUploadEvent::Validating { verified: 1, total: 2 }, None).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_handle_committing_event_is_not_terminal() {
        let result = handle_shard_upload_event(
            ShardUploadEvent::Committing {
                stage: CommitStage::Syncing,
            },
            None,
        )
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_handle_result_sync_performed_returns_true() {
        let result = handle_shard_upload_event(
            ShardUploadEvent::Result {
                result: UploadShardResponseType::SyncPerformed,
            },
            None,
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[test]
    fn test_handle_result_exists_returns_false() {
        let result = handle_shard_upload_event(
            ShardUploadEvent::Result {
                result: UploadShardResponseType::Exists,
            },
            None,
        )
        .unwrap();
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_handle_error_event_propagates_message() {
        let err = handle_shard_upload_event(
            ShardUploadEvent::Error {
                message: "boom".to_string(),
            },
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ClientError::Other(msg) if msg == "boom"));
    }

    #[test]
    fn test_handle_event_forwards_response_to_callback() {
        let seen = Arc::new(Mutex::new(None));
        let seen_cb = seen.clone();
        let callback: ShardUploadProgressCallback = Arc::new(move |progress| {
            if let ShardUploadProgressType::Response(event) = progress {
                *seen_cb.lock().unwrap() = Some(event.clone());
            }
        });

        handle_shard_upload_event(ShardUploadEvent::Validating { verified: 1, total: 2 }, Some(&callback)).unwrap();

        assert_eq!(*seen.lock().unwrap(), Some(ShardUploadEvent::Validating { verified: 1, total: 2 }));
    }

    // === read_shard_upload_ndjson ===

    #[tokio::test]
    async fn test_read_ndjson_result_sync_performed_returns_true() {
        let resp = response_from_chunks(vec![
            "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n",
            "{\"type\":\"result\",\"result\":1}\n",
        ]);
        assert!(read_shard_upload_ndjson(resp, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_ndjson_result_exists_returns_false() {
        let resp = response_from_chunks(vec!["{\"type\":\"result\",\"result\":0}\n"]);
        assert!(!read_shard_upload_ndjson(resp, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_ndjson_error_frame_propagates_as_err() {
        let resp = response_from_chunks(vec!["{\"type\":\"error\",\"message\":\"boom\"}\n"]);
        let err = read_shard_upload_ndjson(resp, None).await.unwrap_err();
        assert!(matches!(err, ClientError::Other(msg) if msg == "boom"));
    }

    #[tokio::test]
    async fn test_read_ndjson_frame_split_mid_line_across_chunks() {
        // The frame is split in the middle of the JSON object across two network reads.
        let resp = response_from_chunks(vec!["{\"type\":\"result\",\"resul", "t\":1}\n"]);
        assert!(read_shard_upload_ndjson(resp, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_ndjson_trailing_frame_without_newline() {
        // No trailing "\n" after the final frame -- exercises the post-loop buffer flush.
        let resp = response_from_chunks(vec!["{\"type\":\"result\",\"result\":1}"]);
        assert!(read_shard_upload_ndjson(resp, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_ndjson_skips_blank_lines() {
        let resp = response_from_chunks(vec!["\n{\"type\":\"result\",\"result\":1}\n"]);
        assert!(read_shard_upload_ndjson(resp, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_ndjson_missing_terminal_frame_errors() {
        let resp = response_from_chunks(vec!["{\"type\":\"validating\",\"verified\":0,\"total\":1}\n"]);
        let err = read_shard_upload_ndjson(resp, None).await.unwrap_err();
        assert!(matches!(err, ClientError::Other(msg) if msg.contains("without a terminal")));
    }

    #[tokio::test]
    async fn test_read_ndjson_invokes_callback_for_every_frame_in_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let callback: ShardUploadProgressCallback = Arc::new(move |progress| {
            if let ShardUploadProgressType::Response(event) = progress {
                seen_cb.lock().unwrap().push(event.clone());
            }
        });

        let resp = response_from_chunks(vec![
            "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n",
            "{\"type\":\"committing\",\"stage\":\"uploading\"}\n",
            "{\"type\":\"result\",\"result\":1}\n",
        ]);

        assert!(read_shard_upload_ndjson(resp, Some(callback)).await.unwrap());

        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            vec![
                ShardUploadEvent::Validating { verified: 0, total: 1 },
                ShardUploadEvent::Committing {
                    stage: CommitStage::Uploading
                },
                ShardUploadEvent::Result {
                    result: UploadShardResponseType::SyncPerformed
                },
            ]
        );
    }
}
