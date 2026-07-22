use futures::TryStreamExt;
use reqwest::Response;

use super::interface::{ShardUploadProgressCallback, ShardUploadProgressType};
use super::retry_wrapper::RetryableReqwestError;
use crate::cas_types::ShardUploadEvent;
use crate::error::ClientError;

/// Parse the `/v2/shards` NDJSON response stream until a terminal `result` or `error` frame.
///
/// Returns [`RetryableReqwestError`] so callers can fold this into [`RetryWrapper::run_and_process`]:
/// terminal `error` frames with `retryable: true` (and transient stream I/O failures) retry the
/// whole upload; other failures are fatal. V2 success has no Exists/SyncPerformed payload.
pub(crate) async fn read_shard_upload_ndjson(
    response: Response,
    progress_callback: Option<ShardUploadProgressCallback>,
) -> std::result::Result<(), RetryableReqwestError> {
    let mut buffer = Vec::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.try_next().await.map_err(map_stream_io_error)? {
        buffer.extend_from_slice(&chunk);
        while let Some(newline_idx) = buffer.iter().position(|&b| b == b'\n') {
            let line = buffer.drain(..=newline_idx).collect::<Vec<u8>>();
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            if line.is_empty() {
                continue;
            }
            let event: ShardUploadEvent = serde_json::from_slice(line).map_err(|err| {
                RetryableReqwestError::FatalError(ClientError::InvalidResponse(format!(
                    "failed to parse shard upload progress frame: {err}; line={}",
                    String::from_utf8_lossy(line)
                )))
            })?;
            if handle_shard_upload_event(event, progress_callback.as_ref())? {
                return Ok(());
            }
        }
    }

    if !buffer.is_empty() {
        let event: ShardUploadEvent = serde_json::from_slice(&buffer).map_err(|err| {
            RetryableReqwestError::FatalError(ClientError::InvalidResponse(format!(
                "failed to parse trailing shard upload progress frame: {err}; line={}",
                String::from_utf8_lossy(&buffer)
            )))
        })?;
        if handle_shard_upload_event(event, progress_callback.as_ref())? {
            return Ok(());
        }
    }

    Err(RetryableReqwestError::FatalError(ClientError::InvalidResponse(
        "shard upload stream ended without a terminal done or error frame".into(),
    )))
}

fn map_stream_io_error(err: reqwest::Error) -> RetryableReqwestError {
    // Match `RetryWrapper::run_and_extract_json` body/decode classification.
    if err.is_connect() || err.is_decode() || err.is_body() || err.is_timeout() {
        RetryableReqwestError::RetryableError(err.into())
    } else {
        RetryableReqwestError::FatalError(err.into())
    }
}

/// Returns `true` when a terminal success (`result`) frame was received.
fn handle_shard_upload_event(
    event: ShardUploadEvent,
    progress_callback: Option<&ShardUploadProgressCallback>,
) -> std::result::Result<bool, RetryableReqwestError> {
    if let Some(callback) = progress_callback {
        callback(ShardUploadProgressType::Response(&event));
    }

    match event {
        ShardUploadEvent::Validating { .. } | ShardUploadEvent::Committing { .. } | ShardUploadEvent::Unknown => {
            Ok(false)
        },
        ShardUploadEvent::Result => Ok(true),
        ShardUploadEvent::Error { message, retryable } => {
            let err = ClientError::Other(message);
            if retryable {
                Err(RetryableReqwestError::RetryableError(err))
            } else {
                Err(RetryableReqwestError::FatalError(err))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::cas_types::CommitStage;

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
        assert!(!result);
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
        assert!(!result);
    }

    #[test]
    fn test_handle_result_is_terminal() {
        let result = handle_shard_upload_event(ShardUploadEvent::Result, None).unwrap();
        assert!(result);
    }

    #[test]
    fn test_handle_unknown_event_is_not_terminal() {
        let result = handle_shard_upload_event(ShardUploadEvent::Unknown, None).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_handle_unknown_event_forwards_to_callback() {
        let seen = Arc::new(Mutex::new(None));
        let seen_cb = seen.clone();
        let callback: ShardUploadProgressCallback = Arc::new(move |progress| {
            if let ShardUploadProgressType::Response(event) = progress {
                *seen_cb.lock().unwrap() = Some(event.clone());
            }
        });

        assert!(!handle_shard_upload_event(ShardUploadEvent::Unknown, Some(&callback)).unwrap());
        assert_eq!(*seen.lock().unwrap(), Some(ShardUploadEvent::Unknown));
    }

    #[test]
    fn test_handle_error_event_is_fatal() {
        let err = handle_shard_upload_event(
            ShardUploadEvent::Error {
                message: "boom".to_string(),
                retryable: false,
            },
            None,
        )
        .unwrap_err();
        assert!(matches!(err, RetryableReqwestError::FatalError(ClientError::Other(msg)) if msg == "boom"));
    }

    #[test]
    fn test_handle_retryable_error_event() {
        let err = handle_shard_upload_event(
            ShardUploadEvent::Error {
                message: "transient".to_string(),
                retryable: true,
            },
            None,
        )
        .unwrap_err();
        assert!(matches!(err, RetryableReqwestError::RetryableError(ClientError::Other(msg)) if msg == "transient"));
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
    async fn test_read_ndjson_result_succeeds() {
        let resp = response_from_chunks(vec![
            "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n",
            "{\"type\":\"result\"}\n",
        ]);
        read_shard_upload_ndjson(resp, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_ndjson_error_frame_is_fatal() {
        let resp = response_from_chunks(vec!["{\"type\":\"error\",\"message\":\"boom\",\"retryable\":false}\n"]);
        let err = read_shard_upload_ndjson(resp, None).await.unwrap_err();
        assert!(matches!(err, RetryableReqwestError::FatalError(ClientError::Other(msg)) if msg == "boom"));
    }

    #[tokio::test]
    async fn test_read_ndjson_retryable_error_frame() {
        let resp = response_from_chunks(vec!["{\"type\":\"error\",\"message\":\"reset\",\"retryable\":true}\n"]);
        let err = read_shard_upload_ndjson(resp, None).await.unwrap_err();
        assert!(matches!(err, RetryableReqwestError::RetryableError(ClientError::Other(msg)) if msg == "reset"));
    }

    #[tokio::test]
    async fn test_read_ndjson_frame_split_mid_line_across_chunks() {
        // The frame is split in the middle of the JSON object across two network reads.
        let resp = response_from_chunks(vec!["{\"type\":\"resul", "t\"}\n"]);
        read_shard_upload_ndjson(resp, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_ndjson_trailing_frame_without_newline() {
        // No trailing "\n" after the final frame -- exercises the post-loop buffer flush.
        let resp = response_from_chunks(vec!["{\"type\":\"result\"}"]);
        read_shard_upload_ndjson(resp, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_ndjson_skips_blank_lines() {
        let resp = response_from_chunks(vec!["\n{\"type\":\"result\"}\n"]);
        read_shard_upload_ndjson(resp, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_ndjson_skips_unknown_frame_types() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let callback: ShardUploadProgressCallback = Arc::new(move |progress| {
            if let ShardUploadProgressType::Response(event) = progress {
                seen_cb.lock().unwrap().push(event.clone());
            }
        });

        let resp = response_from_chunks(vec![
            "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n",
            "{\"type\":\"heartbeat\"}\n",
            "{\"type\":\"result\"}\n",
        ]);
        read_shard_upload_ndjson(resp, Some(callback)).await.unwrap();

        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ShardUploadEvent::Validating { verified: 0, total: 1 },
                ShardUploadEvent::Unknown,
                ShardUploadEvent::Result,
            ]
        );
    }

    #[tokio::test]
    async fn test_read_ndjson_missing_terminal_frame_errors() {
        let resp = response_from_chunks(vec!["{\"type\":\"validating\",\"verified\":0,\"total\":1}\n"]);
        let err = read_shard_upload_ndjson(resp, None).await.unwrap_err();
        assert!(matches!(
            err,
            RetryableReqwestError::FatalError(ClientError::InvalidResponse(msg)) if msg.contains("without a terminal")
        ));
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
            "{\"type\":\"result\"}\n",
        ]);

        read_shard_upload_ndjson(resp, Some(callback)).await.unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            vec![
                ShardUploadEvent::Validating { verified: 0, total: 1 },
                ShardUploadEvent::Committing {
                    stage: CommitStage::Uploading
                },
                ShardUploadEvent::Result,
            ]
        );
    }
}
