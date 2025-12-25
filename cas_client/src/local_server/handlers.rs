//! HTTP Request Handlers for the Local CAS Server
//!
//! This module contains all the Axum request handlers that bridge HTTP requests
//! to `LocalClient` operations. Each handler corresponds to an endpoint in the
//! CAS REST API that `RemoteClient` expects.
//!
//! # Handler Pattern
//!
//! All handlers follow this pattern:
//! 1. Extract request data (path parameters, headers, body)
//! 2. Call the appropriate `LocalClient` method
//! 3. Convert the result to an HTTP response
//!
//! Errors are mapped to appropriate HTTP status codes via `error_to_response`.

use std::path::PathBuf;
use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::HOST;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use cas_types::{
    CASReconstructionFetchInfo, FileRange, HexKey, HexMerkleHash, UploadShardResponse, UploadShardResponseType,
    UploadXorbResponse,
};
use futures_util::StreamExt;
use http::header::RANGE;
use merklehash::MerkleHash;

use crate::error::CasClientError;
use crate::{Client, LocalClient};

/// Represents the different forms a Range header can take.
pub enum FileRangeVariant {
    /// Standard byte range: bytes=start-end (inclusive end, converted to exclusive)
    Normal(FileRange),
    /// Open-ended range: bytes=start- (from start to end of file)
    OpenRHS(u64),
    /// Suffix range: bytes=-N (last N bytes of file)
    Suffix(u64),
}

/// Parses an HTTP Range header into a FileRangeVariant.
///
/// Supports the following formats per RFC 7233:
/// - `bytes=0-499` - First 500 bytes
/// - `bytes=500-` - From byte 500 to end
/// - `bytes=-500` - Last 500 bytes
///
/// Returns `Ok(None)` if no Range header is present.
fn parse_range_header(range_header: Option<&HeaderValue>) -> Result<Option<FileRangeVariant>, (StatusCode, String)> {
    let Some(range_header) = range_header else {
        return Ok(None);
    };

    const RANGE_PREFIX: &str = "bytes=";
    let range_str = range_header
        .to_str()
        .map_err(|e| (StatusCode::RANGE_NOT_SATISFIABLE, format!("Invalid range header: {e}")))?;

    if !range_str.starts_with(RANGE_PREFIX) {
        return Err((StatusCode::RANGE_NOT_SATISFIABLE, format!("Range header doesn't start with {RANGE_PREFIX}")));
    }

    let split = range_str[RANGE_PREFIX.len()..].splitn(2, '-').collect::<Vec<_>>();
    if split.len() != 2 {
        return Err((StatusCode::RANGE_NOT_SATISFIABLE, "Invalid range syntax".to_string()));
    }

    let start_value = if split[0].is_empty() {
        None
    } else {
        Some(
            split[0]
                .parse::<u64>()
                .map_err(|e| (StatusCode::RANGE_NOT_SATISFIABLE, format!("Invalid range start: {e}")))?,
        )
    };
    let end_value = if split[1].is_empty() {
        None
    } else {
        Some(
            split[1]
                .parse::<u64>()
                .map_err(|e| (StatusCode::RANGE_NOT_SATISFIABLE, format!("Invalid range end: {e}")))?,
        )
    };

    match (start_value, end_value) {
        (None, None) => Err((StatusCode::RANGE_NOT_SATISFIABLE, "Invalid range syntax".to_string())),
        (Some(start), Some(end)) => {
            if start > end {
                Err((StatusCode::RANGE_NOT_SATISFIABLE, "Range start > end".to_string()))
            } else {
                // HTTP ranges are inclusive on both ends; FileRange uses exclusive end
                Ok(Some(FileRangeVariant::Normal(FileRange::new(start, end + 1))))
            }
        },
        (Some(start), None) => Ok(Some(FileRangeVariant::OpenRHS(start))),
        (None, Some(suffix_len)) => Ok(Some(FileRangeVariant::Suffix(suffix_len))),
    }
}

/// Maps CasClientError to appropriate HTTP status codes.
fn error_to_response(e: CasClientError) -> Response {
    let status = match &e {
        CasClientError::XORBNotFound(_) | CasClientError::FileNotFound(_) => StatusCode::NOT_FOUND,
        CasClientError::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
        CasClientError::InvalidArguments => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, e.to_string()).into_response()
}

/// Encodes term data (file path) into a URL-safe base64 string.
///
/// The term encodes the local file path that the LocalClient uses.
/// This allows the fetch_term endpoint to retrieve the data.
fn encode_term(file_path: &str) -> String {
    URL_SAFE_NO_PAD.encode(file_path.as_bytes())
}

/// Decodes a URL-safe base64 term string back into file path.
fn decode_term(term: &str) -> Result<PathBuf, String> {
    let bytes = URL_SAFE_NO_PAD.decode(term).map_err(|e| format!("Invalid base64: {e}"))?;
    let file_path = String::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8: {e}"))?;
    Ok(PathBuf::from(file_path))
}

/// Extracts the base URL from request headers (Host header).
fn get_base_url(headers: &HeaderMap) -> String {
    headers
        .get(HOST)
        .and_then(|h| h.to_str().ok())
        .map(|host| format!("http://{host}"))
        .unwrap_or_else(|| "http://localhost".to_string())
}

/// Transforms fetch_info URLs from local file paths to HTTP URLs.
///
/// LocalClient generates URLs in a local format. This function transforms them
/// into proper HTTP URLs that point to the /fetch_term endpoint.
fn transform_fetch_info_urls(
    fetch_info: &mut std::collections::HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>,
    base_url: &str,
) {
    for fetch_infos in fetch_info.values_mut() {
        for fi in fetch_infos.iter_mut() {
            // The original URL from LocalClient is in the format:
            // "/path/to/file":start:end:timestamp
            // We extract the file path and encode it for the HTTP URL.
            // The byte range is already in url_range, so we just need the file path.

            // Parse the local URL format to extract the file path
            let file_path = extract_file_path_from_local_url(&fi.url);

            // Create the HTTP URL with the encoded term
            let encoded_term = encode_term(&file_path);
            fi.url = format!("{base_url}/fetch_term?term={encoded_term}");
        }
    }
}

/// Extracts the file path from LocalClient's URL format.
///
/// LocalClient generates URLs like: "/path/to/file":start:end:timestamp
/// This extracts just the file path portion.
fn extract_file_path_from_local_url(local_url: &str) -> String {
    // The format is: "path":start:end:timestamp
    // We need to extract the path, which is quoted
    let mut parts = local_url.rsplitn(4, ':').collect::<Vec<_>>();
    parts.reverse();

    if !parts.is_empty() {
        // Remove the quotes from the path
        parts[0].trim_matches('"').to_string()
    } else {
        local_url.to_string()
    }
}

/// GET /v1/reconstructions/{file_id}
///
/// Returns reconstruction information for a file, including:
/// - List of terms (chunks) needed to reconstruct the file
/// - Fetch info with URLs/locations for each XORB
///
/// Supports Range header for partial file reconstruction.
///
/// The URLs in fetch_info are transformed from local file paths to HTTP URLs
/// that point to the /fetch_term endpoint.
pub async fn get_reconstruction(
    State(state): State<Arc<LocalClient>>,
    Path(HexMerkleHash(file_id)): Path<HexMerkleHash>,
    headers: HeaderMap,
) -> Response {
    let base_url = get_base_url(&headers);

    let range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => {
            let file_size = match state.get_file_size(&file_id).await {
                Ok(size) => size,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(start, file_size))
        },
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            let file_size = match state.get_file_size(&file_id).await {
                Ok(size) => size,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(file_size.saturating_sub(suffix), file_size))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    match state.get_reconstruction(&file_id, range).await {
        Ok(Some(mut response)) => {
            transform_fetch_info_urls(&mut response.fetch_info, &base_url);
            Json(response).into_response()
        },
        Ok(None) => (StatusCode::RANGE_NOT_SATISFIABLE, "Range not satisfiable").into_response(),
        Err(e) => error_to_response(e),
    }
}

/// GET /reconstructions?file_id=...&file_id=...
///
/// Batch query for reconstruction information for multiple files using query parameters.
/// This is the format used by RemoteClient.
/// Query params: file_id (repeated for each file hash as hex string)
/// Response: Map of file ID -> reconstruction info
///
/// The URLs in fetch_info are transformed from local file paths to HTTP URLs.
pub async fn batch_get_reconstruction(
    State(state): State<Arc<LocalClient>>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    let base_url = get_base_url(&headers);

    // Parse repeated file_id query parameters
    let file_id_strings: Vec<String> = uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|param| {
            let (key, value) = param.split_once('=')?;
            if key == "file_id" {
                Some(value.to_string())
            } else {
                None
            }
        })
        .collect();

    let file_ids: Vec<MerkleHash> = file_id_strings
        .iter()
        .filter_map(|hex| MerkleHash::from_hex(hex).ok())
        .collect();

    if file_ids.is_empty() && !file_id_strings.is_empty() {
        return (StatusCode::BAD_REQUEST, "Invalid file_id format").into_response();
    }

    match state.batch_get_reconstruction(&file_ids).await {
        Ok(mut response) => {
            transform_fetch_info_urls(&mut response.fetch_info, &base_url);
            Json(response).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

/// GET /fetch_term?term=<base64_encoded_path>
///
/// Fetches XORB data based on an encoded term.
/// The term is a URL-safe base64-encoded file path.
/// Supports Range header for partial downloads.
///
/// This endpoint is called by RemoteClient when fetching reconstruction terms.
pub async fn fetch_term(State(_state): State<Arc<LocalClient>>, uri: axum::http::Uri, headers: HeaderMap) -> Response {
    // Extract 'term' query parameter
    let term = uri.query().unwrap_or("").split('&').find_map(|param| {
        let (key, value) = param.split_once('=')?;
        if key == "term" { Some(value.to_string()) } else { None }
    });

    let Some(term) = term else {
        return (StatusCode::BAD_REQUEST, "Missing 'term' query parameter").into_response();
    };

    let file_path = match decode_term(&term) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid term: {e}")).into_response(),
    };

    // Read the file directly from disk
    let data = match std::fs::read(&file_path) {
        Ok(d) => d,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return (StatusCode::NOT_FOUND, "Term data not found").into_response();
            }
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read data: {e}")).into_response();
        },
    };

    // Apply range if specified
    let range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => Some(FileRange::new(start, data.len() as u64)),
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            let len = data.len() as u64;
            Some(FileRange::new(len.saturating_sub(suffix), len))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let response_data = if let Some(range) = range {
        let start = range.start as usize;
        let end = (range.end as usize).min(data.len());
        if start >= data.len() {
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range start out of bounds").into_response();
        }
        data[start..end].to_vec()
    } else {
        data
    };

    (StatusCode::OK, response_data).into_response()
}

/// GET /v1/chunks/{prefix}/{hash}
///
/// Query for a global deduplication shard by chunk hash.
/// Returns the shard data if found, 404 otherwise.
pub async fn get_dedup_info_by_chunk(State(state): State<Arc<LocalClient>>, Path(key): Path<HexKey>) -> Response {
    match state.query_for_global_dedup_shard(&key.prefix, &key.hash).await {
        Ok(Some(data)) => (StatusCode::OK, data).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Shard not found").into_response(),
        Err(e) => error_to_response(e),
    }
}

/// HEAD /v1/xorbs/{prefix}/{hash}
///
/// Check if a XORB exists in the store.
/// Returns 200 if found, 404 otherwise.
pub async fn head_xorb(State(state): State<Arc<LocalClient>>, Path(key): Path<HexKey>) -> Response {
    match state.get_file_reconstruction_info(&key.hash).await {
        Ok(Some(_)) => {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(0));
            (StatusCode::OK, headers).into_response()
        },
        Ok(None) => (StatusCode::NOT_FOUND, "XORB not found").into_response(),
        Err(e) => error_to_response(e),
    }
}

/// POST /v1/xorbs/{prefix}/{hash}
///
/// Upload a XORB (content-addressed block) to the store.
/// Request body: Serialized CAS object data
/// Response: JSON indicating if the XORB was newly inserted
pub async fn post_xorb(State(state): State<Arc<LocalClient>>, Path(key): Path<HexKey>, body: Body) -> Response {
    let data = match collect_body(body).await {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    let cas_object = cas_object::SerializedCasObject {
        hash: key.hash,
        serialized_data: data.to_vec(),
        raw_num_bytes: data.len() as u64,
        num_chunks: 0,
        footer_start: None,
    };

    let permit = match state.acquire_upload_permit().await {
        Ok(p) => p,
        Err(e) => return error_to_response(e),
    };

    match state.upload_xorb(&key.prefix, cas_object, None, permit).await {
        Ok(_) => Json(UploadXorbResponse { was_inserted: true }).into_response(),
        Err(e) => error_to_response(e),
    }
}

/// POST /shards
///
/// Upload a shard (deduplication index) to the store.
/// Request body: Raw shard data
/// Response: JSON indicating if the shard was newly inserted or already existed
pub async fn post_shard(State(state): State<Arc<LocalClient>>, body: Body) -> Response {
    let data = match collect_body(body).await {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    let permit = match state.acquire_upload_permit().await {
        Ok(p) => p,
        Err(e) => return error_to_response(e),
    };

    match state.upload_shard(data, permit).await {
        Ok(was_new) => {
            let result = if was_new {
                UploadShardResponseType::SyncPerformed
            } else {
                UploadShardResponseType::Exists
            };
            Json(UploadShardResponse { result }).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

/// HEAD /v1/files/{file_id}
///
/// Get the size of a file.
/// Returns Content-Length header with file size if found, 404 otherwise.
pub async fn head_file(
    State(state): State<Arc<LocalClient>>,
    Path(HexMerkleHash(file_id)): Path<HexMerkleHash>,
) -> Response {
    match state.get_file_size(&file_id).await {
        Ok(size) => {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(size));
            (StatusCode::OK, headers).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

/// GET /get_xorb/{prefix}/{hash}/
///
/// Download XORB data directly.
/// Supports Range header for partial downloads.
pub async fn get_file_term_data(
    State(state): State<Arc<LocalClient>>,
    Path((_prefix, hash_str)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let hash = match MerkleHash::from_hex(&hash_str) {
        Ok(h) => h,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid hash").into_response(),
    };

    let range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(_)) => return (StatusCode::RANGE_NOT_SATISFIABLE, "Unsupported range type").into_response(),
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    match state.get_file_data(&hash, range).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

/// GET /health
///
/// Health check endpoint. Always returns 200 OK with no-cache headers.
/// Used by load balancers and monitoring systems to verify server availability.
pub async fn health_check() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate, proxy-revalidate"),
    );
    headers.insert(http::header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(http::header::EXPIRES, HeaderValue::from_static("0"));

    (StatusCode::OK, headers).into_response()
}

/// Collects the entire request body into a Bytes buffer.
async fn collect_body(body: Body) -> Result<Bytes, String> {
    let mut stream = body.into_data_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(c) => data.extend_from_slice(&c),
            Err(e) => return Err(format!("Error reading body: {e}")),
        }
    }
    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_term() {
        let file_path = "/tmp/test/data/xorbs/abc123def456.xorb";
        let encoded = encode_term(file_path);
        let decoded = decode_term(&encoded).unwrap();
        assert_eq!(decoded.to_str().unwrap(), file_path);
    }

    #[test]
    fn test_extract_file_path_from_local_url() {
        let local_url = "\"/tmp/test/data/xorbs/abc123.xorb\":100:200:1234567890";
        let file_path = extract_file_path_from_local_url(local_url);
        assert_eq!(file_path, "/tmp/test/data/xorbs/abc123.xorb");
    }
}
