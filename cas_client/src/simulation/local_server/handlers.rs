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

use super::delay_simulation::DelaySimulation;
use crate::error::CasClientError;
use crate::simulation::DirectAccessClient;

/// Server state passed to all handlers.
#[derive(Clone)]
pub struct ServerState {
    pub client: Arc<dyn DirectAccessClient>,
    pub delay_simulation: Arc<DelaySimulation>,
}

// ServerState is Send + Sync because:
// - Arc<dyn DirectAccessClient> is Send + Sync (DirectAccessClient requires Send + Sync)
// - Arc<DelaySimulation> is Send + Sync (DelaySimulation contains only Send + Sync types)
unsafe impl Send for ServerState {}
unsafe impl Sync for ServerState {}

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
/// Encodes a fetch term for HTTP transport.
///
/// The encoded term contains:
/// - xorb_hash: The XORB hash (hex encoded)
///
/// The byte range to fetch comes from the HTTP Range header, not encoded in the term.
fn encode_term(xorb_hash: &MerkleHash) -> String {
    URL_SAFE_NO_PAD.encode(xorb_hash.hex().as_bytes())
}

/// Decodes a fetch term back into its components.
///
/// Returns the xorb_hash.
fn decode_term(term: &str) -> Result<MerkleHash, String> {
    let bytes = URL_SAFE_NO_PAD.decode(term).map_err(|e| format!("Invalid base64: {e}"))?;
    let hash_hex = String::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8: {e}"))?;
    MerkleHash::from_hex(&hash_hex).map_err(|e| format!("Invalid hash: {e}"))
}

/// Extracts the base URL from request headers (Host header).
fn get_base_url(headers: &HeaderMap) -> String {
    headers
        .get(HOST)
        .and_then(|h| h.to_str().ok())
        .map(|host| format!("http://{host}"))
        .unwrap_or_else(|| "http://localhost".to_string())
}

/// Transforms fetch_info URLs from client-internal format to HTTP URLs.
///
/// DirectAccessClient implementations generate URLs in their own internal format.
/// This function transforms them into HTTP URLs that point to the /v1/fetch_term endpoint.
/// The byte range to fetch comes from url_range (sent as HTTP Range header by client).
fn transform_fetch_info_urls(
    fetch_info: &mut std::collections::HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>,
    base_url: &str,
) {
    for (xorb_hash, fetch_infos) in fetch_info.iter_mut() {
        let xorb_hash: MerkleHash = (*xorb_hash).into();
        let encoded_term = encode_term(&xorb_hash);
        for fi in fetch_infos.iter_mut() {
            fi.url = format!("{base_url}/v1/fetch_term?term={encoded_term}");
        }
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
/// that point to the /v1/fetch_term endpoint.
pub async fn get_reconstruction(
    State(state): State<ServerState>,
    Path(HexMerkleHash(file_id)): Path<HexMerkleHash>,
    headers: HeaderMap,
) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }

    let base_url = get_base_url(&headers);

    let range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => {
            let file_size = match state.client.get_file_size(&file_id).await {
                Ok(size) => size,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(start, file_size))
        },
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            let file_size = match state.client.get_file_size(&file_id).await {
                Ok(size) => size,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(file_size.saturating_sub(suffix), file_size))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    match state.client.get_reconstruction(&file_id, range).await {
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
    State(state): State<ServerState>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
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

    match state.client.batch_get_reconstruction(&file_ids).await {
        Ok(mut response) => {
            transform_fetch_info_urls(&mut response.fetch_info, &base_url);
            Json(response).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

/// GET /v1/fetch_term?term=<base64_encoded_term>
///
/// Fetches raw XORB data based on an encoded term.
/// The term contains the xorb hash. The byte range is specified via HTTP Range header.
///
/// This endpoint is called by RemoteClient when fetching reconstruction terms.
/// It returns raw (compressed) bytes that the client will decompress.
pub async fn fetch_term(State(state): State<ServerState>, uri: axum::http::Uri, headers: HeaderMap) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
    // Extract 'term' query parameter
    let term = uri.query().unwrap_or("").split('&').find_map(|param| {
        let (key, value) = param.split_once('=')?;
        if key == "term" { Some(value.to_string()) } else { None }
    });

    let Some(term) = term else {
        return (StatusCode::BAD_REQUEST, "Missing 'term' query parameter").into_response();
    };

    let xorb_hash = match decode_term(&term) {
        Ok(h) => h,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid term: {e}")).into_response(),
    };

    // Get total length of the raw XORB data for Range header handling
    let total_length = match state.client.xorb_raw_length(&xorb_hash).await {
        Ok(len) => len,
        Err(e) => return error_to_response(e),
    };

    // Parse HTTP Range header to determine which bytes to fetch
    let byte_range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => Some(FileRange::new(start, total_length)),
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            Some(FileRange::new(total_length.saturating_sub(suffix), total_length))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    // Fetch raw (serialized/compressed) bytes from the XORB
    match state.client.get_xorb_raw_bytes(&xorb_hash, byte_range).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

/// GET /v1/chunks/{prefix}/{hash}
///
/// Query for a global deduplication shard by chunk hash.
/// Returns the shard data if found, 404 otherwise.
pub async fn get_dedup_info_by_chunk(State(state): State<ServerState>, Path(key): Path<HexKey>) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
    match state.client.query_for_global_dedup_shard(&key.prefix, &key.hash).await {
        Ok(Some(data)) => (StatusCode::OK, data).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Shard not found").into_response(),
        Err(e) => error_to_response(e),
    }
}

/// HEAD /v1/xorbs/{prefix}/{hash}
///
/// Check if a XORB exists in the store.
/// Returns 200 if found, 404 otherwise.
pub async fn head_xorb(State(state): State<ServerState>, Path(key): Path<HexKey>) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
    match state.client.get_file_reconstruction_info(&key.hash).await {
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
pub async fn post_xorb(State(state): State<ServerState>, Path(key): Path<HexKey>, body: Body) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }

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

    let permit = match state.client.acquire_upload_permit().await {
        Ok(p) => p,
        Err(e) => return error_to_response(e),
    };

    match state.client.upload_xorb(&key.prefix, cas_object, None, permit).await {
        Ok(_) => Json(UploadXorbResponse { was_inserted: true }).into_response(),
        Err(e) => error_to_response(e),
    }
}

/// POST /v1/shards
///
/// Upload a shard (deduplication index) to the store.
/// Request body: Raw shard data
/// Response: JSON indicating if the shard was newly inserted or already existed
pub async fn post_shard(State(state): State<ServerState>, body: Body) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
    let data = match collect_body(body).await {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    let permit = match state.client.acquire_upload_permit().await {
        Ok(p) => p,
        Err(e) => return error_to_response(e),
    };

    match state.client.upload_shard(data, permit).await {
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
    State(state): State<ServerState>,
    Path(HexMerkleHash(file_id)): Path<HexMerkleHash>,
) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
    match state.client.get_file_size(&file_id).await {
        Ok(size) => {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(size));
            (StatusCode::OK, headers).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

/// GET /v1/get_xorb/{prefix}/{hash}/
///
/// Download XORB data directly.
/// Supports Range header for partial downloads.
pub async fn get_file_term_data(
    State(state): State<ServerState>,
    Path((_prefix, hash_str)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }
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

    match state.client.get_file_data(&hash, range).await {
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

/// Parses a duration range string in the format "(min, max)" where min and max are duration strings.
///
/// Supports duration formats like "10ms", "1s", "500us", etc. (via duration_str crate).
///
/// Examples:
/// - "(10ms, 100ms)" -> Duration range from 10ms to 100ms
/// - "(1s, 5s)" -> Duration range from 1s to 5s
#[cfg(test)]
fn parse_duration_range(value: &str) -> Result<std::ops::Range<std::time::Duration>, String> {
    let trimmed = value.trim();

    // Remove parentheses if present
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    // Split by comma
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(format!("Expected format '(min, max)' with two duration values, got: {value}"));
    }

    let min = duration_str::parse(parts[0]).map_err(|e| format!("Invalid min duration '{}': {e}", parts[0]))?;
    let max = duration_str::parse(parts[1]).map_err(|e| format!("Invalid max duration '{}': {e}", parts[1]))?;

    if min > max {
        return Err(format!("Min duration ({:?}) cannot be greater than max ({:?})", min, max));
    }

    Ok(min..max)
}

pub async fn set_config(State(state): State<ServerState>, uri: axum::http::Uri, body: Body) -> Response {
    use super::delay_simulation::ServerDelayProfile;

    // Try to parse as JSON body first
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => Bytes::new(),
    };

    if !body_bytes.is_empty() {
        match serde_json::from_slice::<ServerDelayProfile>(&body_bytes) {
            Ok(profile) => {
                state.delay_simulation.update_profile(profile).await;
                return (StatusCode::OK, "Profile updated").into_response();
            },
            Err(_) => {
                // If JSON parsing fails, fall through to query param parsing
            },
        }
    }

    // Fall back to query parameter parsing (legacy format)
    let query = uri.query().unwrap_or("");
    let params: std::collections::HashMap<String, String> = query
        .split('&')
        .filter_map(|param| {
            let (key, value) = param.split_once('=')?;
            Some((key.to_string(), urlencoding::decode(value).ok()?.into_owned()))
        })
        .collect();

    let Some(config_name) = params.get("config") else {
        return (StatusCode::BAD_REQUEST, "Missing 'config' query parameter or JSON body").into_response();
    };

    let Some(value) = params.get("value") else {
        return (StatusCode::BAD_REQUEST, "Missing 'value' query parameter").into_response();
    };

    match config_name.to_lowercase().as_str() {
        "congestion" => match parse_congestion_config(value) {
            Ok((threshold, min_ms, max_ms, error_rate)) => {
                let profile = ServerDelayProfile {
                    random_delay_ms: None,
                    connection_threshold: Some(threshold),
                    min_congestion_penalty_ms: Some(min_ms),
                    max_congestion_penalty_ms: Some(max_ms),
                    congestion_error_rate: Some(error_rate),
                };
                state.delay_simulation.update_profile(profile).await;
                (StatusCode::OK, "Congestion config set").into_response()
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid congestion value: {e}")).into_response(),
        },
        "random_delay" => match parse_random_delay_value(value) {
            Ok((min_ms, max_ms)) => {
                let profile = ServerDelayProfile {
                    random_delay_ms: Some((min_ms, max_ms)),
                    connection_threshold: None,
                    min_congestion_penalty_ms: None,
                    max_congestion_penalty_ms: None,
                    congestion_error_rate: None,
                };
                state.delay_simulation.update_profile(profile).await;
                (StatusCode::OK, "Random delay config set").into_response()
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid random_delay value: {e}")).into_response(),
        },
        _ => (
            StatusCode::BAD_REQUEST,
            format!("Unknown config: {config_name}. Supported: congestion, random_delay"),
        )
            .into_response(),
    }
}

/// Parse random_delay value in format "(min, max)" where min and max are duration strings (e.g. "10ms", "100ms").
/// Returns (min_ms, max_ms).
fn parse_random_delay_value(value: &str) -> Result<(u64, u64), String> {
    let trimmed = value.trim();
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        trimmed[1..trimmed.len() - 1].trim()
    } else {
        trimmed
    };
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(format!("Expected format '(min, max)' with two duration values, got: {value}"));
    }
    let min_dur = duration_str::parse(parts[0]).map_err(|e| format!("Invalid min duration '{}': {e}", parts[0]))?;
    let max_dur = duration_str::parse(parts[1]).map_err(|e| format!("Invalid max duration '{}': {e}", parts[1]))?;
    if min_dur > max_dur {
        return Err("Min duration cannot be greater than max".to_string());
    }
    let min_ms = min_dur.as_millis().try_into().map_err(|_| "Duration too large".to_string())?;
    let max_ms = max_dur.as_millis().try_into().map_err(|_| "Duration too large".to_string())?;
    Ok((min_ms, max_ms))
}

/// Parse congestion config from format: "connection_threshold,min_penalty_ms,max_penalty_ms,error_rate"
/// Returns (threshold, min_ms, max_ms, error_rate)
fn parse_congestion_config(value: &str) -> Result<(u64, u64, u64, f64), String> {
    let parts: Vec<&str> = value.split(',').map(|s| s.trim()).collect();
    if parts.len() != 4 {
        return Err("Expected format: connection_threshold,min_penalty_ms,max_penalty_ms,error_rate".to_string());
    }

    let connection_threshold = parts[0]
        .parse::<u64>()
        .map_err(|e| format!("Invalid connection_threshold: {e}"))?;
    let min_penalty_ms = parts[1].parse::<u64>().map_err(|e| format!("Invalid min_penalty_ms: {e}"))?;
    let max_penalty_ms = parts[2].parse::<u64>().map_err(|e| format!("Invalid max_penalty_ms: {e}"))?;
    let error_rate = parts[3].parse::<f64>().map_err(|e| format!("Invalid error_rate: {e}"))?;

    if !(0.0..=1.0).contains(&error_rate) {
        return Err("error_rate must be between 0.0 and 1.0".to_string());
    }

    Ok((connection_threshold, min_penalty_ms, max_penalty_ms, error_rate))
}

/// POST /simulation/dummy_upload
///
/// Accepts an upload stream and discards all data. Returns after applying the configured delays.
/// This is useful for testing upload throughput without actual storage overhead.
///
/// If congestion is configured and active connections exceed the threshold:
/// - May return INTERNAL_SERVER_ERROR with probability `error_rate`
/// - Otherwise applies an additional penalty delay
pub async fn dummy_upload(State(state): State<ServerState>, body: Body) -> Response {
    let connection_guard = state.delay_simulation.register_connection().await;
    if let Some(simulated_error) = connection_guard.simulate_error() {
        return simulated_error;
    }

    // Consume and discard the entire body
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        if let Err(e) = chunk {
            return (StatusCode::BAD_REQUEST, format!("Error reading body: {e}")).into_response();
        }
        // Discard the chunk
    }

    (StatusCode::OK, "Upload discarded").into_response()
    // connection_guard dropped here, decrementing active connection count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_term() {
        let xorb_hash = MerkleHash::from_hex(&format!("{:0>64}", "abc123")).unwrap();

        let encoded = encode_term(&xorb_hash);
        let decoded_hash = decode_term(&encoded).unwrap();

        assert_eq!(decoded_hash, xorb_hash);
    }

    #[test]
    fn test_parse_duration_range() {
        use std::time::Duration;

        // Basic format with parentheses
        let range = super::parse_duration_range("(10ms, 100ms)").unwrap();
        assert_eq!(range.start, Duration::from_millis(10));
        assert_eq!(range.end, Duration::from_millis(100));

        // Without parentheses
        let range = super::parse_duration_range("50ms, 200ms").unwrap();
        assert_eq!(range.start, Duration::from_millis(50));
        assert_eq!(range.end, Duration::from_millis(200));

        // With extra whitespace
        let range = super::parse_duration_range("  (  1s  ,  5s  )  ").unwrap();
        assert_eq!(range.start, Duration::from_secs(1));
        assert_eq!(range.end, Duration::from_secs(5));

        // Same min and max
        let range = super::parse_duration_range("(100ms, 100ms)").unwrap();
        assert_eq!(range.start, Duration::from_millis(100));
        assert_eq!(range.end, Duration::from_millis(100));

        // Error: min > max
        assert!(super::parse_duration_range("(200ms, 100ms)").is_err());

        // Error: invalid format
        assert!(super::parse_duration_range("100ms").is_err());
        assert!(super::parse_duration_range("(100ms)").is_err());

        // Error: invalid duration string
        assert!(super::parse_duration_range("(invalid, 100ms)").is_err());
    }
}
