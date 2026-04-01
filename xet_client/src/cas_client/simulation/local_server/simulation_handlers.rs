use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use http::header::RANGE;

use super::handlers::{FileRangeVariant, ServerState, error_to_response, parse_range_header};
use super::simulation_types::{
    ConfigDelayRangeRequest, ConfigDurationRequest, FetchTermDataRequest, FetchTermDataResponse, FileShardsEntry,
    FileSizeResponse, XorbExistsResponse, XorbLengthResponse, XorbRangesRequest, XorbRangesResponse,
    XorbRawLengthResponse,
};
use crate::cas_types::{FileRange, HexMerkleHash};

fn not_implemented() -> Response {
    (StatusCode::NOT_IMPLEMENTED, "Deletion controls not available for this server backend").into_response()
}

/// Builds the `/simulation/` sub-router.
pub fn simulation_routes() -> Router<ServerState> {
    Router::new()
        // DirectAccessClient routes
        .route("/xorbs", get(list_xorbs))
        .route("/xorbs/{hash}", delete(delete_xorb).get(get_full_xorb))
        .route("/xorbs/{hash}/ranges", post(get_xorb_ranges))
        .route("/xorbs/{hash}/length", get(xorb_length))
        .route("/xorbs/{hash}/exists", get(xorb_exists))
        .route("/xorbs/{hash}/raw", get(get_xorb_raw_bytes))
        .route("/xorbs/{hash}/raw_length", get(xorb_raw_length))
        .route("/files/{hash}/size", get(get_file_size))
        .route("/files/{hash}/data", get(get_file_data))
        .route("/config/url_expiration", post(set_url_expiration))
        .route("/config/api_delay", post(set_api_delay))
        .route("/fetch_term_data", post(fetch_term_data))
        // DeletionControlableClient routes
        .route("/shards", get(list_shard_entries))
        .route("/shards/{hash}", get(get_shard_bytes).delete(delete_shard_entry))
        .route("/shards/{hash}/dedup_entries", delete(remove_shard_dedup_entries))
        .route("/file_entries", get(list_file_shard_entries))
        .route("/file_entries/{hash}", delete(delete_file_entry))
        .route("/verify_integrity", post(verify_integrity))
        .route("/verify_all_reachable", post(verify_all_reachable))
}

// ---------------------------------------------------------------------------
// DirectAccessClient handlers
// ---------------------------------------------------------------------------

async fn list_xorbs(State(state): State<ServerState>) -> Response {
    match state.client.list_xorbs().await {
        Ok(hashes) => Json(hashes).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn delete_xorb(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    state.client.delete_xorb(&hash).await;
    StatusCode::NO_CONTENT.into_response()
}

async fn get_full_xorb(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    match state.client.get_full_xorb(&hash).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_xorb_ranges(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
    Json(body): Json<XorbRangesRequest>,
) -> Response {
    match state.client.get_xorb_ranges(&hash, body.ranges).await {
        Ok(chunks) => {
            let data: Vec<Vec<u8>> = chunks.into_iter().map(|b| b.to_vec()).collect();
            Json(XorbRangesResponse { data }).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

async fn xorb_length(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    match state.client.xorb_length(&hash).await {
        Ok(length) => Json(XorbLengthResponse { length }).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn xorb_exists(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    match state.client.xorb_exists(&hash).await {
        Ok(exists) => Json(XorbExistsResponse { exists }).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_xorb_raw_bytes(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
    headers: HeaderMap,
) -> Response {
    let total_length = match state.client.xorb_raw_length(&hash).await {
        Ok(len) => len,
        Err(e) => return error_to_response(e),
    };

    let byte_range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => Some(FileRange::new(start, total_length)),
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            Some(FileRange::new(total_length.saturating_sub(suffix), total_length))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    match state.client.get_xorb_raw_bytes(&hash, byte_range).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn xorb_raw_length(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    match state.client.xorb_raw_length(&hash).await {
        Ok(length) => Json(XorbRawLengthResponse { length }).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_file_size(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    match state.client.get_file_size(&hash).await {
        Ok(size) => Json(FileSizeResponse { size }).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_file_data(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
    headers: HeaderMap,
) -> Response {
    let byte_range = match parse_range_header(headers.get(RANGE)) {
        Ok(Some(FileRangeVariant::Normal(range))) => Some(range),
        Ok(Some(FileRangeVariant::OpenRHS(start))) => {
            let file_size = match state.client.get_file_size(&hash).await {
                Ok(s) => s,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(start, file_size))
        },
        Ok(Some(FileRangeVariant::Suffix(suffix))) => {
            let file_size = match state.client.get_file_size(&hash).await {
                Ok(s) => s,
                Err(e) => return error_to_response(e),
            };
            Some(FileRange::new(file_size.saturating_sub(suffix), file_size))
        },
        Ok(None) => None,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    match state.client.get_file_data(&hash, byte_range).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn set_url_expiration(State(state): State<ServerState>, Json(body): Json<ConfigDurationRequest>) -> Response {
    state.client.set_fetch_term_url_expiration(Duration::from_millis(body.millis));
    StatusCode::NO_CONTENT.into_response()
}

async fn set_api_delay(State(state): State<ServerState>, Json(body): Json<ConfigDelayRangeRequest>) -> Response {
    let range = match (body.min_millis, body.max_millis) {
        (Some(min), Some(max)) => Some(Duration::from_millis(min)..Duration::from_millis(max)),
        _ => None,
    };
    state.client.set_api_delay_range(range);
    StatusCode::NO_CONTENT.into_response()
}

async fn fetch_term_data(State(state): State<ServerState>, Json(body): Json<FetchTermDataRequest>) -> Response {
    match state.client.fetch_term_data(body.hash, body.fetch_term).await {
        Ok((data, chunk_byte_indices)) => Json(FetchTermDataResponse {
            data: data.to_vec(),
            chunk_byte_indices,
        })
        .into_response(),
        Err(e) => error_to_response(e),
    }
}

// ---------------------------------------------------------------------------
// DeletionControlableClient handlers
// ---------------------------------------------------------------------------

async fn list_shard_entries(State(state): State<ServerState>) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.list_shard_entries().await {
        Ok(hashes) => Json(hashes).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_shard_bytes(State(state): State<ServerState>, Path(HexMerkleHash(hash)): Path<HexMerkleHash>) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.get_shard_bytes(&hash).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn delete_shard_entry(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.delete_shard_entry(&hash).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_file_shard_entries(State(state): State<ServerState>) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.list_file_shard_entries().await {
        Ok(entries) => {
            let response: Vec<FileShardsEntry> = entries
                .into_iter()
                .map(|(file_hash, shard_hash)| FileShardsEntry { file_hash, shard_hash })
                .collect();
            Json(response).into_response()
        },
        Err(e) => error_to_response(e),
    }
}

async fn delete_file_entry(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.delete_file_entry(&hash).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn remove_shard_dedup_entries(
    State(state): State<ServerState>,
    Path(HexMerkleHash(hash)): Path<HexMerkleHash>,
) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.remove_shard_dedup_entries(&hash).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn verify_integrity(State(state): State<ServerState>) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.verify_integrity().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn verify_all_reachable(State(state): State<ServerState>) -> Response {
    let Some(dc) = &state.deletion_client else {
        return not_implemented();
    };
    match dc.verify_all_reachable().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => error_to_response(e),
    }
}
