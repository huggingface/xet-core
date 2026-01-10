//! Integration tests for file reconstruction using RemoteClient against a local test server.
//!
//! These tests verify that the two reconstruction routines in RemoteClient
//! (`get_file_with_sequential_writer` and `get_file_with_parallel_writer`)
//! correctly download and reconstruct files of various sizes and configurations.

use std::sync::Arc;

use cas_client::{
    Client, ClientTestingUtils, LocalTestServer, RandomFileContents, SeekingOutputProvider,
    sequential_output_from_filepath,
};
use cas_types::FileRange;
use tempfile::NamedTempFile;

/// Small chunk size for testing - produces more terms per file.
const CHUNK_SIZE: usize = 579;

/// Helper to run sequential reconstruction and return the data.
async fn reconstruct_sequential(
    client: &Arc<cas_client::RemoteClient>,
    file_hash: &merklehash::MerkleHash,
    byte_range: Option<FileRange>,
) -> Vec<u8> {
    let temp_file = NamedTempFile::new().unwrap();
    let output = sequential_output_from_filepath(temp_file.path()).unwrap();

    client
        .clone()
        .get_file_with_sequential_writer(file_hash, byte_range, output, None)
        .await
        .unwrap();

    std::fs::read(temp_file.path()).unwrap()
}

/// Helper to run parallel reconstruction and return the data.
async fn reconstruct_parallel(
    client: &Arc<cas_client::RemoteClient>,
    file_hash: &merklehash::MerkleHash,
    byte_range: Option<FileRange>,
) -> Vec<u8> {
    let temp_file = NamedTempFile::new().unwrap();
    let output = SeekingOutputProvider::new_file_provider(temp_file.path().to_path_buf());

    client
        .clone()
        .get_file_with_parallel_writer(file_hash, byte_range, output, None)
        .await
        .unwrap();

    std::fs::read(temp_file.path()).unwrap()
}

/// Uploads a file with the given term specification.
async fn upload_file(server: &LocalTestServer, term_spec: &[(u64, (u64, u64))]) -> RandomFileContents {
    server.client().upload_random_file(term_spec, CHUNK_SIZE).await.unwrap()
}

/// Tests both sequential and parallel reconstruction, verifying correctness.
async fn check_reconstruction(server: &LocalTestServer, file: &RandomFileContents, range: Option<FileRange>) {
    let expected_data = match range {
        Some(r) => &file.data[r.start as usize..r.end as usize],
        None => &file.data[..],
    };

    let sequential_result = reconstruct_sequential(server.remote_client(), &file.file_hash, range).await;
    assert_eq!(sequential_result, expected_data, "Sequential reconstruction mismatch");

    let parallel_result = reconstruct_parallel(server.remote_client(), &file.file_hash, range).await;
    assert_eq!(parallel_result, expected_data, "Parallel reconstruction mismatch");

    assert_eq!(sequential_result, parallel_result, "Sequential and parallel results differ");
}

// ============================================================================
// Single-term file tests
// ============================================================================

/// Tests reconstruction of a single-term file with few chunks.
async fn check_single_term_full_file(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 3))]).await;
    check_reconstruction(server, &file, None).await;
}

/// Tests reconstruction of a single-term file with many chunks.
async fn check_single_term_many_chunks(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 20))]).await;
    check_reconstruction(server, &file, None).await;
}

// ============================================================================
// Multi-term file tests (multiple XORBs)
// ============================================================================

/// Tests reconstruction of a multi-term file.
async fn check_multi_term_full_file(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 2)), (2, (0, 3)), (1, (2, 4))]).await;
    check_reconstruction(server, &file, None).await;
}

/// Tests reconstruction of a file with many terms.
async fn check_many_terms(server: &LocalTestServer) {
    let term_spec: Vec<(u64, (u64, u64))> = (0..10).map(|i| (i, (0, 2))).collect();
    let file = upload_file(server, &term_spec).await;
    check_reconstruction(server, &file, None).await;
}

// ============================================================================
// XORB reuse tests (same XORB referenced multiple times)
// ============================================================================

/// Tests reconstruction when the same XORB is referenced multiple times.
async fn check_xorb_reuse(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 2)), (2, (0, 2)), (1, (2, 4)), (2, (2, 4)), (1, (0, 2))]).await;
    check_reconstruction(server, &file, None).await;
}

// ============================================================================
// Range request tests - partial file downloads
// ============================================================================

/// Tests range reconstruction from the start of the file.
async fn check_range_from_start(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 5))]).await;
    let range_end = file.data.len() as u64 / 2;
    check_reconstruction(server, &file, Some(FileRange::new(0, range_end))).await;
}

/// Tests range reconstruction from the middle of the file.
async fn check_range_middle(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 6))]).await;
    let file_len = file.data.len() as u64;
    check_reconstruction(server, &file, Some(FileRange::new(file_len / 4, file_len * 3 / 4))).await;
}

/// Tests range reconstruction to the end of the file.
async fn check_range_to_end(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 5))]).await;
    let file_len = file.data.len() as u64;
    check_reconstruction(server, &file, Some(FileRange::new(file_len / 2, file_len))).await;
}

/// Tests range reconstruction spanning multiple terms.
async fn check_range_spanning_terms(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 3)), (2, (0, 2)), (3, (0, 3))]).await;
    let term1_size = file.terms[0].data.len() as u64;
    let term2_size = file.terms[1].data.len() as u64;
    check_reconstruction(server, &file, Some(FileRange::new(term1_size / 2, term1_size + term2_size / 2))).await;
}

/// Tests range reconstruction in the middle of a multi-term file.
async fn check_range_multi_term_middle(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 4)), (2, (0, 3)), (3, (0, 2))]).await;
    let file_len = file.data.len() as u64;
    check_reconstruction(server, &file, Some(FileRange::new(file_len / 4, file_len * 3 / 4))).await;
}

// ============================================================================
// Edge cases
// ============================================================================

/// Tests reconstruction of a small byte range.
async fn check_small_range(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 4))]).await;
    check_reconstruction(server, &file, Some(FileRange::new(100, 200))).await;
}

/// Tests reconstruction of a single byte.
async fn check_single_byte_range(server: &LocalTestServer) {
    let file = upload_file(server, &[(1, (0, 3))]).await;
    check_reconstruction(server, &file, Some(FileRange::new(50, 51))).await;
}

/// Runs all reconstruction checks with a given server.
async fn run_all_reconstruction_checks(server: &LocalTestServer) {
    // Single-term file tests
    check_single_term_full_file(server).await;
    check_single_term_many_chunks(server).await;

    // Multi-term file tests
    check_multi_term_full_file(server).await;
    check_many_terms(server).await;

    // XORB reuse tests
    check_xorb_reuse(server).await;

    // Range request tests
    check_range_from_start(server).await;
    check_range_middle(server).await;
    check_range_to_end(server).await;
    check_range_spanning_terms(server).await;
    check_range_multi_term_middle(server).await;

    // Edge cases
    check_small_range(server).await;
    check_single_byte_range(server).await;
}

/// Main test that runs all reconstruction checks with in-memory storage.
#[tokio::test]
async fn test_reconstruction_with_local_server() {
    let server = LocalTestServer::start(true).await;
    run_all_reconstruction_checks(&server).await;
}
