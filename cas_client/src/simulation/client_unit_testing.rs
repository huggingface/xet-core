//! Common unit tests for Client implementations.
//!
//! This module provides test functions that work with `Arc<dyn DirectAccessClient>` to ensure
//! consistent behavior across different client implementations (LocalClient, MemoryClient).
//!
//! Each test function takes a fresh client instance to ensure test isolation.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use cas_types::FileRange;

use super::{ClientTestingUtils, DirectAccessClient};
use crate::error::CasClientError;

/// Runs all common Client trait tests using a factory that creates fresh clients.
pub async fn test_client_functionality<Fut>(factory: impl Fn() -> Fut)
where
    Fut: Future<Output = Arc<dyn DirectAccessClient>>,
{
    test_reconstruction_merges_adjacent_ranges(factory().await).await;
    test_reconstruction_with_multiple_xorbs(factory().await).await;
    test_reconstruction_overlapping_range_merging(factory().await).await;
    test_range_requests(factory().await).await;
    test_upload_configurations(factory().await).await;
    test_chunk_boundary_shrinking(factory().await).await;
    test_chunk_boundary_multiple_segments(factory().await).await;
    test_batch_reconstruction(factory().await).await;
    test_basic_xorb_put_get(factory().await).await;
    test_xorb_ranges(factory().await).await;
    test_xorb_length(factory().await).await;
    test_missing_xorb(factory().await).await;
    test_xorb_list_and_delete(factory().await).await;
    test_get_file_data(factory().await).await;
    test_get_file_data_with_ranges(factory().await).await;
    test_get_file_size(factory().await).await;
    test_global_dedup(factory().await).await;
}

/// Tests that adjacent chunk ranges from the same xorb are merged into a single fetch_info.
pub async fn test_reconstruction_merges_adjacent_ranges(client: Arc<dyn DirectAccessClient>) {
    let term_spec = &[(1, (0, 2)), (1, (2, 4))];
    let file = client.upload_random_file(term_spec, 2048).await.unwrap();

    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
    assert_eq!(reconstruction.terms.len(), 2);
    assert_eq!(reconstruction.fetch_info.len(), 1);

    let xorb_hash_hex = reconstruction.terms[0].hash;
    let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
    assert_eq!(fetch_infos.len(), 1);
    assert_eq!(fetch_infos[0].range.start, 0);
    assert_eq!(fetch_infos[0].range.end, 4);
}

/// Tests reconstruction with segments from multiple different xorbs.
pub async fn test_reconstruction_with_multiple_xorbs(client: Arc<dyn DirectAccessClient>) {
    let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
    let file = client.upload_random_file(term_spec, 2048).await.unwrap();

    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
    assert_eq!(reconstruction.terms.len(), 3);
    assert_eq!(reconstruction.fetch_info.len(), 2);
}

/// Tests that overlapping chunk ranges within the same xorb are correctly merged.
pub async fn test_reconstruction_overlapping_range_merging(client: Arc<dyn DirectAccessClient>) {
    let chunk_size = 2048usize;

    // Test 1: Simple overlapping ranges [0,3) and [1,4) -> merged to [0,4)
    {
        let term_spec = &[(1, (0, 3)), (1, (1, 4))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 4);
    }

    // Test 2: Subset range - second range is fully contained in first [0,5) and [1,3) -> [0,5)
    {
        let term_spec = &[(1, (0, 5)), (1, (1, 3))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 5);
    }

    // Test 3: Multiple overlapping ranges forming a chain [0,2), [1,4), [3,6) -> [0,6)
    {
        let term_spec = &[(1, (0, 2)), (1, (1, 4)), (1, (3, 6))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 6);
    }

    // Test 4: Non-contiguous ranges should NOT be merged [0,2) and [4,6) -> two separate ranges
    {
        let term_spec = &[(1, (0, 2)), (1, (4, 6))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 2);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 2);
        assert_eq!(fetch_infos[1].range.start, 4);
        assert_eq!(fetch_infos[1].range.end, 6);
    }

    // Test 5: Touch at boundary (adjacent) [0,3) and [3,5) -> [0,5)
    {
        let term_spec = &[(1, (0, 3)), (1, (3, 5))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 5);
    }

    // Test 6: Same range repeated multiple times [2,5), [2,5), [2,5) -> [2,5)
    {
        let term_spec = &[(1, (2, 5)), (1, (2, 5)), (1, (2, 5))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 2);
        assert_eq!(fetch_infos[0].range.end, 5);
    }

    // Test 7: Mixed overlapping and non-contiguous [0,3), [2,4), [6,8), [7,10) -> [0,4) and [6,10)
    {
        let term_spec = &[(1, (0, 3)), (1, (2, 4)), (1, (6, 8)), (1, (7, 10))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 4);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 2);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 4);
        assert_eq!(fetch_infos[1].range.start, 6);
        assert_eq!(fetch_infos[1].range.end, 10);
    }
}

/// Tests range request behavior for get_reconstruction.
pub async fn test_range_requests(client: Arc<dyn DirectAccessClient>) {
    let term_spec = &[(1, (0, 5))];
    let file = client.upload_random_file(term_spec, 2048).await.unwrap();

    // Calculate total file size from terms
    let reconstruction_full = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
    let total_file_size: u64 = reconstruction_full.terms.iter().map(|t| t.unpacked_length as u64).sum();

    // Partial out-of-range truncates
    let response = client
        .get_reconstruction(&file.file_hash, Some(FileRange::new(total_file_size / 2, total_file_size + 1000)))
        .await
        .unwrap()
        .unwrap();
    assert!(!response.terms.is_empty());
    assert!(response.offset_into_first_range > 0);

    // Entire range out of bounds returns Ok(None) (like RemoteClient's 416 handling)
    let result = client
        .get_reconstruction(&file.file_hash, Some(FileRange::new(total_file_size + 100, total_file_size + 1000)))
        .await;
    assert!(result.unwrap().is_none());

    // Start equals file size returns Ok(None)
    let result = client
        .get_reconstruction(&file.file_hash, Some(FileRange::new(total_file_size, total_file_size + 100)))
        .await;
    assert!(result.unwrap().is_none());

    // Valid range within bounds succeeds
    let response = client
        .get_reconstruction(&file.file_hash, Some(FileRange::new(0, total_file_size / 2)))
        .await
        .unwrap()
        .unwrap();
    assert!(!response.terms.is_empty());
    assert_eq!(response.offset_into_first_range, 0);

    // End exactly at file size succeeds
    let response = client
        .get_reconstruction(&file.file_hash, Some(FileRange::new(0, total_file_size)))
        .await
        .unwrap()
        .unwrap();
    let total_unpacked: u64 = response.terms.iter().map(|t| t.unpacked_length as u64).sum();
    assert_eq!(total_unpacked, total_file_size);
}

/// Tests various file upload configurations.
pub async fn test_upload_configurations(client: Arc<dyn DirectAccessClient>) {
    // Test 1: Single segment with 3 chunks
    {
        let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 1);
    }

    // Test 2: Multiple segments from the same xorb
    {
        let term_spec = &[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 1);
    }

    // Test 3: Segments from different xorbs
    {
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (3, (0, 4))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 3);
    }

    // Test 4: Overlapping chunk references from same xorb
    {
        let term_spec = &[(1, (0, 3)), (1, (1, 4)), (1, (2, 5))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();

        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 1);
    }
}

/// Tests chunk boundary shrinking with a single xorb.
pub async fn test_chunk_boundary_shrinking(client: Arc<dyn DirectAccessClient>) {
    let chunk_size: usize = 2048;
    let term_spec = &[(1, (0, 5))];
    let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

    let reconstruction_full = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
    let total_file_size: u64 = reconstruction_full.terms.iter().map(|t| t.unpacked_length as u64).sum();
    assert_eq!(total_file_size, (5 * chunk_size) as u64);

    // Test 1: Range starting in the middle of chunk 1 should skip chunk 0
    {
        let start = chunk_size as u64 + 500;
        let end = total_file_size;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 1);
        assert_eq!(response.terms[0].range.end, 5);
        assert_eq!(response.offset_into_first_range, 500);
    }

    // Test 2: Range starting exactly at a chunk boundary
    {
        let start = (chunk_size * 2) as u64;
        let end = total_file_size;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 2);
        assert_eq!(response.terms[0].range.end, 5);
        assert_eq!(response.offset_into_first_range, 0);
    }

    // Test 3: Range ending in the middle of a chunk
    {
        let start = 0u64;
        let end = (chunk_size * 2) as u64 + 500;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 0);
        assert_eq!(response.terms[0].range.end, 3);
        assert_eq!(response.offset_into_first_range, 0);
    }

    // Test 4: Range fully within a single chunk
    {
        let start = (chunk_size * 2) as u64 + 100;
        let end = (chunk_size * 2) as u64 + 500;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 2);
        assert_eq!(response.terms[0].range.end, 3);
        assert_eq!(response.offset_into_first_range, 100);
    }

    // Test 5: Range spanning exactly one chunk boundary
    {
        let start = chunk_size as u64 - 100;
        let end = chunk_size as u64 + 100;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 0);
        assert_eq!(response.terms[0].range.end, 2);
        assert_eq!(response.offset_into_first_range, chunk_size as u64 - 100);
    }
}

/// Tests chunk boundary shrinking with multiple segments across different xorbs.
pub async fn test_chunk_boundary_multiple_segments(client: Arc<dyn DirectAccessClient>) {
    let chunk_size = 2048usize;
    let term_spec = &[(1, (0, 4)), (2, (0, 4))];
    let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

    let reconstruction_full = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
    let total_file_size: u64 = reconstruction_full.terms.iter().map(|t| t.unpacked_length as u64).sum();
    assert_eq!(total_file_size, (8 * chunk_size) as u64);

    // Test 1: Range that skips first chunk of first xorb
    {
        let start = chunk_size as u64 + 500;
        let end = total_file_size;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 2);
        assert_eq!(response.terms[0].range.start, 1);
        assert_eq!(response.terms[0].range.end, 4);
        assert_eq!(response.terms[1].range.start, 0);
        assert_eq!(response.terms[1].range.end, 4);
        assert_eq!(response.offset_into_first_range, 500);
    }

    // Test 2: Range fully within first xorb
    {
        let start = chunk_size as u64;
        let end = (chunk_size * 3) as u64;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 1);
        assert_eq!(response.terms[0].range.end, 3);
        assert_eq!(response.offset_into_first_range, 0);
    }

    // Test 3: Range fully within second xorb
    {
        let xorb1_size = (chunk_size * 4) as u64;
        let start = xorb1_size + chunk_size as u64;
        let end = xorb1_size + (chunk_size * 3) as u64;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 1);
        assert_eq!(response.terms[0].range.start, 1);
        assert_eq!(response.terms[0].range.end, 3);
        assert_eq!(response.offset_into_first_range, 0);
    }

    // Test 4: Range spanning xorb boundary
    {
        let xorb1_size = (chunk_size * 4) as u64;
        let start = (chunk_size * 2) as u64;
        let end = xorb1_size + (chunk_size * 2) as u64 + 500;
        let response = client
            .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.terms.len(), 2);
        assert_eq!(response.terms[0].range.start, 2);
        assert_eq!(response.terms[0].range.end, 4);
        assert_eq!(response.terms[1].range.start, 0);
        assert_eq!(response.terms[1].range.end, 3);
        assert_eq!(response.offset_into_first_range, 0);
    }
}

/// Tests batch reconstruction.
pub async fn test_batch_reconstruction(client: Arc<dyn DirectAccessClient>) {
    // Upload multiple files
    let file1 = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let file2 = client.upload_random_file(&[(2, (0, 4))], 2048).await.unwrap();
    let file3 = client.upload_random_file(&[(3, (0, 2))], 2048).await.unwrap();

    // Batch query
    let batch_response = client
        .batch_get_reconstruction(&[file1.file_hash, file2.file_hash, file3.file_hash])
        .await
        .unwrap();

    assert_eq!(batch_response.files.len(), 3);
    assert!(batch_response.files.contains_key(&file1.file_hash.into()));
    assert!(batch_response.files.contains_key(&file2.file_hash.into()));
    assert!(batch_response.files.contains_key(&file3.file_hash.into()));
}

/// Tests basic XORB put and get operations via upload_random_file.
pub async fn test_basic_xorb_put_get(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    // Get the xorb hash from the first term
    let xorb_hash = file.term_xorb_hash(0).unwrap();

    // Verify the xorb exists
    assert!(client.xorb_exists(&xorb_hash).await.unwrap());

    // Get the full xorb data and verify it contains the expected chunks
    let xorb_data = client.get_full_xorb(&xorb_hash).await.unwrap();
    assert!(!xorb_data.is_empty());

    // The xorb data should contain all chunks from the file
    assert_eq!(xorb_data, file.data);
}

/// Tests get_xorb_ranges for partial chunk retrieval.
pub async fn test_xorb_ranges(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 4))], 2048).await.unwrap();
    let xorb_hash = file.term_xorb_hash(0).unwrap();

    // Get ranges of chunks
    let ranges = vec![(0, 2), (2, 4)];
    let result = client.get_xorb_ranges(&xorb_hash, ranges).await.unwrap();

    assert_eq!(result.len(), 2);

    // Concatenated ranges should equal full data
    let combined: Bytes = [result[0].as_ref(), result[1].as_ref()].concat().into();
    assert_eq!(combined, file.data);

    // Empty range should return empty vec
    let empty_result = client.get_xorb_ranges(&xorb_hash, vec![]).await.unwrap();
    assert_eq!(empty_result.len(), 1);
    assert!(empty_result[0].is_empty());

    // Range with start >= end should return empty vec
    let empty_range_result = client.get_xorb_ranges(&xorb_hash, vec![(2, 2)]).await.unwrap();
    assert_eq!(empty_range_result.len(), 1);
    assert!(empty_range_result[0].is_empty());
}

/// Tests xorb_length returns correct length.
pub async fn test_xorb_length(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let xorb_hash = file.term_xorb_hash(0).unwrap();

    let length = client.xorb_length(&xorb_hash).await.unwrap();
    assert_eq!(length as usize, file.data.len());
}

/// Tests that operations on missing XORBs return appropriate errors.
pub async fn test_missing_xorb(client: Arc<dyn DirectAccessClient>) {
    let fake_hash =
        merklehash::MerkleHash::from_hex("d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da").unwrap();

    // xorb_exists should return false for missing xorb
    assert!(!client.xorb_exists(&fake_hash).await.unwrap());

    // get_full_xorb should return XORBNotFound
    let result = client.get_full_xorb(&fake_hash).await;
    assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));

    // xorb_length should return XORBNotFound
    let result = client.xorb_length(&fake_hash).await;
    assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));

    // get_xorb_ranges should return XORBNotFound
    let result = client.get_xorb_ranges(&fake_hash, vec![(0, 1)]).await;
    assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));
}

/// Tests list_xorbs and delete_xorb operations.
pub async fn test_xorb_list_and_delete(client: Arc<dyn DirectAccessClient>) {
    // Initially should be empty
    let initial_list = client.list_xorbs().await.unwrap();
    assert!(initial_list.is_empty());

    // Upload a file which creates xorbs
    let file = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();
    let xorb_hash = file.term_xorb_hash(0).unwrap();

    // Now should have one xorb
    let list = client.list_xorbs().await.unwrap();
    assert_eq!(list.len(), 1);
    assert!(list.contains(&xorb_hash));

    // Delete the xorb
    client.delete_xorb(&xorb_hash).await;

    // Should be empty again
    let final_list = client.list_xorbs().await.unwrap();
    assert!(final_list.is_empty());

    // xorb should no longer exist
    assert!(!client.xorb_exists(&xorb_hash).await.unwrap());

    // Deleting non-existent xorb should not fail
    client.delete_xorb(&xorb_hash).await;
}

/// Tests get_file_data returns correct data.
pub async fn test_get_file_data(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 4))], 2048).await.unwrap();

    // Get full file data
    let data = client.get_file_data(&file.file_hash, None).await.unwrap();
    assert_eq!(data, file.data);

    // Multi-xorb file
    let file2 = client.upload_random_file(&[(1, (0, 2)), (2, (0, 3))], 2048).await.unwrap();
    let data2 = client.get_file_data(&file2.file_hash, None).await.unwrap();
    assert_eq!(data2, file2.data);
}

/// Tests get_file_data with byte ranges.
pub async fn test_get_file_data_with_ranges(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 5))], 2048).await.unwrap();
    let file_size = file.data.len() as u64;

    // First half
    let half = file_size / 2;
    let first_half = client
        .get_file_data(&file.file_hash, Some(FileRange::new(0, half)))
        .await
        .unwrap();
    assert_eq!(first_half, &file.data[..half as usize]);

    // Second half
    let second_half = client
        .get_file_data(&file.file_hash, Some(FileRange::new(half, file_size)))
        .await
        .unwrap();
    assert_eq!(second_half, &file.data[half as usize..]);

    // Range extending beyond file size should be truncated
    let truncated = client
        .get_file_data(&file.file_hash, Some(FileRange::new(half, file_size + 1000)))
        .await
        .unwrap();
    assert_eq!(truncated, &file.data[half as usize..]);

    // Entire range out of bounds returns error
    let result = client
        .get_file_data(&file.file_hash, Some(FileRange::new(file_size + 100, file_size + 1000)))
        .await;
    assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

    // Start equals file size returns error
    let result = client
        .get_file_data(&file.file_hash, Some(FileRange::new(file_size, file_size + 100)))
        .await;
    assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));
}

/// Tests get_file_size returns correct size.
pub async fn test_get_file_size(client: Arc<dyn DirectAccessClient>) {
    let file = client.upload_random_file(&[(1, (0, 4))], 2048).await.unwrap();

    let size = client.get_file_size(&file.file_hash).await.unwrap();
    assert_eq!(size as usize, file.data.len());
}

/// Tests global dedup functionality: uploading a shard and querying for dedup.
pub async fn test_global_dedup(client: Arc<dyn DirectAccessClient>) {
    use std::io::Cursor;

    use mdb_shard::shard_format::test_routines::gen_random_shard_with_cas_references;
    use mdb_shard::utils::{parse_shard_filename, shard_file_name};
    use mdb_shard::{MDBShardFile, MDBShardInfo};
    use tempfile::TempDir;

    let tmp_dir = TempDir::new().unwrap();
    let shard_dir_1 = tmp_dir.path().join("shard_1");
    std::fs::create_dir_all(&shard_dir_1).unwrap();
    let shard_dir_2 = tmp_dir.path().join("shard_2");
    std::fs::create_dir_all(&shard_dir_2).unwrap();

    let shard_in = gen_random_shard_with_cas_references(0, &[16; 8], &[2; 20], true, true).unwrap();

    let new_shard_path = shard_in.write_to_directory(&shard_dir_1, None).unwrap();

    let shard_hash = parse_shard_filename(&new_shard_path).unwrap();

    let permit = client.acquire_upload_permit().await.unwrap();
    client
        .upload_shard(std::fs::read(&new_shard_path).unwrap().into(), permit)
        .await
        .unwrap();

    let dedup_hashes =
        MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut std::fs::File::open(&new_shard_path).unwrap()).unwrap();

    assert_ne!(dedup_hashes.len(), 0);

    // Query for global dedup and verify we get the shard back
    let new_shard = client
        .query_for_global_dedup_shard("default", &dedup_hashes[0])
        .await
        .unwrap()
        .unwrap();

    let sf = MDBShardFile::write_out_from_reader(shard_dir_2.clone(), &mut Cursor::new(new_shard)).unwrap();

    assert_eq!(sf.path, shard_dir_2.join(shard_file_name(&shard_hash)));
}

/// Runs all URL expiration tests. Must be called from a `#[tokio::test(start_paused = true)]` context.
pub async fn test_url_expiration_functionality<Fut>(factory: impl Fn() -> Fut)
where
    Fut: std::future::Future<Output = Arc<dyn DirectAccessClient>>,
{
    test_url_expiration_within_window(factory().await).await;
    test_url_expiration_after_window(factory().await).await;
    test_url_expiration_default_infinite(factory().await).await;
    test_url_expiration_exact_boundary(factory().await).await;
}

/// Tests that URL remains valid within the expiration window.
async fn test_url_expiration_within_window(client: Arc<dyn DirectAccessClient>) {
    use cas_types::HexMerkleHash;
    use tokio::time::Duration;

    // Set URL expiration to 60 seconds
    client.set_fetch_term_url_expiration(Duration::from_secs(60));

    // Upload a file and get reconstruction info (which creates URLs with current timestamp)
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();

    // Get the fetch_info for the first term's xorb
    let xorb_hash = file.terms[0].xorb_hash;
    let hex_hash: HexMerkleHash = xorb_hash.into();
    let fetch_info = reconstruction.fetch_info.get(&hex_hash).unwrap().first().unwrap().clone();

    // Advance time by 30 seconds (still within the 60 second window)
    tokio::time::advance(Duration::from_secs(30)).await;

    // The fetch should still succeed
    let result = client.fetch_term_data(xorb_hash, fetch_info).await;
    assert!(result.is_ok(), "URL should be valid within expiration window");
}

/// Tests that URL expires after the expiration window.
async fn test_url_expiration_after_window(client: Arc<dyn DirectAccessClient>) {
    use cas_types::HexMerkleHash;
    use tokio::time::Duration;

    // Set URL expiration to 60 seconds
    client.set_fetch_term_url_expiration(Duration::from_secs(60));

    // Upload a file and get reconstruction info (which creates URLs with current timestamp)
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();

    // Get the fetch_info for the first term's xorb
    let xorb_hash = file.terms[0].xorb_hash;
    let hex_hash: HexMerkleHash = xorb_hash.into();
    let fetch_info = reconstruction.fetch_info.get(&hex_hash).unwrap().first().unwrap().clone();

    // Advance time by 61 seconds (past the 60 second window)
    tokio::time::advance(Duration::from_secs(61)).await;

    // The fetch should fail with expiration error
    let result = client.fetch_term_data(xorb_hash, fetch_info).await;
    assert!(result.is_err(), "URL should be expired after expiration window");
    assert!(matches!(result.unwrap_err(), CasClientError::PresignedUrlExpirationError));
}

/// Tests that default URL expiration is effectively infinite.
async fn test_url_expiration_default_infinite(client: Arc<dyn DirectAccessClient>) {
    use cas_types::HexMerkleHash;
    use tokio::time::Duration;

    // Don't set expiration - default should be effectively infinite (u64::MAX ms)

    // Upload a file and get reconstruction info
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();

    // Get the fetch_info for the first term's xorb
    let xorb_hash = file.terms[0].xorb_hash;
    let hex_hash: HexMerkleHash = xorb_hash.into();
    let fetch_info = reconstruction.fetch_info.get(&hex_hash).unwrap().first().unwrap().clone();

    // Advance time by 1 year - should still work with default infinite expiration
    tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;

    // The fetch should still succeed
    let result = client.fetch_term_data(xorb_hash, fetch_info).await;
    assert!(result.is_ok(), "URL should not expire with default infinite expiration");
}

/// Tests URL expiration at exact boundary.
async fn test_url_expiration_exact_boundary(client: Arc<dyn DirectAccessClient>) {
    use cas_types::HexMerkleHash;
    use tokio::time::Duration;

    // Set URL expiration to 60 seconds
    client.set_fetch_term_url_expiration(Duration::from_secs(60));

    // Upload a file and get reconstruction info
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();

    // Get the fetch_info for the first term's xorb
    let xorb_hash = file.terms[0].xorb_hash;
    let hex_hash: HexMerkleHash = xorb_hash.into();
    let fetch_info = reconstruction.fetch_info.get(&hex_hash).unwrap().first().unwrap().clone();

    // Test inside boundary (59 seconds elapsed, within 60 second window) - should be valid
    tokio::time::advance(Duration::from_secs(59)).await;

    let result = client.fetch_term_data(xorb_hash, fetch_info.clone()).await;
    assert!(result.is_ok(), "URL should be valid inside expiration boundary");

    // Advance 2 more seconds (now 61 seconds total, past 60 second window) - should be expired
    tokio::time::advance(Duration::from_secs(2)).await;

    let result = client.fetch_term_data(xorb_hash, fetch_info).await;
    assert!(result.is_err(), "URL should be expired past boundary");
    assert!(matches!(result.unwrap_err(), CasClientError::PresignedUrlExpirationError));
}

// =============================================================================
// API Delay Tests
// =============================================================================

/// Entry point to test API delay functionality.
/// These tests use start_paused=true to control time precisely.
pub async fn test_api_delay_functionality<Fut>(factory: impl Fn() -> Fut)
where
    Fut: std::future::Future<Output = Arc<dyn DirectAccessClient>>,
{
    test_api_delay_disabled_by_default(factory().await).await;
    test_api_delay_fixed_delay(factory().await).await;
    test_api_delay_range(factory().await).await;
    test_api_delay_can_be_disabled(factory().await).await;
}

/// Tests that API delay is disabled by default (no delay applied).
async fn test_api_delay_disabled_by_default(client: Arc<dyn DirectAccessClient>) {
    use tokio::time::{Duration, Instant};

    // Upload a file
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    // Measure time for an API call (should be near-instant with no delay)
    let start = Instant::now();
    let _result = client.get_file_reconstruction_info(&file.file_hash).await.unwrap();
    let elapsed = start.elapsed();

    // With time paused, elapsed should be effectively 0 (or very small)
    assert!(elapsed < Duration::from_millis(10), "No delay should be applied by default");
}

/// Tests that a fixed delay is applied to API calls.
async fn test_api_delay_fixed_delay(client: Arc<dyn DirectAccessClient>) {
    use tokio::time::Duration;

    // Set a fixed delay of 100ms (start == end means fixed delay)
    let delay = Duration::from_millis(100);
    client.set_api_delay_range(Some(delay..delay));

    // Upload a file
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    // Measure time for an API call
    let start = tokio::time::Instant::now();
    let _result = client.get_file_reconstruction_info(&file.file_hash).await.unwrap();
    let elapsed = start.elapsed();

    // With tokio::time paused, the delay should be exactly 100ms
    assert!(elapsed >= Duration::from_millis(100), "Fixed delay should be applied: elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_millis(150),
        "Delay should not be much more than configured: elapsed={elapsed:?}"
    );
}

/// Tests that delay falls within the specified range.
async fn test_api_delay_range(client: Arc<dyn DirectAccessClient>) {
    use tokio::time::Duration;

    // Set a delay range of 50-150ms
    client.set_api_delay_range(Some(Duration::from_millis(50)..Duration::from_millis(150)));

    // Upload a file
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    // Make multiple API calls and check they fall within range
    for _ in 0..5 {
        let start = tokio::time::Instant::now();
        let _result = client.get_file_reconstruction_info(&file.file_hash).await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(50), "Delay should be at least 50ms: elapsed={elapsed:?}");
        assert!(elapsed < Duration::from_millis(200), "Delay should be at most ~150ms: elapsed={elapsed:?}");
    }
}

/// Tests that delay can be disabled after being enabled.
async fn test_api_delay_can_be_disabled(client: Arc<dyn DirectAccessClient>) {
    use tokio::time::Duration;

    // Set a delay
    client.set_api_delay_range(Some(Duration::from_millis(100)..Duration::from_millis(100)));

    // Upload a file
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    // Verify delay is applied
    let start = tokio::time::Instant::now();
    let _result = client.get_file_reconstruction_info(&file.file_hash).await.unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(100), "Delay should be applied");

    // Disable the delay
    client.set_api_delay_range(None);

    // Verify delay is no longer applied
    let start = tokio::time::Instant::now();
    let _result = client.get_file_reconstruction_info(&file.file_hash).await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "Delay should not be applied after disabling: elapsed={elapsed:?}"
    );
}
