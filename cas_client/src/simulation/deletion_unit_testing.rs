//! Common unit tests for DeletionControlableClient implementations.
//!
//! This module provides test functions that work with `Arc<C>` where
//! `C: DirectAccessClient + DeletionControlableClient` to ensure consistent
//! behavior across different client implementations (LocalClient, SimulationControlClient).
//!
//! Each test function takes a fresh client instance to ensure test isolation.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use merklehash::MerkleHash;

use super::client_testing_utils::RandomFileContents;
use super::{ClientTestingUtils, DeletionControlableClient, DirectAccessClient};

/// Runs all common DeletionControlableClient tests using a factory that creates fresh clients.
pub async fn test_deletion_functionality<C, Fut>(factory: impl Fn() -> Fut)
where
    C: DirectAccessClient + DeletionControlableClient + 'static,
    Fut: Future<Output = Arc<C>>,
{
    test_shard_listing_and_deletion(factory().await).await;
    test_file_entry_deletion(factory().await).await;
    test_verify_integrity_detects_missing_xorb(factory().await).await;
    test_deletion_lifecycle(factory().await).await;
    test_missing_shard_entry_errors(factory().await).await;
}

fn expected_xorb_hashes(files: &[&RandomFileContents]) -> HashSet<MerkleHash> {
    files.iter().flat_map(|f| f.terms.iter().map(|t| t.xorb_hash)).collect()
}

fn expected_file_hashes(files: &[&RandomFileContents]) -> HashSet<MerkleHash> {
    files.iter().map(|f| f.file_hash).collect()
}

async fn full_file_hash_set<C: DeletionControlableClient>(client: &C) -> HashSet<MerkleHash> {
    client
        .list_file_shard_entries()
        .await
        .unwrap()
        .into_iter()
        .map(|(fh, _)| fh)
        .collect()
}

async fn full_xorb_hash_set<C: DirectAccessClient>(client: &C) -> HashSet<MerkleHash> {
    client.list_xorbs().await.unwrap().into_iter().collect()
}

/// Tests shard listing, shard byte retrieval, and shard deletion.
async fn test_shard_listing_and_deletion<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    assert!(client.list_shard_entries().await.unwrap().is_empty());
    assert!(client.list_file_shard_entries().await.unwrap().is_empty());

    let file = client.upload_random_file(&[(1, (0, 3)), (2, (0, 2))], 2048).await.unwrap();
    let expected_xorbs = expected_xorb_hashes(&[&file]);

    let file_shard_entries = client.list_file_shard_entries().await.unwrap();
    assert_eq!(file_shard_entries.len(), 1);
    assert_eq!(file_shard_entries[0].0, file.file_hash);

    let shard_entries = client.list_shard_entries().await.unwrap();
    assert_eq!(shard_entries.len(), 1);
    assert_eq!(file_shard_entries[0].1, shard_entries[0]);

    for shard_hash in &shard_entries {
        let shard_bytes = client.get_shard_bytes(shard_hash).await.unwrap();
        assert!(!shard_bytes.is_empty());
    }

    assert_eq!(full_xorb_hash_set(client.as_ref()).await, expected_xorbs);

    client.verify_integrity().await.unwrap();

    client.delete_shard_entry(&shard_entries[0]).await.unwrap();
    assert!(client.list_shard_entries().await.unwrap().is_empty());
}

/// Tests file entry deletion: file entries are removed but shards and xorbs remain.
async fn test_file_entry_deletion<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let file = client.upload_random_file(&[(1, (0, 3)), (2, (0, 2))], 2048).await.unwrap();
    let expected_xorbs = expected_xorb_hashes(&[&file]);

    client.verify_integrity().await.unwrap();

    client.delete_file_entry(&file.file_hash).await.unwrap();

    assert!(client.list_file_shard_entries().await.unwrap().is_empty());

    let file_gone = matches!(client.get_file_reconstruction_info(&file.file_hash).await, Ok(None) | Err(_));
    assert!(file_gone);

    assert_eq!(full_xorb_hash_set(client.as_ref()).await, expected_xorbs);

    assert!(!client.list_shard_entries().await.unwrap().is_empty());
}

/// Tests that verify_integrity detects when xorbs referenced by shards are missing.
async fn test_verify_integrity_detects_missing_xorb<C: DirectAccessClient + DeletionControlableClient + 'static>(
    client: Arc<C>,
) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    client.verify_integrity().await.unwrap();

    for t in &file.terms {
        client.delete_xorb(&t.xorb_hash).await;
    }

    assert!(client.verify_integrity().await.is_err());
}

/// Tests the full deletion lifecycle: delete file entries, then shards, then xorbs.
async fn test_deletion_lifecycle<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let file1 = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let file2 = client.upload_random_file(&[(2, (0, 2))], 2048).await.unwrap();

    let all_xorbs = expected_xorb_hashes(&[&file1, &file2]);
    let all_files = expected_file_hashes(&[&file1, &file2]);

    client.verify_integrity().await.unwrap();

    assert_eq!(full_file_hash_set(client.as_ref()).await, all_files);
    assert_eq!(full_xorb_hash_set(client.as_ref()).await, all_xorbs);

    // Step 1: Delete file entries -- shards and xorbs remain
    client.delete_file_entry(&file1.file_hash).await.unwrap();

    assert_eq!(full_file_hash_set(client.as_ref()).await, HashSet::from([file2.file_hash]));

    client.delete_file_entry(&file2.file_hash).await.unwrap();
    assert!(client.list_file_shard_entries().await.unwrap().is_empty());
    assert!(!client.list_shard_entries().await.unwrap().is_empty());

    assert_eq!(full_xorb_hash_set(client.as_ref()).await, all_xorbs);

    // Step 2: Delete shards -- xorbs remain
    let shard_hashes = client.list_shard_entries().await.unwrap();
    for h in &shard_hashes {
        client.delete_shard_entry(h).await.unwrap();
    }

    assert!(client.list_shard_entries().await.unwrap().is_empty());
    assert_eq!(full_xorb_hash_set(client.as_ref()).await, all_xorbs);

    // Step 3: Delete xorbs -- everything gone
    for h in &all_xorbs {
        client.delete_xorb(h).await;
    }

    assert!(client.list_xorbs().await.unwrap().is_empty());
}

/// Tests that operations on missing shard entries return appropriate errors.
async fn test_missing_shard_entry_errors<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let file = client.upload_random_file(&[(7, (0, 2))], 2048).await.unwrap();
    let mut shard_hashes = client.list_shard_entries().await.unwrap();
    let shard_hash = shard_hashes.pop().unwrap();

    client.delete_shard_entry(&shard_hash).await.unwrap();

    assert!(client.get_shard_bytes(&shard_hash).await.is_err());
    assert!(client.delete_shard_entry(&shard_hash).await.is_err());
    assert!(client.list_file_shard_entries().await.unwrap().is_empty());
    assert_eq!(full_xorb_hash_set(client.as_ref()).await, expected_xorb_hashes(&[&file]));
}
