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

use xet_core_structures::merklehash::MerkleHash;

use super::client_testing_utils::RandomFileContents;
use super::deletion_controls::ObjectTag;
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
    test_soft_delete_is_idempotent(factory().await).await;
    test_soft_delete_blocks_reconstruction(factory().await).await;
    test_soft_delete_blocks_direct_file_access(factory().await).await;
    test_soft_delete_read_delete_race_stable(factory().await).await;
    test_remove_shard_dedup_entries_removes_correct_entries(factory().await).await;
    test_remove_shard_dedup_entries_noop_on_unknown_hash(factory().await).await;
    test_verify_integrity_after_file_deletion(factory().await).await;
    test_list_xorbs_and_tags(factory().await).await;
    test_delete_xorb_if_tag_matches(factory().await).await;
    test_list_shards_with_tags(factory().await).await;
    test_delete_shard_if_tag_matches(factory().await).await;
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

/// Tests that deleting the same file hash twice does not panic and the file remains hidden.
async fn test_soft_delete_is_idempotent<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    assert_eq!(full_file_hash_set(client.as_ref()).await, HashSet::from([file.file_hash]));

    client.delete_file_entry(&file.file_hash).await.unwrap();
    client.delete_file_entry(&file.file_hash).await.unwrap();

    assert!(full_file_hash_set(client.as_ref()).await.is_empty());
    let file_gone = matches!(client.get_file_reconstruction_info(&file.file_hash).await, Ok(None) | Err(_));
    assert!(file_gone);
}

/// Tests that soft-deleted files cannot be reconstructed (download returns None).
async fn test_soft_delete_blocks_reconstruction<C: DirectAccessClient + DeletionControlableClient + 'static>(
    client: Arc<C>,
) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    let recon = client.get_reconstruction(&file.file_hash, None).await.unwrap();
    assert!(recon.is_some(), "File should be reconstructable before deletion");

    client.delete_file_entry(&file.file_hash).await.unwrap();

    let recon_after = client.get_reconstruction(&file.file_hash, None).await;
    let is_gone = matches!(recon_after, Ok(None) | Err(_));
    assert!(is_gone, "Deleted file should not be reconstructable");
}

/// Tests that soft-deleted files are blocked from direct file-access paths too.
async fn test_soft_delete_blocks_direct_file_access<C: DirectAccessClient + DeletionControlableClient + 'static>(
    client: Arc<C>,
) {
    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

    let size_before = client.get_file_size(&file.file_hash).await.unwrap();
    assert_eq!(size_before as usize, file.data.len());
    assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);

    client.delete_file_entry(&file.file_hash).await.unwrap();

    let size_after = client.get_file_size(&file.file_hash).await;
    assert!(size_after.is_err(), "Deleted file should not report a size");

    let data_after = client.get_file_data(&file.file_hash, None).await;
    assert!(data_after.is_err(), "Deleted file should not return file data");
}

/// Compact stress-style check: concurrent reads racing with delete remain safe and converge.
async fn test_soft_delete_read_delete_race_stable<C: DirectAccessClient + DeletionControlableClient + 'static>(
    client: Arc<C>,
) {
    let file = client.upload_random_file(&[(1, (0, 4))], 2048).await.unwrap();
    let fh_a = file.file_hash;
    let fh_b = file.file_hash;

    let read_data_client = client.clone();
    let read_data_task = tokio::spawn(async move {
        for _ in 0..32 {
            let _ = read_data_client.get_file_data(&fh_a, None).await;
            tokio::task::yield_now().await;
        }
    });

    let read_size_client = client.clone();
    let read_size_task = tokio::spawn(async move {
        for _ in 0..32 {
            let _ = read_size_client.get_file_size(&fh_b).await;
            tokio::task::yield_now().await;
        }
    });

    tokio::task::yield_now().await;
    client.delete_file_entry(&file.file_hash).await.unwrap();

    read_data_task.await.unwrap();
    read_size_task.await.unwrap();

    assert!(client.get_file_data(&file.file_hash, None).await.is_err());
    assert!(client.get_file_size(&file.file_hash).await.is_err());
}

/// Tests that remove_shard_dedup_entries removes only entries for the target shard.
async fn test_remove_shard_dedup_entries_removes_correct_entries<
    C: DirectAccessClient + DeletionControlableClient + 'static,
>(
    client: Arc<C>,
) {
    let file_a = client.upload_random_file(&[(10, (0, 3))], 2048).await.unwrap();
    let file_b = client.upload_random_file(&[(20, (0, 2))], 2048).await.unwrap();

    let file_entries = client.list_file_shard_entries().await.unwrap();
    let shard_a = file_entries.iter().find(|(fh, _)| *fh == file_a.file_hash).unwrap().1;
    let shard_b = file_entries.iter().find(|(fh, _)| *fh == file_b.file_hash).unwrap().1;
    assert_ne!(shard_a, shard_b, "Files should be in different shards");

    for t in &file_a.terms {
        let dedup = client
            .query_for_global_dedup_shard("default", &t.chunk_hashes[0])
            .await
            .unwrap();
        assert!(dedup.is_some(), "Chunk from file A should have a dedup entry");
    }
    for t in &file_b.terms {
        let dedup = client
            .query_for_global_dedup_shard("default", &t.chunk_hashes[0])
            .await
            .unwrap();
        assert!(dedup.is_some(), "Chunk from file B should have a dedup entry");
    }

    client.remove_shard_dedup_entries(&shard_a).await.unwrap();

    for t in &file_a.terms {
        let dedup = client
            .query_for_global_dedup_shard("default", &t.chunk_hashes[0])
            .await
            .unwrap();
        assert!(dedup.is_none(), "Dedup entries for shard A's chunks should be removed");
    }
    for t in &file_b.terms {
        let dedup = client
            .query_for_global_dedup_shard("default", &t.chunk_hashes[0])
            .await
            .unwrap();
        assert!(dedup.is_some(), "Dedup entries for shard B's chunks should be preserved");
    }
}

/// Tests that remove_shard_dedup_entries is a no-op for an unknown shard hash.
async fn test_remove_shard_dedup_entries_noop_on_unknown_hash<
    C: DirectAccessClient + DeletionControlableClient + 'static,
>(
    client: Arc<C>,
) {
    let file = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();

    let bogus_hash = MerkleHash::from([0xFFu8; 32]);
    client.remove_shard_dedup_entries(&bogus_hash).await.unwrap();

    for t in &file.terms {
        let dedup = client
            .query_for_global_dedup_shard("default", &t.chunk_hashes[0])
            .await
            .unwrap();
        assert!(dedup.is_some(), "Existing dedup entries should be untouched");
    }
}

/// Tests that verify_integrity passes after deleting a file (soft-delete does not break integrity).
async fn test_verify_integrity_after_file_deletion<C: DirectAccessClient + DeletionControlableClient + 'static>(
    client: Arc<C>,
) {
    let file1 = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let _file2 = client.upload_random_file(&[(2, (0, 2))], 2048).await.unwrap();
    client.verify_integrity().await.unwrap();

    client.delete_file_entry(&file1.file_hash).await.unwrap();
    client.verify_integrity().await.unwrap();
}

/// Tests that list_xorbs_and_tags returns entries matching list_xorbs with non-zero tags.
async fn test_list_xorbs_and_tags<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    assert!(client.list_xorbs_and_tags().await.unwrap().is_empty());

    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let expected_xorbs = expected_xorb_hashes(&[&file]);

    let xorbs_and_tags = client.list_xorbs_and_tags().await.unwrap();
    let listed_hashes: HashSet<MerkleHash> = xorbs_and_tags.iter().map(|(h, _)| *h).collect();
    assert_eq!(listed_hashes, expected_xorbs);

    let zero_tag: ObjectTag = [0u8; 32];
    for (_, tag) in &xorbs_and_tags {
        assert_ne!(tag, &zero_tag, "Tag should be non-zero");
    }
}

/// Tests conditional xorb deletion: wrong tag keeps the xorb, correct tag deletes it.
async fn test_delete_xorb_if_tag_matches<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let file = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();
    let xorb_hash = file.terms[0].xorb_hash;

    let xorbs_and_tags = client.list_xorbs_and_tags().await.unwrap();
    let (_, correct_tag) = xorbs_and_tags.iter().find(|(h, _)| *h == xorb_hash).unwrap();

    let wrong_tag: ObjectTag = [0xFFu8; 32];
    let deleted = client.delete_xorb_if_tag_matches(&xorb_hash, &wrong_tag).await.unwrap();
    assert!(!deleted, "Wrong tag should not delete the xorb");
    assert!(client.xorb_exists(&xorb_hash).await.unwrap(), "Xorb should still exist after wrong tag");

    let deleted = client.delete_xorb_if_tag_matches(&xorb_hash, correct_tag).await.unwrap();
    assert!(deleted, "Correct tag should delete the xorb");
    assert!(!client.xorb_exists(&xorb_hash).await.unwrap(), "Xorb should be gone after correct tag");
}

/// Tests that list_shards_with_tags returns entries matching list_shard_entries with non-zero tags.
async fn test_list_shards_with_tags<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    assert!(client.list_shards_with_tags().await.unwrap().is_empty());

    let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
    let _ = expected_xorb_hashes(&[&file]);

    let shards_and_tags = client.list_shards_with_tags().await.unwrap();
    let shard_entries = client.list_shard_entries().await.unwrap();
    let listed_hashes: HashSet<MerkleHash> = shards_and_tags.iter().map(|(h, _)| *h).collect();
    let expected_hashes: HashSet<MerkleHash> = shard_entries.into_iter().collect();
    assert_eq!(listed_hashes, expected_hashes);

    let zero_tag: ObjectTag = [0u8; 32];
    for (_, tag) in &shards_and_tags {
        assert_ne!(tag, &zero_tag, "Tag should be non-zero");
    }
}

/// Tests conditional shard deletion: wrong tag keeps the shard, correct tag deletes it.
async fn test_delete_shard_if_tag_matches<C: DirectAccessClient + DeletionControlableClient + 'static>(client: Arc<C>) {
    let _file = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();

    let shards_and_tags = client.list_shards_with_tags().await.unwrap();
    assert!(!shards_and_tags.is_empty());
    let (shard_hash, correct_tag) = &shards_and_tags[0];

    let wrong_tag: ObjectTag = [0xFFu8; 32];
    let deleted = client.delete_shard_if_tag_matches(shard_hash, &wrong_tag).await.unwrap();
    assert!(!deleted, "Wrong tag should not delete the shard");
    assert!(!client.list_shard_entries().await.unwrap().is_empty(), "Shard should still exist after wrong tag");

    let deleted = client.delete_shard_if_tag_matches(shard_hash, correct_tag).await.unwrap();
    assert!(deleted, "Correct tag should delete the shard");
    assert!(client.list_shard_entries().await.unwrap().is_empty(), "Shard should be gone after correct tag");
}
