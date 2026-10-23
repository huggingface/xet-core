//! Stress test for giving up stream uploads inside a commit, then retrying them.
//!
//! Runs with tiny chunks and xorbs, so every stream cuts many xorbs while it is written: aborts land while
//! dedup work and xorb uploads of the same commit are in flight.
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_runtime::test_set_constants;

test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 4 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 4;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use rand::prelude::*;
    use tempfile::tempdir;
    use xet::xet_session::{Sha256Policy, XetFileMetadata, XetSessionBuilder, XetStreamUpload, XetUploadCommit};

    fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        rng.fill(&mut data[..]);
        data
    }

    /// Writes `data` to `stream` in blocks of random size.
    async fn write_in_blocks(stream: &XetStreamUpload, data: &[u8], rng: &mut StdRng) {
        let mut pos = 0;
        while pos < data.len() {
            let end = (pos + rng.random_range(1..=64 << 10)).min(data.len());
            stream.write(data[pos..end].to_vec()).await.unwrap();
            pos = end;
        }
    }

    /// Streams `data` as `name` the way a caller retrying a failing source would: `n_aborted` attempts are
    /// given up after a random prefix, either between writes or while a write is in flight. Then a last
    /// attempt writes everything and finishes, unless `give_up` is set.
    async fn stream_with_retries(
        commit: XetUploadCommit,
        name: String,
        data: Arc<Vec<u8>>,
        n_aborted: usize,
        give_up: bool,
        mut rng: StdRng,
    ) -> Option<XetFileMetadata> {
        for _ in 0..n_aborted {
            let attempt = commit.upload_stream(Some(name.clone()), Sha256Policy::Compute).await.unwrap();
            let cut = rng.random_range(0..=data.len());
            write_in_blocks(&attempt, &data[..cut], &mut rng).await;
            if rng.random_bool(0.5) {
                attempt.abort();
            } else {
                let in_flight = tokio::spawn({
                    let (attempt, rest) = (attempt.clone(), data[cut..].to_vec());
                    async move { attempt.write(rest).await }
                });
                attempt.abort();
                // The write either completed before the abort or was cancelled by it.
                let _ = in_flight.await.unwrap();
            }
        }
        if give_up {
            return None;
        }
        let stream = commit.upload_stream(Some(name), Sha256Policy::Compute).await.unwrap();
        write_in_blocks(&stream, &data, &mut rng).await;
        Some(stream.finish().await.unwrap())
    }

    /// Streams are written concurrently into one commit. A seeded random set of attempts is aborted and
    /// retried, some files are given up for good, and one last stream is aborted right before `commit()`
    /// while its dedup work is still cutting xorbs. The commit must hold exactly the finished attempts, its
    /// progress must add up, and every finished file must download back intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_commit_with_concurrent_stream_aborts_and_retries() {
        for seed in 0..4 {
            let temp = tempdir().unwrap();
            let endpoint = format!("local://{}", temp.path().join("cas").display());
            let session = XetSessionBuilder::new().build().unwrap();
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            let mut rng = StdRng::seed_from_u64(seed);

            let files: Vec<(String, Arc<Vec<u8>>)> = (0..8)
                .map(|i| {
                    let len = rng.random_range(1..512 << 10);
                    (format!("file_{i}.bin"), Arc::new(random_bytes(&mut rng, len)))
                })
                .collect();
            let uploads: Vec<_> = files
                .iter()
                .map(|(name, data)| {
                    let (n_aborted, give_up) = (rng.random_range(0..=2), rng.random_bool(0.25));
                    let file_rng = StdRng::seed_from_u64(rng.random());
                    tokio::spawn(stream_with_retries(
                        commit.clone(),
                        name.clone(),
                        data.clone(),
                        n_aborted,
                        give_up,
                        file_rng,
                    ))
                })
                .collect();
            let mut finished = Vec::new();
            for ((name, data), upload) in files.iter().zip(uploads) {
                if let Some(meta) = upload.await.unwrap() {
                    finished.push((name, data, meta));
                }
            }

            let last = commit
                .upload_stream(Some("last.bin".into()), Sha256Policy::Compute)
                .await
                .unwrap();
            last.write(random_bytes(&mut rng, 1 << 20)).await.unwrap();
            last.abort();

            let report = commit.commit().await.unwrap();
            let expected: HashSet<_> = finished.iter().map(|(_, _, meta)| meta.task_id).collect();
            assert_eq!(report.uploads.keys().copied().collect::<HashSet<_>>(), expected, "seed {seed}");
            let progress = &report.progress;
            assert_eq!(progress.total_bytes_completed, progress.total_bytes, "seed {seed}");
            assert_eq!(progress.total_transfer_bytes_completed, progress.total_transfer_bytes, "seed {seed}");
            let finished_bytes: u64 = finished.iter().map(|(_, data, _)| data.len() as u64).sum();
            assert!(progress.total_bytes >= finished_bytes, "seed {seed}");

            let group = session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            for (name, _, meta) in &finished {
                group
                    .download_file_to_path(meta.xet_info.clone(), temp.path().join(name))
                    .await
                    .unwrap();
            }
            group.finish().await.unwrap();
            for (name, data, _) in &finished {
                assert_eq!(&std::fs::read(temp.path().join(name)).unwrap(), data.as_ref(), "seed {seed}: {name}");
            }
        }
    }
}
