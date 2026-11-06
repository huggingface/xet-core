//! A file given up mid-stream must not block, or corrupt, the finalize of the session it belonged to.
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_runtime::test_set_constants;

// Tiny chunks and xorbs, so that the dedup work of a single write cuts many xorbs.
test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 4 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 4;
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use xet_data::processing::test_utils::TestEnvironment;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy};

    /// A stream abandoned right after a large write leaves that write's dedup work running in the background,
    /// still cutting and registering xorbs. Finalize must wait for it, so every xorb it cuts is uploaded with
    /// the session; the session's progress then adds up, and the file finished alongside round-trips.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_finalize_waits_for_abandoned_stream_background_work() {
        let env = TestEnvironment::new().await;
        let session = FileUploadSession::new(env.config.clone()).await.unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        let mut abandoned_data = vec![0u8; 4 << 20];
        rng.fill(&mut abandoned_data[..]);
        let (abandoned_id, mut abandoned) =
            session.start_clean(Some("abandoned".into()), None, Sha256Policy::Skip).unwrap();
        abandoned.add_data(&abandoned_data).await.unwrap();
        drop(abandoned);
        session.abandon_file(abandoned_id);

        let mut kept_data = vec![0u8; 64 << 10];
        rng.fill(&mut kept_data[..]);
        let (_, mut kept) = session.start_clean(Some("kept".into()), None, Sha256Policy::Skip).unwrap();
        kept.add_data(&kept_data).await.unwrap();
        let (kept_info, _) = kept.finish().await.unwrap();

        let (_, report) = session.clone().finalize_with_report().await.unwrap();
        assert_eq!(report.total_bytes_completed, report.total_bytes);
        assert_eq!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
        let abandoned_item = session.item_report(abandoned_id).unwrap();
        assert_eq!(abandoned_item.bytes_completed, abandoned_item.total_bytes);

        let dest = env.base_dir.join("kept.out");
        let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
        download.download_file(&kept_info, &dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), kept_data);
    }
}
