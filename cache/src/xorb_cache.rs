use std::cmp::min;
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, info_span, warn};
use tracing_futures::Instrument;

use cas::key::Key;
use cas::singleflight;

use crate::metrics::{
    BLOCKS_READ, DATA_READ, READ_ERROR_COUNT, REQUEST_LATENCY_MS, REQUEST_THROUGHPUT,
    SOURCE_DISK_CACHE, SOURCE_REMOTE, SOURCE_SINGLEFLIGHT, WRITE_ERROR_COUNT,
};
use crate::{BlockConverter, BlockReadRequest, BlockReader, CacheError, DiskCache, FileMetadata};
use crate::{Remote, XorbCache};

#[derive(Debug)]
pub struct XorbCacheImpl {
    cache: DiskCache,
    remote: Arc<dyn Remote>,
    block_converter: BlockConverter,
    request_merger: singleflight::Group<Arc<Vec<u8>>, CacheError>,
}

impl XorbCacheImpl {
    pub fn new(
        cache: DiskCache,
        remote: Arc<dyn Remote>,
        block_converter: BlockConverter,
        request_merger: singleflight::Group<Arc<Vec<u8>>, CacheError>,
    ) -> Self {
        XorbCacheImpl {
            cache,
            remote,
            block_converter,
            request_merger,
        }
    }

    async fn read_block_from_cache(&self, request: &BlockReadRequest) -> Option<Vec<u8>> {
        let block_id = request.block_range().idx();
        // check cache for block
        let result = self
            .cache
            .get(request)
            .instrument(info_span!("cache_read_block", %block_id))
            .await;
        match result {
            Ok(data) => {
                debug!("Found data for block: {} in cache!", block_id);
                return Some(data);
            }
            Err(CacheError::BlockNotFound) => {
                debug!("Cache miss for block: {}", block_id);
            }
            Err(e) => {
                READ_ERROR_COUNT.inc();
                debug!(
                    "Unexpected issue reading block: {} from cache: {:?}",
                    block_id, e
                );
            }
        };
        None
    }

    async fn read_block_from_remote(
        remote: Arc<dyn Remote>,
        cache: DiskCache,
        key: Key,
        full_block_request: BlockReadRequest,
    ) -> Result<Arc<Vec<u8>>, CacheError> {
        let block_id = full_block_request.block_range().idx();
        let block_offsets_abs = full_block_request.block_range().to_abs_offsets();
        let data = remote
            .fetch(&key, block_offsets_abs)
            .instrument(info_span!("cache_read_remote", %block_id))
            .await
            .map_err(CacheError::from)?;
        let data = Arc::new(data);
        debug!(
            "Found data for block: {} in remote!",
            full_block_request.block_range().idx()
        );
        // cache the block
        let res = cache
            .put(&full_block_request, &data)
            .instrument(info_span!("cache_put_block", %block_id))
            .await;
        if res.is_ok() {
            debug!(
                "Stored block: {} in cache",
                full_block_request.block_range().idx()
            );
        } else if let Err(e) = res {
            WRITE_ERROR_COUNT.inc();
            // This is debug as it doesn't actually cause problems.
            info!(
                "Failed to store block: {} in cache: err: {:?}",
                full_block_request.block_range().idx(),
                e
            );
        }
        Ok(data)
    }

    /// Note: assumes that the range has been sanitized (i.e. 0 <= start <= end <= length)
    /// TODO: Break this up into more logical pieces, parallelize reads for better performance,
    ///       instrument with cache hit-rates (BHR/OHR), and stream results to callers.
    async fn fetch_range_internal(
        &self,
        key: &Key,
        range: Range<u64>,
        size: Option<u64>,
    ) -> Result<Vec<u8>, CacheError> {
        let mut vec: Vec<u8> = Vec::with_capacity((range.end - range.start) as usize);
        //TODO: do we need versions?
        let md = Arc::new(FileMetadata::new(key.to_string(), size, 1));

        for block_range in self.block_converter.to_block_ranges(range) {
            let start = SystemTime::now();
            let request = BlockReadRequest::from_block_range(block_range, md.clone());
            let block_id = request.block_range().idx();
            debug!("Block {} requested", block_id);
            // check cache for block
            if let Some(mut data) = self.read_block_from_cache(&request).await {
                let len = data.len();
                vec.append(&mut data);
                observe_read(SOURCE_DISK_CACHE, start, len);
                continue;
            }

            // load data from remote
            // expand read to load entire block so we can cache it and merge the request
            // with any concurrent reads trying to fetch the same block.
            let full_block = self
                .block_converter
                .get_full_block_range_for(block_id, md.size())
                .ok_or_else(|| {
                    CacheError::InvariantViolated("couldn't extend block size".to_string())
                })?;
            let full_block_request = BlockReadRequest::from_block_range(full_block, md.clone());

            let request_key = format!("remote_{full_block_request}");
            let (res, is_owner) = self
                .request_merger
                .work(
                    request_key.as_str(),
                    Self::read_block_from_remote(
                        self.remote.clone(),
                        self.cache.clone(),
                        key.clone(),
                        full_block_request,
                    ),
                )
                .await;
            let data = res?;
            let source = if is_owner {
                SOURCE_REMOTE
            } else {
                SOURCE_SINGLEFLIGHT
            };
            observe_read(source, start, data.len());

            // return sub-range of data the caller requested
            if (request.start_off() as usize) < data.len() {
                let request_end_off = min(request.end_off() as usize, data.len());
                let return_data = &data[(request.start_off() as usize)..request_end_off];
                vec.extend(return_data.iter());
            }
        }
        debug!("Finished reading data. Read: {} bytes", vec.len());
        Ok(vec)
    }

    /// Stores a key into cache, reading the value from the provided Reader
    async fn put_cache_internal(&self, key: &Key, contents: &[u8]) -> Result<(), CacheError> {
        let len = contents.len() as u64;
        let md = Arc::new(FileMetadata::new(key.to_string(), Some(len), 1));

        for block_range in self.block_converter.to_block_ranges(0..len) {
            let block_id = block_range.idx();
            // Since we are breaking up the content into multiple blocks, we need to use
            // absolute offsets instead of relative offsets.
            let range = block_range.to_abs_offsets();
            let data_slice = &contents[range.start as usize..range.end as usize];

            let full_block_request = BlockReadRequest::from_block_range(block_range, md.clone());
            // cache the block
            let res = self
                .cache
                .put(&full_block_request, data_slice)
                .instrument(info_span!("cache_put_block", %block_id))
                .await;
            if res.is_ok() {
                debug!(
                    "Stored block: {} in cache",
                    full_block_request.block_range().idx()
                );
            } else if let Err(e) = res {
                WRITE_ERROR_COUNT.inc();
                info!(
                    "Failed to store block: {} in cache: err: {:?}",
                    full_block_request.block_range().idx(),
                    e
                );
            }
        }
        Ok(())
    }
}

fn observe_read(source: &str, start: SystemTime, size: usize) {
    let labels = [source];
    BLOCKS_READ.with_label_values(&labels).inc();
    DATA_READ.with_label_values(&labels).inc_by(size as u64);
    match start.elapsed() {
        Ok(elapsed) => {
            let elapsed_ms = elapsed.as_millis();
            REQUEST_LATENCY_MS
                .with_label_values(&labels)
                .observe(elapsed_ms as f64);
            // 8bit / Byte * 1e3 ms / s * 1 MB / 1e6 Byte
            let throughput_mbps = (size * 8) as f64 / (elapsed_ms as f64 * 1000.0);
            REQUEST_THROUGHPUT
                .with_label_values(&labels)
                .observe(throughput_mbps);
        }
        Err(e) => {
            warn!("Stopwatch error occurred: {:?}", e)
        }
    }
}

#[async_trait::async_trait]
impl XorbCache for XorbCacheImpl {
    #[tracing::instrument(skip(self, key), name = "cache_read")]
    async fn fetch_xorb_range(
        &self,
        key: &Key,
        range: Range<u64>,
        size: Option<u64>,
    ) -> Result<Vec<u8>, CacheError> {
        if range.end < range.start {
            return Err(CacheError::InvalidRange(range.start, range.end));
        }

        let (start_off, end_off) = match size {
            Some(size) => (min(range.start, size), min(range.end, size)),
            None => (range.start, range.end),
        };
        self.fetch_range_internal(key, start_off..end_off, size)
            .await
    }
    async fn put_cache(&self, key: &Key, contents: &[u8]) -> Result<(), CacheError> {
        self.put_cache_internal(key, contents).await
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};
    use test_context::futures::future::join;
    use tokio::time::sleep;

    use crate::{util::test_utils::CacheDirTest, MockRemote};

    use super::*;

    fn new_test_xc(
        dir_prefix: &str,
        block_size: u64,
        capacity: u64,
        mock_remote: Arc<dyn Remote>,
    ) -> (CacheDirTest, XorbCacheImpl) {
        let test_cache_dir = CacheDirTest::new(dir_prefix);
        let disk_cache = DiskCache::from_config(test_cache_dir.get_path_str(), capacity).unwrap();
        let block_converter = BlockConverter::new(block_size);
        let request_merger = singleflight::Group::new();

        (
            test_cache_dir,
            XorbCacheImpl::new(disk_cache, mock_remote, block_converter, request_merger),
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let mut mock_remote = MockRemote::new();
        mock_remote
            .expect_fetch()
            .times(1)
            .returning(|_, _| Ok(vec![13, 21, 7]));
        let (_dir, test_xc) = new_test_xc("__tmp_xorb_fetch", 143, 457, Arc::new(mock_remote));

        let test_key = Key::default();

        let result = test_xc
            .fetch_xorb_range(&test_key, 0..3, Some(10))
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result, vec![13, 21, 7]);

        let result_none = test_xc
            .fetch_xorb_range(&test_key, 0..3, Some(10))
            .await
            .unwrap();
        assert_eq!(result, result_none);
    }

    #[tokio::test]
    async fn test_put_cache_blocks() {
        let block_size: usize = 16;
        // insert content of the indicated size into the cache and verify that
        // the block(s) are stored correctly.
        let test_put_content = |size: usize| async move {
            let mut mock_remote = MockRemote::new();
            mock_remote
                .expect_fetch()
                .times(0)
                .returning(|_, _| Err(anyhow::anyhow!("not expected to call remote")));
            let mut rng = StdRng::seed_from_u64(0);
            let dir_prefix = format!("__tmp_xorb_put_{size}");
            let (_dir, test_xc) = new_test_xc(
                &dir_prefix,
                block_size as u64,
                u64::MAX,
                Arc::new(mock_remote),
            );
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data[..]);
            let test_key = Key::default();
            test_xc.put_cache(&test_key, &data[..]).await.unwrap();

            let retrieved_data = test_xc
                .fetch_xorb_range(&test_key, 0..(size as u64), None)
                .await
                .unwrap();

            if data != retrieved_data {
                for i in 0..size {
                    assert_eq!(data[i], retrieved_data[i], "Data at index {i} mismatch:");
                }
            }
        };

        // test various sizes of content
        test_put_content(0).await;
        test_put_content(3).await;
        test_put_content(block_size).await;
        test_put_content(block_size + 1).await;
        test_put_content(block_size * 2).await;
    }

    #[tokio::test]
    async fn test_range_preconditions() {
        let mut mock_remote = MockRemote::new();
        mock_remote.expect_fetch().times(0);
        let (_dir, test_xc) =
            new_test_xc("__tmp_xorb_preconditions", 143, 457, Arc::new(mock_remote));

        let test_key = Key::default();

        let result = test_xc
            .fetch_xorb_range(&test_key, 4..4, Some(10))
            .await
            .unwrap();
        assert!(result.is_empty());
        let result_none = test_xc
            .fetch_xorb_range(&test_key, 4..4, None)
            .await
            .unwrap();
        assert_eq!(result, result_none);
    }

    #[tokio::test]
    #[allow(clippy::reversed_empty_ranges)]
    async fn test_range_inverted_range() {
        let mut mock_remote = MockRemote::new();
        mock_remote.expect_fetch().times(0);
        let (_dir, test_xc) = new_test_xc("__tmp_xorb_inverted", 143, 457, Arc::new(mock_remote));

        let test_key = Key::default();

        let result = test_xc.fetch_xorb_range(&test_key, 7..3, Some(10)).await;
        assert!(result.is_err());
        let result_none = test_xc.fetch_xorb_range(&test_key, 7..3, None).await;
        assert!(result_none.is_err());
    }

    /// Mocks remote and keeps the number of times `fetch` is called.
    #[derive(Debug, Default)]
    struct FetchRecorder {
        times_called: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl Remote for FetchRecorder {
        async fn fetch(
            &self,
            _key: &Key,
            range: Range<u64>,
        ) -> std::result::Result<Vec<u8>, anyhow::Error> {
            sleep(Duration::new(0, 500_000_000)).await;
            self.times_called.fetch_add(1, Ordering::SeqCst);
            let mut vec = Vec::with_capacity((range.end - range.start) as usize);
            for i in range {
                vec.push(i as u8);
            }
            Ok(vec)
        }
    }

    #[tokio::test]
    async fn test_read_simultaneous() {
        let mock_remote = FetchRecorder::default();
        let times_called = mock_remote.times_called.clone();
        let (dir, test_xc) =
            new_test_xc("__tmp_xorb_simultaneous", 143, 457, Arc::new(mock_remote));

        let test_key = Key::default();

        let r1 = 0..30;
        let r2 = 30..47;

        let f1 = test_xc.fetch_xorb_range(&test_key, r1.clone(), Some(57));
        let f2 = test_xc.fetch_xorb_range(&test_key, r2.clone(), Some(57));
        let (res1, res2) = join(f1, f2).await;
        assert_eq!(30, res1.unwrap().len());
        assert_eq!(17, res2.unwrap().len());
        assert_eq!(1, times_called.load(Ordering::SeqCst));
        assert_eq!(1, dir.get_entries().len());

        let f1 = test_xc.fetch_xorb_range(&test_key, r1, None);
        let f2 = test_xc.fetch_xorb_range(&test_key, r2, None);
        let (res1, res2) = join(f1, f2).await;
        assert_eq!(30, res1.unwrap().len());
        assert_eq!(17, res2.unwrap().len());
        assert_eq!(1, times_called.load(Ordering::SeqCst));
        assert_eq!(1, dir.get_entries().len());
    }
}
