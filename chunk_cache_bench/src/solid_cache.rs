use chunk_cache::error::ChunkCacheError;
use chunk_cache::ChunkCache;
use r2d2_postgres::postgres::NoTls;

use crate::ChunkCacheExt;

#[derive(Clone)]
pub struct SolidCache {
    pool: r2d2::Pool<r2d2_postgres::PostgresConnectionManager<NoTls>>,
}

impl Default for SolidCache {
    fn default() -> Self {
        let config = "host=localhost user=postgres dbname=cache".parse().unwrap();
        let manager = r2d2_postgres::PostgresConnectionManager::new(config, NoTls);
        let pool = r2d2::Pool::new(manager).unwrap();

        Self { pool }
    }
}

impl SolidCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChunkCacheExt for SolidCache {
    fn _initialize(_cache_root: std::path::PathBuf, _capacity: u64) -> Result<Self, ChunkCacheError> {
        Ok(Self::new())
    }

    fn name() -> &'static str {
        "solidcache"
    }
}

impl ChunkCache for SolidCache {
    fn get(&self, key: &cas_types::Key, range: &cas_types::ChunkRange) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        let start = range.start as i32;
        let end = range.end as i32;

        let mut conn = self.pool.get().map_err(ChunkCacheError::general)?;
        let row = match conn.query_one(
            "SELECT * FROM cache WHERE key = $1 AND start <= $2 AND \"end\" >= $3 LIMIT 1",
            &[&key.to_string(), &start, &end],
        ) {
            Ok(row) => row,
            Err(_) => return Ok(None),
        };

        let start: i32 = row.get(1);
        let start = start as u32;
        let chunk_byte_indices: Vec<i32> = row.get(3);
        let data: &[u8] = row.get(4);
        let first = chunk_byte_indices[(range.start - start) as usize] as usize;
        let last = chunk_byte_indices[(range.end - start) as usize] as usize;
        let res = data[first..last].to_vec();
        Ok(Some(res))
    }

    fn put(
        &self,
        key: &cas_types::Key,
        range: &cas_types::ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        let start = range.start as i32;
        let end = range.end as i32;
        let cbi: Vec<i32> = chunk_byte_indices.iter().map(|v| *v as i32).collect();

        let mut conn = self.pool.get().map_err(ChunkCacheError::general)?;
        if conn
            .query_one(
                "SELECT start FROM cache WHERE key = $1 AND start <= $2 AND \"end\" >= $3 LIMIT 1",
                &[&key.to_string(), &start, &end],
            )
            .is_ok()
        {
            return Ok(());
        };

        conn.execute(
            "INSERT INTO cache (key, start, \"end\", chunk_byte_indices, data) VALUES ($1, $2, $3, $4, $5)",
            &[&key.to_string(), &start, &end, &cbi, &data],
        )
        .map_err(ChunkCacheError::general)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chunk_cache::{ChunkCache, RandomEntryIterator};
    use rand::thread_rng;

    use super::SolidCache;

    #[test]
    #[ignore = "need a running postgres"]
    fn test_postgres() {
        let cache = SolidCache::new();
        let mut it = RandomEntryIterator::new(thread_rng());
        let mut kr = Vec::new();
        for _ in 0..5 {
            let (key, range, chunk_byte_indices, data) = it.next().unwrap();
            let result = cache.put(&key, &range, &chunk_byte_indices, &data);
            assert!(result.is_ok(), "{result:?}");
            kr.push((key, range));
        }
        for (key, range) in kr {
            let result = cache.get(&key, &range);
            assert!(result.is_ok(), "{result:?}");
            let result = result.unwrap();
            assert!(result.is_some(), "{result:?}");
        }
        let (key, range) = it.next_key_range();
        let result = cache.get(&key, &range);
        assert!(result.is_ok(), "{result:?}");
        let result = result.unwrap();
        assert!(result.is_none(), "{result:?}");
    }

    #[test]
    #[ignore = "need a running postgres"]
    fn test_postgres_get_miss() {
        let cache = SolidCache::new();
        let mut it = RandomEntryIterator::new(thread_rng());

        let (key, range) = it.next_key_range();
        let result = cache.get(&key, &range);
        assert!(result.is_ok(), "{result:?}");
        let result = result.unwrap();
        assert!(result.is_none(), "{result:?}");
    }
}
