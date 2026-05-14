use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, Transaction, TransactionBehavior, params};

use super::cache_item::{CacheItem, VerificationCell};
use super::{key_dir, try_parse_key};
use crate::cas_types::Key;
use crate::chunk_cache::error::ChunkCacheError;

const METADATA_DB_FILE_NAME: &str = ".chunk_cache.sqlite3";

const SQL_UPDATE_ACCESS: &str = "
    UPDATE cache_items
    SET last_access_ns = ?1
    WHERE key_name = ?2
      AND range_start = ?3
      AND range_end = ?4
      AND len = ?5
      AND checksum = ?6
";
const SQL_INSERT_ITEM_IF_MISSING: &str = "
    INSERT OR IGNORE INTO cache_items
        (key_name, range_start, range_end, len, checksum, last_access_ns)
    VALUES (?2, ?3, ?4, ?5, ?6, ?1)
";
const SQL_DELETE_ITEM: &str = "
    DELETE FROM cache_items
    WHERE key_name = ?1
      AND range_start = ?2
      AND range_end = ?3
      AND len = ?4
      AND checksum = ?5
";
const SQL_TOTAL_BYTES: &str = "SELECT COALESCE(SUM(len), 0) FROM cache_items";
const SQL_LEAST_RECENTLY_USED_ITEMS: &str = "
    SELECT key_name, range_start, range_end, len, checksum
    FROM cache_items
    ORDER BY last_access_ns ASC
    LIMIT ?1
";
const SQL_RECORD_INSERT: &str = "
    INSERT OR REPLACE INTO cache_items
        (key_name, range_start, range_end, len, checksum, last_access_ns)
    VALUES (?2, ?3, ?4, ?5, ?6, ?1)
";
const SQL_ALL_ITEMS: &str = "SELECT key_name, range_start, range_end, len, checksum FROM cache_items";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheMetadataItem {
    pub(crate) key: Key,
    pub(crate) item: CacheItem,
}

pub(crate) struct CacheMetadataDb {
    conn: Connection,
    access_update_interval_ns: u64,
    recent_access_updates: HashMap<CacheMetadataItem, i64>,
}

impl std::fmt::Debug for CacheMetadataDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheMetadataDb")
            .field("access_update_interval_ns", &self.access_update_interval_ns)
            .field("recent_access_updates", &self.recent_access_updates.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct CacheMetadataTransaction<'tx> {
    tx: &'tx Transaction<'tx>,
}

impl CacheMetadataDb {
    pub(crate) fn open(cache_root: &Path, access_update_interval_ns: u64) -> Result<Self, ChunkCacheError> {
        std::fs::create_dir_all(cache_root)?;
        let conn = Connection::open(cache_root.join(METADATA_DB_FILE_NAME)).map_err(sqlite_error)?;
        conn.busy_timeout(Duration::from_secs(30)).map_err(sqlite_error)?;
        conn.pragma_update(None, "journal_mode", "WAL").map_err(sqlite_error)?;
        conn.pragma_update(None, "synchronous", "NORMAL").map_err(sqlite_error)?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cache_items (
                key_name TEXT NOT NULL,
                range_start INTEGER NOT NULL,
                range_end INTEGER NOT NULL,
                len INTEGER NOT NULL,
                checksum INTEGER NOT NULL,
                last_access_ns INTEGER NOT NULL,
                PRIMARY KEY (key_name, range_start, range_end, len, checksum)
            );
            CREATE INDEX IF NOT EXISTS cache_items_lru_idx ON cache_items(last_access_ns);
            ",
        )
        .map_err(sqlite_error)?;

        Ok(Self {
            conn,
            access_update_interval_ns,
            recent_access_updates: HashMap::new(),
        })
    }

    pub(crate) fn reconcile(
        &mut self,
        state: &HashMap<Key, Vec<VerificationCell<CacheItem>>>,
    ) -> Result<(), ChunkCacheError> {
        let now = current_time_ns();
        self.with_write_transaction(|metadata| {
            let live_items = state
                .iter()
                .flat_map(|(key, items)| {
                    items.iter().map(|item| CacheMetadataItem {
                        key: key.clone(),
                        item: item.as_ref().clone(),
                    })
                })
                .collect::<HashSet<_>>();

            for metadata_item in metadata.all_items()? {
                if !live_items.contains(&metadata_item) {
                    metadata.delete_item(&metadata_item.key, &metadata_item.item)?;
                }
            }

            for metadata_item in live_items {
                metadata.insert_item_if_missing(&metadata_item.key, &metadata_item.item, now)?;
            }

            Ok(())
        })
    }

    pub(crate) fn record_access(&mut self, key: &Key, item: &CacheItem) -> Result<(), ChunkCacheError> {
        let metadata_item = CacheMetadataItem {
            key: key.clone(),
            item: item.clone(),
        };
        let now = current_time_ns();
        let access_update_interval_ns = i64::try_from(self.access_update_interval_ns).unwrap_or(i64::MAX);
        if self
            .recent_access_updates
            .get(&metadata_item)
            .is_some_and(|last_update| now.saturating_sub(*last_update) < access_update_interval_ns)
        {
            return Ok(());
        }

        let updated_rows = {
            let mut stmt = self.conn.prepare_cached(SQL_UPDATE_ACCESS).map_err(sqlite_error)?;
            stmt.execute(metadata_params(key, item, now)?).map_err(sqlite_error)?
        };

        if updated_rows == 0 {
            self.insert_item_if_missing(key, item, now)?;
        }

        self.recent_access_updates.insert(metadata_item, now);
        Ok(())
    }

    pub(crate) fn delete_item(&mut self, key: &Key, item: &CacheItem) -> Result<(), ChunkCacheError> {
        self.delete_item_impl(key, item)?;
        self.recent_access_updates.remove(&CacheMetadataItem {
            key: key.clone(),
            item: item.clone(),
        });
        Ok(())
    }

    pub(crate) fn with_write_transaction<T>(
        &mut self,
        operation: impl FnOnce(&CacheMetadataTransaction<'_>) -> Result<T, ChunkCacheError>,
    ) -> Result<T, ChunkCacheError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let result = {
            let metadata_tx = CacheMetadataTransaction { tx: &tx };
            operation(&metadata_tx)
        };

        match result {
            Ok(value) => {
                tx.commit().map_err(sqlite_error)?;
                Ok(value)
            },
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            },
        }
    }

    fn insert_item_if_missing(&self, key: &Key, item: &CacheItem, last_access_ns: i64) -> Result<(), ChunkCacheError> {
        let mut stmt = self.conn.prepare_cached(SQL_INSERT_ITEM_IF_MISSING).map_err(sqlite_error)?;
        stmt.execute(metadata_params(key, item, last_access_ns)?)
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn delete_item_impl(&self, key: &Key, item: &CacheItem) -> Result<(), ChunkCacheError> {
        let mut stmt = self.conn.prepare_cached(SQL_DELETE_ITEM).map_err(sqlite_error)?;
        stmt.execute(item_identity_params(key, item)?).map_err(sqlite_error)?;
        Ok(())
    }
}

impl CacheMetadataTransaction<'_> {
    pub(crate) fn total_bytes(&self) -> Result<u64, ChunkCacheError> {
        let mut stmt = self.tx.prepare_cached(SQL_TOTAL_BYTES).map_err(sqlite_error)?;
        let total_bytes: i64 = stmt.query_row([], |row| row.get(0)).map_err(sqlite_error)?;
        u64::try_from(total_bytes).map_err(ChunkCacheError::general)
    }

    pub(crate) fn least_recently_used_items(&self, limit: usize) -> Result<Vec<CacheMetadataItem>, ChunkCacheError> {
        let limit = i64::try_from(limit).map_err(ChunkCacheError::general)?;
        let mut stmt = self.tx.prepare_cached(SQL_LEAST_RECENTLY_USED_ITEMS).map_err(sqlite_error)?;
        let rows = stmt
            .query_map(params![limit], |row| metadata_item_from_row(row))
            .map_err(sqlite_error)?;

        rows.map(|row| row.map_err(sqlite_error)).collect()
    }

    pub(crate) fn record_insert(&self, key: &Key, item: &CacheItem) -> Result<(), ChunkCacheError> {
        let now = current_time_ns();
        let mut stmt = self.tx.prepare_cached(SQL_RECORD_INSERT).map_err(sqlite_error)?;
        stmt.execute(metadata_params(key, item, now)?).map_err(sqlite_error)?;
        Ok(())
    }

    pub(crate) fn insert_item_if_missing(
        &self,
        key: &Key,
        item: &CacheItem,
        last_access_ns: i64,
    ) -> Result<(), ChunkCacheError> {
        let mut stmt = self.tx.prepare_cached(SQL_INSERT_ITEM_IF_MISSING).map_err(sqlite_error)?;
        stmt.execute(metadata_params(key, item, last_access_ns)?)
            .map_err(sqlite_error)?;
        Ok(())
    }

    pub(crate) fn delete_item(&self, key: &Key, item: &CacheItem) -> Result<(), ChunkCacheError> {
        let mut stmt = self.tx.prepare_cached(SQL_DELETE_ITEM).map_err(sqlite_error)?;
        stmt.execute(item_identity_params(key, item)?).map_err(sqlite_error)?;
        Ok(())
    }

    fn all_items(&self) -> Result<Vec<CacheMetadataItem>, ChunkCacheError> {
        let mut stmt = self.tx.prepare_cached(SQL_ALL_ITEMS).map_err(sqlite_error)?;
        let rows = stmt.query_map([], |row| metadata_item_from_row(row)).map_err(sqlite_error)?;

        rows.map(|row| row.map_err(sqlite_error)).collect()
    }
}

fn metadata_item_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CacheMetadataItem> {
    let key_name: String = row.get(0)?;
    let range_start: u32 = row.get(1)?;
    let range_end: u32 = row.get(2)?;
    let len: u64 = row.get(3)?;
    let checksum: u32 = row.get(4)?;
    let key = try_parse_key(key_name.as_bytes())
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;

    Ok(CacheMetadataItem {
        key,
        item: CacheItem {
            range: crate::cas_types::ChunkRange::new(range_start, range_end),
            len,
            checksum,
        },
    })
}

fn metadata_params(
    key: &Key,
    item: &CacheItem,
    last_access_ns: i64,
) -> Result<(i64, String, u32, u32, u64, u32), ChunkCacheError> {
    Ok((last_access_ns, key_name(key)?, item.range.start, item.range.end, item.len, item.checksum))
}

fn item_identity_params(key: &Key, item: &CacheItem) -> Result<(String, u32, u32, u64, u32), ChunkCacheError> {
    Ok((key_name(key)?, item.range.start, item.range.end, item.len, item.checksum))
}

fn key_name(key: &Key) -> Result<String, ChunkCacheError> {
    key_dir(key)
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .ok_or(ChunkCacheError::Infallible)
}

fn current_time_ns() -> i64 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn sqlite_error(error: rusqlite::Error) -> ChunkCacheError {
    ChunkCacheError::general(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas_types::ChunkRange;
    use crate::chunk_cache::random_key;

    const DEFAULT_ACCESS_UPDATE_INTERVAL_NS: u64 =
        xet_runtime::config::groups::chunk_cache::DEFAULT_CHUNK_CACHE_ACCESS_UPDATE_INTERVAL_NS;

    #[test]
    fn key_name_round_trips_through_disk_key_encoding() {
        let mut rng = rand::rng();
        let key = random_key(&mut rng);
        let key_name = key_name(&key).unwrap();
        assert_eq!(try_parse_key(key_name.as_bytes()).unwrap(), key);
    }

    fn open_db(cache_root: &Path) -> CacheMetadataDb {
        CacheMetadataDb::open(cache_root, DEFAULT_ACCESS_UPDATE_INTERVAL_NS).unwrap()
    }

    #[test]
    fn record_insert_and_access_updates_lru_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut db = open_db(temp_dir.path());
        let mut rng = rand::rng();
        let key_a = random_key(&mut rng);
        let key_b = random_key(&mut rng);
        let item_a = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 1,
        };
        let item_b = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 2,
        };

        db.with_write_transaction(|tx| {
            tx.record_insert(&key_a, &item_a)?;
            tx.record_insert(&key_b, &item_b)?;
            Ok(())
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.record_access(&key_a, &item_a).unwrap();

        let candidates = db.with_write_transaction(|tx| tx.least_recently_used_items(1)).unwrap();
        assert_eq!(candidates[0].key, key_b);
        assert_eq!(candidates[0].item, item_b);
    }

    #[test]
    fn reconcile_removes_stale_rows_and_keeps_existing_access_time() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut db = open_db(temp_dir.path());
        let mut rng = rand::rng();
        let key_a = random_key(&mut rng);
        let key_b = random_key(&mut rng);
        let key_c = random_key(&mut rng);
        let item_a = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 1,
        };
        let item_b = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 2,
        };
        let item_c = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 3,
        };

        db.with_write_transaction(|tx| {
            tx.record_insert(&key_a, &item_a)?;
            Ok(())
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.with_write_transaction(|tx| {
            tx.record_insert(&key_b, &item_b)?;
            Ok(())
        })
        .unwrap();

        std::thread::sleep(Duration::from_millis(2));
        let state = HashMap::from([
            (key_b.clone(), vec![VerificationCell::new_verified(item_b.clone())]),
            (key_c.clone(), vec![VerificationCell::new_verified(item_c.clone())]),
        ]);
        db.reconcile(&state).unwrap();

        let candidates = db.with_write_transaction(|tx| tx.least_recently_used_items(10)).unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].key, key_b);
        assert_eq!(candidates[0].item, item_b);
        assert_eq!(candidates[1].key, key_c);
        assert_eq!(candidates[1].item, item_c);
    }

    #[test]
    fn access_update_interval_zero_records_every_access() {
        let (candidates, key_a, item_a, key_b, item_b) = repeated_access_candidates(0);

        assert_eq!(candidates[0].key, key_b);
        assert_eq!(candidates[0].item, item_b);
        assert_eq!(candidates[1].key, key_a);
        assert_eq!(candidates[1].item, item_a);
    }

    #[test]
    fn large_access_update_interval_skips_repeated_access_updates() {
        let (candidates, key_a, item_a, key_b, item_b) = repeated_access_candidates(u64::MAX);

        assert_eq!(candidates[0].key, key_a);
        assert_eq!(candidates[0].item, item_a);
        assert_eq!(candidates[1].key, key_b);
        assert_eq!(candidates[1].item, item_b);
    }

    fn repeated_access_candidates(
        access_update_interval_ns: u64,
    ) -> (Vec<CacheMetadataItem>, Key, CacheItem, Key, CacheItem) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut db = CacheMetadataDb::open(temp_dir.path(), access_update_interval_ns).unwrap();
        let mut rng = rand::rng();
        let key_a = random_key(&mut rng);
        let key_b = random_key(&mut rng);
        let item_a = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 1,
        };
        let item_b = CacheItem {
            range: ChunkRange::new(0, 1),
            len: 10,
            checksum: 2,
        };

        db.with_write_transaction(|tx| {
            tx.record_insert(&key_a, &item_a)?;
            Ok(())
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.with_write_transaction(|tx| {
            tx.record_insert(&key_b, &item_b)?;
            Ok(())
        })
        .unwrap();

        std::thread::sleep(Duration::from_millis(2));
        db.record_access(&key_a, &item_a).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.record_access(&key_b, &item_b).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.record_access(&key_a, &item_a).unwrap();

        let candidates = db.with_write_transaction(|tx| tx.least_recently_used_items(2)).unwrap();
        (candidates, key_a, item_a, key_b, item_b)
    }
}
