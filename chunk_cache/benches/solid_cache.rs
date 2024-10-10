use std::sync::{Arc, Mutex};

use cas_types::Key;
use chunk_cache::{error::ChunkCacheError, ChunkCache};
use merklehash::MerkleHash;
use postgres::NoTls;

struct SolidCache {
    conn: Arc<Mutex<postgres::Client>>,
}

impl SolidCache {
    pub fn new() -> Self {
        let conn = Arc::new(Mutex::new(
            postgres::Client::connect("host=localhost user=postgres", NoTls).unwrap(),
        ));
        Self { conn }
    }
}

impl ChunkCache for SolidCache {
    fn get(
        &self,
        key: &cas_types::Key,
        range: &cas_types::Range,
    ) -> Result<Option<Vec<u8>>, chunk_cache::error::ChunkCacheError> {
        let mut conn = self.conn.lock()?;

        let row = match conn.query_one(
            "SELECT * FROM cache WHERE key = $1 AND start <= $2 AND end >= $3 LIMIT 1",
            &[&key.to_string(), &range.start, &range.end],
        ) {
            Ok(row) => row,
            Err(_) => return Ok(None),
        };

        let chunk_byte_indicies: Vec<u32> = row.get(3);
        let data: &[u8] = row.get(4);
        let first = chunk_byte_indicies[range.start as usize] as usize;
        let last = chunk_byte_indicies[range.end as usize] as usize;
        let res = data[first..last].to_vec();
        Ok(Some(res))
    }

    fn put(
        &self,
        key: &cas_types::Key,
        range: &cas_types::Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), chunk_cache::error::ChunkCacheError> {
        let conn = self.conn.lock()?;
        todo!()
    }
}

fn key_from_string(s: &str) -> Key {
    let split: Vec<&str> = s.split('/').collect();
    let prefix = String::from(split[0]);
    let hash = MerkleHash::from_hex(split[1]).unwrap();
    Key { prefix, hash }
}
