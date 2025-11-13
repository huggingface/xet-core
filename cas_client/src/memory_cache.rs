// A single-threaded cache with Bélády's optimal replacement policy, with optional disk back up.

use std::collections::HashMap;
use std::fmt::Display;

use bytes::Bytes;
use serde::Serialize;
use tempfile::TempDir;

pub struct ClairvoyantHybridCache<K, V: Serialize> {
    memory: HashMap<K, V>,
    disk: Option<DiskStorage>,
}

impl<K, V: Serialize> ClairvoyantHybridCache<K, V> {}

struct DiskStorage {
    _tempdir: TempDir,
}

impl DiskStorage {
    fn put<K>(&self, k: K, v: &[u8]) -> std::io::Result<()>
    where
        K: Display,
    {
        todo!()
    }

    fn get<K>(&self, k: &K) -> std::io::Result<Bytes>
    where
        K: Display,
    {
        todo!()
    }

    fn remove<K>(&self, k: &K)
    where
        K: Display,
    {
        todo!()
    }
}
