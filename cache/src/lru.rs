use crate::metrics::{LRU_REQUESTS, STATUS_EXPIRED, STATUS_HIT, STATUS_MISS};
use chrono::{DateTime, Duration, Utc};
use lru::LruCache;
use std::{self, hash::Hash};

const LRU_CACHE_CAPACITY: usize = 1000;
const LRU_CACHE_TIMEOUT_MINUTES: i64 = 5;

struct Timestamped<Value> {
    t: DateTime<Utc>,
    value: Value,
}

impl<Value> Timestamped<Value> {
    fn new(value: Value) -> Timestamped<Value> {
        Timestamped {
            t: Utc::now(),
            value,
        }
    }
}

pub struct Lru<K: Eq + Hash + Clone, V: Clone> {
    lru: LruCache<K, Timestamped<V>>,
    duration: Duration,
    name: String,
}

impl<K: Eq + Hash + Clone, V: Clone> Default for Lru<K, V> {
    fn default() -> Self {
        Self::new(LRU_CACHE_CAPACITY, LRU_CACHE_TIMEOUT_MINUTES, "default")
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Lru<K, V> {
    pub fn new(capacity: usize, duration_minutes: i64, name: &str) -> Self {
        Self {
            lru: LruCache::new(std::num::NonZero::new(capacity.max(1)).unwrap()),
            duration: {
                #[allow(deprecated)] // Sometimes the linter seems to hate this....
                Duration::minutes(duration_minutes)
            },
            name: name.to_string(),
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn fetch(&mut self, key: &K) -> Option<V> {
        if let Some(timestamped_v) = self.lru.get(key) {
            if Utc::now() - timestamped_v.t < self.duration {
                let val = timestamped_v.value.clone();
                self.observe_request(STATUS_HIT);
                return Some(val);
            } else {
                self.observe_request(STATUS_EXPIRED);
                self.lru.pop_entry(key);
            }
        } else {
            self.observe_request(STATUS_MISS);
        }
        None
    }

    pub fn put(&mut self, key: &K, val: V) {
        self.lru.put(key.clone(), Timestamped::new(val));
    }

    fn observe_request(&self, status: &str) {
        LRU_REQUESTS.with_label_values(&[&self.name, status]).inc();
    }
}
