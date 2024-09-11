use std::sync::Mutex;

use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec,
};

const DEFAULT_SERVICE: &str = "cas";
const NAMESPACE: &str = "cache";

// Allow the SERVICE to be dynamically configurable
lazy_static! {
    static ref SERVICE: Mutex<String> = Mutex::new(DEFAULT_SERVICE.to_string());
}

pub const SOURCE_DISK_CACHE: &str = "disk_cache";
pub const SOURCE_REMOTE: &str = "remote";
pub const SOURCE_SINGLEFLIGHT: &str = "singleflight";

// XorbCache metrics
lazy_static! {
    pub static ref BLOCKS_READ: IntCounterVec = register_int_counter_vec!(
        prefix_name(NAMESPACE, "block_count").as_str(),
        "count of blocks served by the cache by data source",
        &["source"]
    )
    .unwrap();
    pub static ref DATA_READ: IntCounterVec = register_int_counter_vec!(
        prefix_name(NAMESPACE, "data_count").as_str(),
        "count of bytes served by the cache by data source",
        &["source"]
    )
    .unwrap();
    pub static ref READ_ERROR_COUNT: IntCounter = register_int_counter!(
        prefix_name(NAMESPACE, "read_error_count").as_str(),
        "count of errors reading from the cache"
    )
    .unwrap();
    pub static ref WRITE_ERROR_COUNT: IntCounter = register_int_counter!(
        prefix_name(NAMESPACE, "write_error_count").as_str(),
        "count of errors writing to the cache"
    )
    .unwrap();
    pub static ref REQUEST_LATENCY_MS: HistogramVec = register_histogram_vec!(
        prefix_name(NAMESPACE, "latency_ms").as_str(),
        "latency of cache requests in milliseconds by data source",
        &["source"],
        vec!(10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0)
    )
    .unwrap();
    pub static ref REQUEST_THROUGHPUT: HistogramVec = register_histogram_vec!(
        prefix_name(NAMESPACE, "throughput_mbps").as_str(),
        "throughput of cache requests in Mbps by data source",
        &["source"],
        vec!(10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0)
    )
    .unwrap();
}

pub const NAME_DISK_CACHE: &str = "disk";

// BlockCache metrics (e.g. DiskCache)
lazy_static! {
    pub static ref BLOCKS_STORED: IntGaugeVec = register_int_gauge_vec!(
        prefix_name(NAMESPACE, "blocks_stored").as_str(),
        "number of blocks stored in the indicated cache",
        &["name"]
    )
    .unwrap();
    pub static ref BYTES_STORED: IntGaugeVec = register_int_gauge_vec!(
        prefix_name(NAMESPACE, "bytes_stored").as_str(),
        "number of bytes stored in the indicated cache",
        &["name"]
    )
    .unwrap();
    // Note: eviction age is specific to disk since we expect the histogram
    // buckets to be specific to the cache type (e.g. if we introduce a memcache,
    // it should have lower expected eviction ages).
    pub static ref DISK_EVICTION_AGE: Histogram = register_histogram!(
        prefix_name(NAMESPACE, "disk_eviction_age").as_str(),
        "age of entries evicted by the disk cache in seconds",
        vec!(60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 43200.0, 86400.0)
    )
    .unwrap();
}

pub const STATUS_EXPIRED: &str = "expired";
pub const STATUS_MISS: &str = "miss";
pub const STATUS_HIT: &str = "hit";

// Lru metrics
lazy_static! {
    // status values: [expired, miss, hit]
    pub static ref LRU_REQUESTS: IntCounterVec = register_int_counter_vec!(
        prefix_name(NAMESPACE, "lru_request_count").as_str(),
        "count of requests to an lru cache broken down by cache name and status",
        &["name", "status"]
    )
    .unwrap();
}

fn prefix_name(namespace: &str, name: &str) -> String {
    let service = SERVICE
        .lock()
        .expect("Couldn't get service name for cache metrics");
    format!("{service}_{namespace}_{name}")
}

/// Sets the name for the "service" field of cache metrics. Needs to be set as
/// part of application startup before cache metrics are initialized.
pub fn set_metrics_service_name(service_name: String) {
    let mut contents = SERVICE
        .lock()
        .expect("FATAL: couldn't lock cache metrics service name");
    *contents = service_name;
}
