use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};

// Some of the common tracking things
lazy_static! {
    pub static ref FILTER_CAS_BYTES_PRODUCED: IntCounter =
        register_int_counter!("filter_process_cas_bytes_produced", "Number of CAS bytes produced during cleaning")
            .unwrap();
    pub static ref FILTER_BYTES_CLEANED: IntCounter =
        register_int_counter!("filter_process_bytes_cleaned", "Number of bytes cleaned").unwrap();
    pub static ref FILTER_BYTES_SMUDGED: IntCounter =
        register_int_counter!("filter_process_bytes_smudged", "Number of bytes smudged").unwrap();
    pub static ref RUNTIME_CHUNKING: IntCounter =
        register_int_counter!("runtime_chunking", "Runtime of chunking in ns").unwrap();
    pub static ref RUNTIME_HASHING: IntCounter =
        register_int_counter!("runtime_hashing", "Runtime of hashing in ns").unwrap();
    pub static ref RUNTIME_SHA256: IntCounter =
        register_int_counter!("runtime_sha256", "Runtime of computing SHA-256 in ns").unwrap();
    pub static ref RUNTIME_DEDUP_QUERY: IntCounter =
        register_int_counter!("runtime_dedup_query", "Runtime of global dedup query in ns").unwrap();
    pub static ref RUNTIME_XORB_UPLOAD: IntCounter =
        register_int_counter!("runtime_xorb_upload", "Runtime of uploading xorbs in ns").unwrap();
}
