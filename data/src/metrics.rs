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
}
