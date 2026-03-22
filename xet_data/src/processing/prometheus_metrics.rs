use std::sync::LazyLock;

use prometheus::{IntCounter, register_int_counter};

// Some of the common tracking things
pub static FILTER_CAS_BYTES_PRODUCED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("filter_process_xorb_bytes_produced", "Number of CAS bytes produced during cleaning").unwrap()
});
pub static FILTER_BYTES_CLEANED: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("filter_process_bytes_cleaned", "Number of bytes cleaned").unwrap());
pub static FILTER_BYTES_SMUDGED: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("filter_process_bytes_smudged", "Number of bytes smudged").unwrap());
