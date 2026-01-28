#![allow(non_snake_case)]

use serial_test::serial;
use utils::EnvVarGuard;
use xet_config::{XetConfig, config_group};

// Create test config modules
mod data_config {
    use super::*;
    config_group!({
        ref TEST_INT: usize = 42;
    });
}

mod shard_config {
    use super::*;
    config_group!({
        ref TEST_STRING: String = "default".to_string();
    });
}

#[test]
fn test_primary_config_struct() {
    // Test that XetConfig can be created with defaults
    let config = XetConfig::default();

    // Verify the struct has the correct fields (using actual XetConfig fields)
    assert_eq!(config.mdb_shard.target_size, 64 * 1024 * 1024);
}

#[test]
fn test_primary_config_clone() {
    // Test that the main XetConfig can be cloned
    let config1 = XetConfig::new();
    let config2 = config1.clone();

    assert_eq!(config1.mdb_shard.target_size, config2.mdb_shard.target_size);
}

#[test]
fn test_primary_config_debug() {
    let config = XetConfig::new();
    let debug_str = format!("{:?}", config);
    // Just verify it doesn't panic
    assert!(!debug_str.is_empty());
}

#[test]
fn test_config_values_type_alias() {
    // Verify that the ConfigValueGroup is accessible
    let mut _data_config = data_config::ConfigValueGroup::new();
    _data_config.apply_env_overrides();
    let mut _shard_config = shard_config::ConfigValueGroup::new();
    _shard_config.apply_env_overrides();

    // Verify they work correctly
    let mut data1 = data_config::ConfigValueGroup::new();
    data1.apply_env_overrides();
    assert_eq!(data1.TEST_INT, 42);
}

#[test]
#[serial(config_env)]
fn test_group_name_env_var_prefixing() {
    // Test that environment variables use the group name prefix
    // For group "data", default_cas_endpoint should be loaded from HF_XET_DATA_DEFAULT_CAS_ENDPOINT
    let _guard = EnvVarGuard::set("HF_XET_DATA_DEFAULT_CAS_ENDPOINT", "http://test:8080");
    let config = XetConfig::new();

    assert_eq!(config.data.default_cas_endpoint, "http://test:8080");
    assert_eq!(config.mdb_shard.target_size, 64 * 1024 * 1024); // Should still be default

    // Test mdb_shard group
    let _guard2 = EnvVarGuard::set("HF_XET_MDB_SHARD_TARGET_SIZE", "1000000");
    let config2 = XetConfig::new();

    assert_eq!(config2.data.default_cas_endpoint, "http://test:8080"); // Still from previous guard
    assert_eq!(config2.mdb_shard.target_size, 1000000);
}
