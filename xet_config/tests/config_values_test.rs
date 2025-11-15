#![allow(non_snake_case)]

use std::time::Duration;

use config::config_group;
use serial_test::serial;
use utils::{ByteSize, EnvVarGuard};

mod example {
    use super::*;

    config_group!({
        /// Test integer value
        ref TEST_INT: usize = 42;

        /// Test string value
        ref TEST_STRING: String = "default".to_string();

        /// Test boolean value
        ref TEST_BOOL: bool = false;

        /// Test duration value
        ref TEST_DURATION: Duration = Duration::from_secs(60);

        /// Test byte size value
        ref TEST_BYTE_SIZE: ByteSize = ByteSize::from("1mb");

        /// Test value
        ref TEST_RELEASE_FIXED: u64 = 100;

        /// Test value (high performance mode is now handled at XetConfig level)
        ref TEST_HIGH_PERF: usize = 10;

        /// Test optional value
        ref TEST_OPTIONAL: Option<String> = None;
    });
}

#[test]
#[serial(config_env)]
fn test_basic_configuration() {
    // Ensure env vars are not set to test default values
    // Use unsafe remove_var to ensure clean state (this is the pattern used in the codebase)
    unsafe {
        std::env::remove_var("HF_XET_EXAMPLE_TEST_INT");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_STRING");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_BOOL");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_DURATION");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_BYTE_SIZE");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_RELEASE_FIXED");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_OPTIONAL");
        std::env::remove_var("HF_XET_EXAMPLE_TEST_HIGH_PERF");
    }

    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    assert_eq!(config.TEST_INT, 42);
    assert_eq!(config.TEST_STRING, "default");
    assert_eq!(config.TEST_BOOL, false);
    assert_eq!(config.TEST_DURATION, Duration::from_secs(60));
    assert_eq!(config.TEST_BYTE_SIZE.as_u64(), 1_000_000);
    assert_eq!(config.TEST_RELEASE_FIXED, 100);
    assert_eq!(config.TEST_HIGH_PERF, 10);
    assert_eq!(config.TEST_OPTIONAL, None);
}

#[test]
#[serial(config_env)]
fn test_environment_override() {
    let _guard1 = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_INT", "100");
    let _guard2 = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_STRING", "override");
    let _guard3 = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_BOOL", "true");
    let _guard4 = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_DURATION", "2m");
    let _guard5 = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_BYTE_SIZE", "2mb");

    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();

    assert_eq!(config.TEST_INT, 100);
    assert_eq!(config.TEST_STRING, "override");
    assert_eq!(config.TEST_BOOL, true);
    assert_eq!(config.TEST_DURATION, Duration::from_secs(120));
    assert_eq!(config.TEST_BYTE_SIZE.as_u64(), 2_000_000);
}

#[test]
#[serial(config_env)]
fn test_optional_value() {
    // First test with no env var (should be None)
    // Ensure it's not set by removing it first
    unsafe {
        std::env::remove_var("HF_XET_EXAMPLE_TEST_OPTIONAL");
    }
    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    assert_eq!(config.TEST_OPTIONAL, None);

    // Then test with env var set
    let _guard = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_OPTIONAL", "some_value");
    let mut config2 = example::ConfigValueGroup::new();
    config2.apply_env_overrides();
    assert_eq!(config2.TEST_OPTIONAL, Some("some_value".to_string()));
}

#[test]
#[serial(config_env)]
fn test_high_performance_mode() {
    // High performance mode is now handled at the XetConfig level via with_high_performance().
    // This test just verifies that the config group uses the standard value.
    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    assert_eq!(config.TEST_HIGH_PERF, 10);
}

#[test]
#[serial(config_env)]
fn test_release_fixed_in_release() {
    // All config group values are now env configurable
    let _guard = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_RELEASE_FIXED", "200");
    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    assert_eq!(config.TEST_RELEASE_FIXED, 200);
}

#[test]
#[serial(config_env)]
fn test_configuration_clone() {
    let mut config1 = example::ConfigValueGroup::new();
    config1.apply_env_overrides();
    let config2 = config1.clone();
    assert_eq!(config1.TEST_INT, config2.TEST_INT);
    assert_eq!(config1.TEST_STRING, config2.TEST_STRING);
}

#[test]
#[serial(config_env)]
fn test_configuration_debug() {
    // Clean up any existing env vars
    unsafe {
        std::env::remove_var("HF_XET_EXAMPLE_TEST_INT");
    }

    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("TEST_INT"));
    // The value might be overridden by env, so just check the field name exists
    assert!(debug_str.contains("TEST_INT"));
}

#[test]
#[serial(config_env)]
fn test_invalid_env_value_falls_back_to_default() {
    let _guard = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_INT", "not_a_number");
    let mut config = example::ConfigValueGroup::new();
    config.apply_env_overrides();
    // Should fall back to default
    assert_eq!(config.TEST_INT, 42);
}

#[test]
#[serial(config_env)]
fn test_multiple_instances() {
    // Clean up first
    unsafe {
        std::env::remove_var("HF_XET_EXAMPLE_TEST_INT");
    }

    let mut config1 = example::ConfigValueGroup::new();
    config1.apply_env_overrides();
    let initial_value = config1.TEST_INT;

    let _guard = EnvVarGuard::set("HF_XET_EXAMPLE_TEST_INT", "999");
    let mut config2 = example::ConfigValueGroup::new();
    config2.apply_env_overrides();
    assert_eq!(config2.TEST_INT, 999);

    // Guard will restore on drop, so after it's dropped, we should get the initial value
    drop(_guard);

    // Manually remove to ensure clean state
    unsafe {
        std::env::remove_var("HF_XET_EXAMPLE_TEST_INT");
    }

    let mut config3 = example::ConfigValueGroup::new();
    config3.apply_env_overrides();
    assert_eq!(config3.TEST_INT, initial_value);
}

#[test]
fn test_config_values_name() {
    // Verify that the CONFIG_VALUES_NAME constant is accessible
    assert_eq!(example::CONFIG_VALUES_NAME, "ConfigValueGroup");
}
