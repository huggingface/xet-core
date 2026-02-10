#![allow(non_snake_case)]

use serial_test::serial;
use utils::EnvVarGuard;
use xet_config::XetConfig;

/// Integration test to verify that environment variable aliases work correctly.
/// This test ensures backward compatibility with old environment variable names.
#[test]
#[serial(config_env)]
fn test_environment_variable_aliases() {
    // Client aliases
    {
        let _guard = EnvVarGuard::set("HF_XET_UPLOAD_REPORTING_BLOCK_SIZE", "1024000");
        assert_eq!(XetConfig::new().client.upload_reporting_block_size, 1024000);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_FIXED_UPLOAD_CONCURRENCY", "16");
        let config = XetConfig::new();
        assert_eq!(config.client.ac_initial_upload_concurrency, 16);
        assert_eq!(config.client.ac_min_upload_concurrency, 16);
        assert_eq!(config.client.ac_max_upload_concurrency, 16);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_FIXED_DOWNLOAD_CONCURRENCY", "32");
        let config = XetConfig::new();
        assert_eq!(config.client.ac_initial_download_concurrency, 32);
        assert_eq!(config.client.ac_min_download_concurrency, 32);
        assert_eq!(config.client.ac_max_download_concurrency, 32);
    }

    // Data aliases
    {
        let _guard = EnvVarGuard::set("HF_XET_MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES", "512");
        assert_eq!(XetConfig::new().data.min_spacing_between_global_dedup_queries, 512);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_LOCAL_CAS_SCHEME", "file://");
        assert_eq!(XetConfig::new().data.local_cas_scheme, "file://");
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_MAX_CONCURRENT_FILE_INGESTION", "25");
        assert_eq!(XetConfig::new().data.max_concurrent_file_ingestion, 25);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_INGESTION_BLOCK_SIZE", "16mb");
        assert_eq!(*XetConfig::new().data.ingestion_block_size, 16_000_000);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_PROGRESS_UPDATE_INTERVAL", "500ms");
        assert_eq!(XetConfig::new().data.progress_update_interval.as_millis(), 500);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_PROGRESS_UPDATE_SPEED_SAMPLING_WINDOW", "20sec");
        assert_eq!(XetConfig::new().data.progress_update_speed_sampling_window.as_secs(), 20);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_SESSION_XORB_METADATA_FLUSH_INTERVAL", "30sec");
        assert_eq!(XetConfig::new().data.session_xorb_metadata_flush_interval.as_secs(), 30);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_SESSION_XORB_METADATA_FLUSH_MAX_COUNT", "128");
        assert_eq!(XetConfig::new().data.session_xorb_metadata_flush_max_count, 128);
    }

    // MDB shard aliases
    {
        let _guard = EnvVarGuard::set("HF_XET_SHARD_CACHE_SIZE_LIMIT", "32gb");
        assert_eq!(*XetConfig::new().mdb_shard.cache_size_limit, 32_000_000_000);
    }
    {
        let _guard = EnvVarGuard::set("HF_XET_CHUNK_INDEX_TABLE_MAX_SIZE", "128000000");
        assert_eq!(XetConfig::new().mdb_shard.chunk_index_table_max_size, 128_000_000);
    }
}

/// Test that primary environment variable takes precedence over alias when both are set.
#[test]
#[serial(config_env)]
fn test_primary_env_var_precedence_over_alias() {
    {
        let _guard1 = EnvVarGuard::set("HF_XET_MDB_SHARD_CACHE_SIZE_LIMIT", "8gb");
        let _guard2 = EnvVarGuard::set("HF_XET_SHARD_CACHE_SIZE_LIMIT", "32gb");
        assert_eq!(*XetConfig::new().mdb_shard.cache_size_limit, 8_000_000_000);
    }
}

/// Test that default values are used when neither primary nor alias is set.
#[test]
#[serial(config_env)]
fn test_default_values_when_no_env_vars_set() {
    let config = XetConfig::new();
    assert_eq!(config.data.max_concurrent_file_ingestion, 8);
    assert_eq!(config.mdb_shard.chunk_index_table_max_size, 64 * 1024 * 1024);
}
