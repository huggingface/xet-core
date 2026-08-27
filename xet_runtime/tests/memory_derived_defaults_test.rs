use serial_test::serial;
use xet_runtime::config::XetConfig;
use xet_runtime::core::{XetCommon, XetContext};
use xet_runtime::utils::system_memory::{default_download_buffer_sizes, high_performance_download_buffer_sizes};
use xet_runtime::utils::{ByteSize, EnvVarGuard};

/// Clears the buffer-related env vars for the guards' lifetime, so tests see default
/// behavior regardless of what the developer has exported.
#[must_use]
fn clear_buffer_env_vars() -> [EnvVarGuard; 4] {
    [
        "HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE",
        "HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_PERFILE_SIZE",
        "HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT",
        "HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS",
    ]
    .map(EnvVarGuard::unset)
}

#[test]
#[serial(config_env)]
fn test_download_buffer_defaults_are_memory_derived() {
    let _env_guards = clear_buffer_env_vars();
    let expected = default_download_buffer_sizes();
    let config = XetConfig::default();
    assert_eq!(config.reconstruction.download_buffer_size, expected.size);
    assert_eq!(config.reconstruction.download_buffer_perfile_size, expected.perfile);
    assert_eq!(config.reconstruction.download_buffer_limit, expected.limit);
}

#[test]
#[serial(config_env)]
fn test_normalize_raises_limit_to_size() {
    let _env_guards = clear_buffer_env_vars();
    let mut group = XetConfig::default().reconstruction.clone();
    group.download_buffer_size = ByteSize::from("16gb");
    group.download_buffer_limit = ByteSize::from("8gb");
    group.normalize();
    assert_eq!(group.download_buffer_limit, ByteSize::from("16gb"));

    // A coherent configuration is left untouched.
    let mut coherent = XetConfig::default().reconstruction.clone();
    coherent.download_buffer_size = ByteSize::from("1gb");
    coherent.download_buffer_limit = ByteSize::from("8gb");
    coherent.normalize();
    assert_eq!(coherent.download_buffer_limit, ByteSize::from("8gb"));
}

#[test]
#[serial(config_env)]
fn test_high_performance_values_are_memory_aware() {
    let _env_guards = clear_buffer_env_vars();
    let config = XetConfig::default().with_high_performance();
    let hp = high_performance_download_buffer_sizes();
    assert_eq!(config.reconstruction.download_buffer_size, hp.size);
    assert_eq!(config.reconstruction.download_buffer_perfile_size, hp.perfile);
    assert_eq!(config.reconstruction.download_buffer_limit, hp.limit);

    // The values actually applied on this machine describe one coherent allocation.
    let size = config.reconstruction.download_buffer_size.as_u64();
    let perfile = config.reconstruction.download_buffer_perfile_size.as_u64();
    let limit = config.reconstruction.download_buffer_limit.as_u64();
    let concurrent_files = config.data.max_concurrent_file_downloads as u64;
    assert!(size + concurrent_files * perfile <= limit);
}

#[test]
#[serial(config_env)]
fn test_env_override_beats_high_performance() {
    let _env_guards = clear_buffer_env_vars();
    let _guard = EnvVarGuard::set("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "123mb");

    // Mirrors the XetConfig::new() ordering when the HP flag is set: defaults ->
    // high performance -> env overrides. (The HP flag itself is latched in a
    // LazyLock at first read, so it cannot be toggled via env in-process.)
    let config = XetConfig::default().with_high_performance().with_env_overrides();

    // The explicitly set env var beats the high-performance preset...
    assert_eq!(config.reconstruction.download_buffer_size, ByteSize::from("123mb"));

    // ...while fields with no env override keep their high-performance values
    // instead of being reset to standard defaults.
    let hp = high_performance_download_buffer_sizes();
    assert_eq!(config.reconstruction.download_buffer_perfile_size, hp.perfile);
    assert_eq!(config.reconstruction.download_buffer_limit, hp.limit);
    assert_eq!(config.data.max_concurrent_file_ingestion, 100);
}

#[test]
#[serial(config_env)]
fn test_incoherent_buffer_config_does_not_panic_common() {
    let _env_guards = clear_buffer_env_vars();
    let mut config = XetConfig::default();
    config.reconstruction.download_buffer_size = ByteSize::from("16gb");
    config.reconstruction.download_buffer_limit = ByteSize::from("8gb");

    // Constructing shared state from an incoherent config must not panic; the
    // semaphore gets at least the requested buffer size.
    let common = XetCommon::new(&config);
    assert!(common.reconstruction_download_buffer.total_permits() >= 16_000_000_000);
}

#[test]
#[serial(config_env)]
fn test_context_normalizes_reconstruction_config() {
    let _env_guards = clear_buffer_env_vars();
    // Raising only the size via env (a limit below it) previously panicked at
    // context construction; now the limit is normalized up to match.
    let _guard = EnvVarGuard::set("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "100gb");
    let ctx = XetContext::default().unwrap();
    assert_eq!(ctx.config.reconstruction.download_buffer_size, ByteSize::from("100gb"));
    assert!(ctx.config.reconstruction.download_buffer_limit >= ctx.config.reconstruction.download_buffer_size);
}

#[test]
#[serial(config_env)]
fn test_env_override_beats_derived_default() {
    let _env_guards = clear_buffer_env_vars();
    let _guard = EnvVarGuard::set("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE", "123mb");
    let config = XetConfig::new();
    assert_eq!(config.reconstruction.download_buffer_size, ByteSize::from("123mb"));

    // Knobs without an env override keep their derived defaults.
    let expected = default_download_buffer_sizes();
    assert_eq!(config.reconstruction.download_buffer_perfile_size, expected.perfile);
    assert_eq!(config.reconstruction.download_buffer_limit, expected.limit);
}

#[test]
#[serial(config_env)]
fn test_kill_switch_restores_static_defaults_in_config() {
    let _env_guards = clear_buffer_env_vars();
    let _guard = EnvVarGuard::set("HF_XET_MEMORY_DERIVED_DOWNLOAD_BUFFERS", "0");
    let config = XetConfig::new();
    assert_eq!(config.reconstruction.download_buffer_size, ByteSize::from("2gb"));
    assert_eq!(config.reconstruction.download_buffer_perfile_size, ByteSize::from("512mb"));
    assert_eq!(config.reconstruction.download_buffer_limit, ByteSize::from("8gb"));
}
