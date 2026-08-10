use std::time::Duration;

crate::config_group!({
    /// Whether the client reports transfer performance telemetry to the CAS server.
    ///
    /// When enabled, each upload or download transfer sends a single summary document to
    /// `POST /v1/telemetry` when it finishes, plus periodic heartbeat documents for transfers
    /// that run longer than `heartbeat_after`. Sends are best-effort: they are never retried,
    /// never block data movement, and failures are only logged at DEBUG.
    ///
    /// The payload carries no file names, paths, hashes, repository ids, or user ids.
    ///
    /// The default value is true.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_ENABLED` to set this value.
    ref enabled: bool = true;

    /// How long a transfer must run before it starts emitting heartbeat documents.
    ///
    /// Short transfers - the common case - only ever emit their terminal summary. Heartbeats
    /// exist so a long transfer that hangs or is killed still reports something.
    ///
    /// Set to zero to disable heartbeats entirely.
    ///
    /// The default value is 5 minutes.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_HEARTBEAT_AFTER` to set this value.
    ref heartbeat_after: Duration = Duration::from_secs(300);

    /// The interval between heartbeat documents once `heartbeat_after` has elapsed.
    ///
    /// The default value is 5 minutes.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_HEARTBEAT_INTERVAL` to set this value.
    ref heartbeat_interval: Duration = Duration::from_secs(300);

    /// Whole-request budget for a single telemetry POST, including connection setup.
    ///
    /// The default value is 5 seconds.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_REQUEST_TIMEOUT` to set this value.
    ref request_timeout: Duration = Duration::from_secs(5);

    /// How long a transfer waits for its terminal telemetry document to be delivered.
    ///
    /// This runs after all transfer work has completed, so it delays no data movement, but it
    /// does delay the return of `finalize()`. A short bounded wait is used because a fully
    /// detached final send is usually lost: host processes frequently exit within milliseconds
    /// of the transfer returning.
    ///
    /// Set to zero to make the terminal send fully detached.
    ///
    /// The default value is 2 seconds.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_FINAL_FLUSH_TIMEOUT` to set this value.
    ref final_flush_timeout: Duration = Duration::from_secs(2);

    /// Maximum number of telemetry requests allowed in flight at once, across the whole process.
    ///
    /// Documents submitted beyond this cap are dropped rather than queued; this is the
    /// backpressure mechanism that keeps a degraded telemetry endpoint from accumulating tasks.
    ///
    /// The cap is process-wide rather than per-transfer, so it bounds a wide fan-out - a snapshot
    /// download becomes many concurrent per-file transfers, and a per-transfer cap would multiply
    /// by that count instead of limiting it.
    ///
    /// The default value is 32. It is sized as a process-wide number: a transfer emits one terminal
    /// document, and heartbeats only start after `heartbeat_after`, so the realistic burst is a set
    /// of concurrent transfers finalizing together. A much smaller ceiling would shed most of a wide
    /// snapshot's terminal documents, which are the ones worth keeping.
    ///
    /// Use the environment variable `HF_XET_TELEMETRY_MAX_IN_FLIGHT` to set this value.
    ref max_in_flight: usize = 32;
});

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use serial_test::serial;

    use crate::config::XetConfig;
    use crate::utils::EnvVarGuard;

    const XET_ENABLED: &str = "HF_XET_TELEMETRY_ENABLED";

    /// Clears every variable that participates in the gating decision, so a value exported in the
    /// developer's shell cannot make these tests pass or fail spuriously.
    fn clear_all() -> Vec<EnvVarGuard> {
        [XET_ENABLED].into_iter().map(EnvVarGuard::unset).collect()
    }

    fn telemetry_enabled() -> bool {
        XetConfig::default().with_env_overrides().telemetry.enabled
    }

    #[test]
    #[serial(env)]
    fn test_enabled_by_default() {
        let _guards = clear_all();
        assert!(telemetry_enabled());
    }

    #[test]
    #[serial(env)]
    fn test_disabled_by_own_env_var() {
        let _guards = clear_all();
        let _g = EnvVarGuard::set(XET_ENABLED, "0");
        assert!(!telemetry_enabled());
    }

    #[test]
    #[serial(env)]
    fn test_durations_and_cap_have_expected_defaults() {
        let _guards = clear_all();
        let t = XetConfig::default().with_env_overrides().telemetry;
        assert_eq!(t.heartbeat_after.as_secs(), 300);
        assert_eq!(t.heartbeat_interval.as_secs(), 300);
        assert_eq!(t.request_timeout.as_secs(), 5);
        assert_eq!(t.final_flush_timeout.as_secs(), 2);
        // Process-wide, not per-transfer - see `max_in_flight`'s docs for why it is sized this way.
        assert_eq!(t.max_in_flight, 32);
    }

    #[test]
    #[serial(env)]
    fn test_durations_parse_from_env() {
        let _guards = clear_all();
        let _after = EnvVarGuard::set("HF_XET_TELEMETRY_HEARTBEAT_AFTER", "90s");
        let _flush = EnvVarGuard::set("HF_XET_TELEMETRY_FINAL_FLUSH_TIMEOUT", "0s");
        let t = XetConfig::default().with_env_overrides().telemetry;
        assert_eq!(t.heartbeat_after.as_secs(), 90);
        assert_eq!(t.final_flush_timeout.as_secs(), 0);
    }
}
