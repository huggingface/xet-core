This update replaces `chrono` and `humantime` with `jiff` in xet-runtime, xet-client, xet-data and the simulation tools. RustSec marks both crates unmaintained (RUSTSEC-2026-0543, RUSTSEC-2026-0542), so every downstream `cargo audit` had to ignore them.

What changed
- No public type changes: chrono types only appeared in private code (`logging::init`, the telemetry envelope, the file cleaner). Log file names keep the same format and local-time offset.
- New public function `xet_runtime::utils::configuration_utils::parse_duration(&str) -> Result<std::time::Duration, String>`. Duration config values (`HF_XET_*` environment variables, `ParsableConfigValue for Duration`, Python `XetConfig` strings) and the simulation server's duration ranges now go through it.
- Accepted duration strings are the same as before for `ns`, `us`, `ms`, `s`, `m`, `h`, `d`, `w`, `month`, `y` and their long names, with or without spaces, fractions such as `1.5s`, and a bare `0`. Months and years keep humantime's fixed lengths (30.44 and 365.25 days). ISO 8601 durations (`PT5S`) are also accepted now. humantime's capital `M` shorthand for months is no longer accepted; use `month`.
- Telemetry `time` is still RFC 3339 UTC with millisecond precision (`2026-01-02T03:04:05.678Z`). The `start_ts` and `end_processing_ts` fields of the `client_telemetry` clean event are RFC 3339 with a `Z` suffix instead of `+00:00`.
- On wasm, jiff is built without time zone support, so no time zone database is embedded; the current time comes from the JS host (`js` feature).

Migration
- Nothing is required for Rust callers.
- Anyone setting a duration with humantime's `M` month shorthand (for example `HF_XET_LOG_DIR_MAX_RETENTION_AGE=1M`) should write `1month`.
- Code that parsed durations with `humantime::parse_duration` to match xet config semantics can call `parse_duration` instead.
