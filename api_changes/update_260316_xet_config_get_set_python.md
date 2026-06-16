# API Update: `XetConfig` dotted-path get/set and Python bindings (2026-03-16)

## Overview

`xet_runtime::config::XetConfig` now exposes dotted-path configuration accessors,
plus optional Python bindings behind a feature flag. The config group list is
also centralized to reduce drift between group declarations and dispatch logic.

## Rust API additions

### New public methods on `xet_runtime::config::XetConfig`

- `with_config(path: &str, value: impl ToString) -> Result<Self, ConfigError>`
- `get(path: &str) -> Result<String, ConfigError>`
- `all_keys(&self) -> Vec<String>`

`path` must be in `group.field` format (for example:
`"data.max_concurrent_file_ingestion"`).

### New public error type

- `xet_runtime::config::ConfigError`
  - `UnknownField(String)`
  - `UnknownGroup(String)`
  - `InvalidPath(String)`
  - `ParseError { field: String, value: String }`

This is returned by `with_config` / `get` when dispatch or parsing fails.

## Feature and module changes

- New optional feature in `xet-runtime`: `python`
  - enables `xet_runtime::config::python` conversion helpers
  - enables Python-facing config wrappers in `xet_runtime::config::xet_config::py_xet_config`
- `xet_pkg` now exposes a matching `python` feature forwarding to `xet-runtime/python`.
- Workspace dependency list now includes `pyo3` (used as optional in `xet-runtime`).

## Public re-exports

When `python` feature is enabled:

- `xet_runtime::config::PyXetConfig` is re-exported.

## Config serialization/parsing behavior updates

`ParsableConfigValue` now includes:

- `to_config_string(&self) -> String`

Used by the new `get` API to return string values that round-trip through parsing.

Notable behavior:

- `Option<T>`:
  - empty/whitespace input parses as `Some(None)` (used by `with_config`)
  - `None` serializes as empty string
- `bool` string output from `get` is normalized to `"true"` / `"false"`
- `Duration` string output is normalized to unit-suffixed format (`s`, `ms`, `us`, `ns`)
- `TemplatedPathBuf` now exposes `template_string()` and `get` returns the original template string

## Python-facing API (feature-gated)

`PyXetConfig` (when enabled) supports:

- constructor: `XetConfig()`
- `with_config("group.field", value)`
- `with_config({"group.field": value, ...})`
- `get("group.field")`
- `obj["group.field"]`
- `keys()`, `items()`, `__len__`, iteration

Values are converted as native Python types via per-type conversion traits.

## Group registration / dispatch

A new exported macro provides a single source of truth for standard config groups:

- `all_config_groups!(callback_macro)`

This now drives:

- group module declarations
- `XetConfig` fields
- `with_env_overrides` dispatch
- `with_config` / `get` dispatch
- key enumeration

`system_monitor` remains cfg-gated and is appended separately for non-wasm targets.

## Downstream update guidance

If downstream code currently mutates config fields directly, no migration is required.
Use the new dotted-path API when dynamic/config-driven updates are needed.

If downstream Rust code matches config errors, match against `ConfigError` variants.

If Python bindings are consumed downstream, enable the `python` feature in
`xet-runtime` (or `xet_pkg`).
