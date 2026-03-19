# API Update: Anyhow Removal and Error Alias Cleanup (2026-03-18)

## Overview

This change removes direct `anyhow` usage from core crates and finalizes migration to canonical
package-level error types.

Main impact:
- public legacy error alias modules were removed;
- several public error enum variants changed payload types;
- `SessionError` alias usage was removed in favor of `xet::XetError`;
- Python exception mapping was tightened for `hf_xet`;
- downstream imports should now use canonical error paths directly.

This is an API-breaking cleanup for callers still importing old alias paths.

## Canonical Error Types (now required)

Use these types directly:
- `xet_client::ClientError`
- `xet_core_structures::CoreError`
- `xet_data::DataError`
- `xet::XetError`
- `xet_runtime::RuntimeError`

## Removed Legacy Alias Modules

The following compatibility modules were removed:
- `xet_client::cas_client::error` (previous `CasClientError` alias)
- `xet_client::cas_types::error` (previous `CasTypesError` alias)
- `xet_client::hub_client::errors` (previous `HubClientError` alias)
- `xet_core_structures::metadata_shard::error` (previous `MDBShardError` alias)
- `xet_core_structures::xorb_object::error` (previous `XorbObjectError` alias)
- `xet_data::processing::errors` (previous `DataProcessingError` alias)
- `xet::xet_session::errors` (previous `SessionError` alias)
- `xet_runtime::core::errors` (previous `MultithreadedRuntimeError` alias)

## Breaking Type/Variant Changes

### `xet_client::ClientError`
- `InternalError(anyhow::Error)` -> `InternalError(String)`
- `CredentialHelper(anyhow::Error)` -> `CredentialHelper(String)`

### `xet_core_structures::CoreError` (renamed from `FormatError`)
- `Internal(anyhow::Error)` -> `InternalError(String)`
- `Format(anyhow::Error)` -> `MalformedData(String)` (or a more specific `CoreError` variant)

### `xet::xet_session` / `xet::XetError`
- `xet::xet_session::SessionError` alias was removed.
- Public session APIs now return `Result<_, xet::XetError>`.
- `ClientError::PresignedUrlExpirationError` now maps to `XetError::Authentication`.
- `XetError::Timeout(String)` is used for timeout-class network failures.

Code matching old variant names or payload types must be updated.

## Trait Signature Change

`xet_client::hub_client::CredentialHelper` now uses:

```rust
async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder, xet_client::ClientError>;
```

## Migration Guide

Replace old imports and aliases directly:

```rust
// old
use xet_client::cas_client::CasClientError;
use xet_data::processing::errors::DataProcessingError;
use xet::xet_session::SessionError;
use xet_runtime::core::errors::MultithreadedRuntimeError;

// new
use xet_client::ClientError;
use xet_data::DataError;
use xet::XetError;
use xet_runtime::RuntimeError;
```

## Python (`hf_xet`) behavior

`From<XetError> for PyErr` now maps:
- `Authentication` -> `hf_xet.XetAuthenticationError` (inherits `PermissionError`)
- `NotFound` -> `hf_xet.XetObjectNotFoundError` (inherits `FileNotFoundError`)
- `Network` -> `ConnectionError`
- `Timeout` -> `TimeoutError`
- `Cancelled` -> `RuntimeError`

For constructors that previously accepted `anyhow::Error`, construct string-backed variants
instead (`InternalError`, `CredentialHelper`, `MalformedData`, and related `CoreError` variants).

## Behavior Notes

No intended runtime behavior change in retry, session, or reconstruction logic; this update is
primarily an error-surface and API cleanup.
