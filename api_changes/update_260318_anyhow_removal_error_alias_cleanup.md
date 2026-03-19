# API Update: Anyhow Removal and Legacy Error Alias Cleanup (2026-03-18)

## Overview

This change removes direct `anyhow` usage from core crates and finalizes migration to canonical
package-level error types.

Main impact:
- public legacy error alias modules were removed;
- several public error enum variants changed payload types;
- downstream imports should now use canonical error paths directly.

This is an API-breaking cleanup for callers still importing old alias paths.

## Canonical Error Types (now required)

Use these types directly:
- `xet_client::ClientError`
- `xet_core_structures::FormatError`
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

### `xet_core_structures::FormatError`
- `Internal(anyhow::Error)` -> `InternalError(String)`
- `Format(anyhow::Error)` -> `FormatError(String)`

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

For constructors that previously accepted `anyhow::Error`, construct string-backed variants
instead (`InternalError`, `CredentialHelper`, `FormatError`).

## Behavior Notes

No intended runtime behavior change in retry, session, or reconstruction logic; this update is
primarily an error-surface and API cleanup.
