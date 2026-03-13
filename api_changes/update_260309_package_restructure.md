# API Update: Package Consolidation & Error Hierarchy (2026-03-09)

## Overview

Twenty standalone workspace crates have been consolidated into five packages plus two
top-level crates.  Every error type in the workspace has been merged into a
per-package canonical error enum, and a new top-level `XetError` provides
user-facing error categorisation.

Backward-compatible type aliases are provided in the original module locations,
so most downstream code that imports the *old* error type name from its *old*
module path will continue to compile—but all new code should use the canonical
names and paths listed below.

---

## 1. Workspace Crate Consolidation

### New workspace members

| New Package (dir) | Crate name (`Cargo.toml`) | Rust import name |
|---|---|---|
| `xet_runtime/` | `xet-runtime` | `xet_runtime` |
| `xet_core_structures/` | `xet-core-structures` | `xet_core_structures` |
| `xet_client/` | `xet-client` | `xet_client` |
| `xet_data/` | `xet-data` | `xet_data` |
| `xet_pkg/` | `hf-xet` | `xet` |
| `git_xet/` | `git_xet` | `git_xet` |
| `simulation/` | `simulation` | `simulation` |

### Old crate → new location mapping

| Old Crate | Old Import Name | New Package | New Module Path |
|---|---|---|---|
| `xet_runtime` | `xet_runtime` | `xet-runtime` | `xet_runtime::core` |
| `utils` | `utils` | `xet-runtime` | `xet_runtime::utils` |
| `xet_config` | `xet_config` | `xet-runtime` | `xet_runtime::config` |
| `xet_logging` | `xet_logging` | `xet-runtime` | `xet_runtime::logging` |
| `error_printer` | `error_printer` | `xet-runtime` | `xet_runtime::error_printer` |
| `file_utils` | `file_utils` | `xet-runtime` | `xet_runtime::file_utils` |
| `merklehash` | `merklehash` | `xet-core-structures` | `xet_core_structures::merklehash` |
| `mdb_shard` | `mdb_shard` | `xet-core-structures` | `xet_core_structures::metadata_shard` |
| `xorb_object` | `xorb_object` | `xet-core-structures` | `xet_core_structures::xorb_object` |
| `cas_client` | `cas_client` | `xet-client` | `xet_client::cas_client` |
| `hub_client` | `hub_client` | `xet-client` | `xet_client::hub_client` |
| `cas_types` | `cas_types` | `xet-client` | `xet_client::cas_types` |
| `chunk_cache` | `chunk_cache` | `xet-client` | `xet_client::chunk_cache` |
| `data` | `data` | `xet-data` | `xet_data` (root) |
| `deduplication` | `deduplication` | `xet-data` | `xet_data::deduplication` |
| `file_reconstruction` | `file_reconstruction` | `xet-data` | `xet_data::file_reconstruction` |
| `progress_tracking` | `progress_tracking` | `xet-data` | `xet_data::progress_tracking` |
| `xet_session` | `xet-session` | `xet` | `xet::xet_session` |

### Binding crates (unchanged structure, updated dependencies)

These crates are excluded from the workspace and built separately.  Their
internal imports have been updated but their public APIs are unchanged.

| Crate | Location |
|---|---|
| `hf_xet` | `hf_xet/` |
| `hf_xet_wasm` | `wasm/hf_xet_wasm/` |
| `hf_xet_thin_wasm` | `wasm/hf_xet_thin_wasm/` |

### `git_xet` location

The `git_xet` crate remains at `git_xet/` in the repository root.

---

## 2. Import Path Migration

When updating downstream code, replace import paths as follows.

### Runtime

```rust
// Old
use xet_runtime::XetRuntime;
use xet_runtime::errors::MultithreadedRuntimeError;
use xet_runtime::sync_primatives::spawn_os_thread;

// New
use xet_runtime::core::XetRuntime;
use xet_runtime::RuntimeError;   // re-exported at crate root
use xet_runtime::core::sync_primatives::spawn_os_thread;
```

### Merklehash

```rust
// Old
use merklehash::MerkleHash;
use merklehash::DataHashHexParseError;

// New
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::merklehash::DataHashHexParseError;
```

### MDB Shard (metadata shard)

```rust
// Old
use mdb_shard::error::MDBShardError;
use mdb_shard::shard_format::MDBShardInfo;
use mdb_shard::xorb_structs::XorbChunkSequenceHeader;

// New
use xet_core_structures::metadata_shard::error::MDBShardError;  // alias for FormatError
use xet_core_structures::metadata_shard::shard_format::MDBShardInfo;
use xet_core_structures::metadata_shard::xorb_structs::XorbChunkSequenceHeader;
```

### XORB Object

```rust
// Old
use xorb_object::error::XorbObjectError;
use xorb_object::XorbObject;

// New
use xet_core_structures::xorb_object::error::XorbObjectError;  // alias for FormatError
use xet_core_structures::xorb_object::XorbObject;
```

### CAS Client

```rust
// Old
use cas_client::CasClientError;
use cas_client::interface::Client;
use cas_client::auth::AuthError;

// New
use xet_client::cas_client::error::CasClientError;  // alias for ClientError
use xet_client::cas_client::interface::Client;
use xet_client::cas_client::auth::AuthError;
```

### Hub Client

```rust
// Old
use hub_client::HubClientError;

// New
use xet_client::hub_client::errors::HubClientError;  // alias for ClientError
```

### CAS Types

```rust
// Old
use cas_types::error::CasTypesError;
use cas_types::XorbReconstructionTerm;

// New
use xet_client::cas_types::error::CasTypesError;  // alias for ClientError
use xet_client::cas_types::XorbReconstructionTerm;
```

### Chunk Cache

```rust
// Old
use chunk_cache::error::ChunkCacheError;

// New
use xet_client::chunk_cache::error::ChunkCacheError;  // retained as distinct type
```

### Data Processing

```rust
// Old
use data::errors::DataProcessingError;

// New
use xet_data::processing::errors::DataProcessingError;  // alias for DataError
// or directly:
use xet_data::DataError;
```

### File Reconstruction

```rust
// Old
use file_reconstruction::FileReconstructionError;

// New
use xet_data::file_reconstruction::FileReconstructionError;  // retained as distinct type
```

### Session

```rust
// Old
use xet_session::errors::SessionError;

// New
use xet::xet_session::errors::SessionError;  // alias for XetError
// or directly:
use xet::XetError;
```

### Utils

```rust
// Old
use utils::errors::SingleflightError;
use utils::errors::KeyError;        // REMOVED — unused
use utils::errors::AuthError;       // moved

// New
use xet_runtime::utils::errors::SingleflightError;
// KeyError was removed entirely (was unused).
// AuthError lives in xet_client::cas_client::auth::AuthError (unchanged definition).
```

### Configuration

```rust
// Old
use xet_config::*;

// New
use xet_runtime::config::*;
```

### Logging

```rust
// Old
use xet_logging::*;

// New
use xet_runtime::logging::*;
```

### Error Printer

```rust
// Old
use error_printer::*;

// New
use xet_runtime::error_printer::*;
```

### File Utils

```rust
// Old
use file_utils::*;

// New
use xet_runtime::file_utils::*;
```

### Deduplication

```rust
// Old
use deduplication::*;

// New
use xet_data::deduplication::*;
```

### Progress Tracking

```rust
// Old
use progress_tracking::*;

// New
use xet_data::progress_tracking::*;
```

---

## 3. Error Type Consolidation

### 3.1 `RuntimeError` (crate: `xet-runtime`)

**Canonical path:** `xet_runtime::error::RuntimeError`  
**Re-exported at:** `xet_runtime::RuntimeError`

Replaces: `MultithreadedRuntimeError`

| Old Type / Variant | New Type / Variant |
|---|---|
| `MultithreadedRuntimeError::RuntimeInitializationError(io::Error)` | `RuntimeError::RuntimeInit(io::Error)` |
| `MultithreadedRuntimeError::TaskPanic(String)` | `RuntimeError::TaskPanic(String)` |
| `MultithreadedRuntimeError::TaskCanceled(String)` | `RuntimeError::TaskCanceled(String)` |
| `MultithreadedRuntimeError::Other(String)` | `RuntimeError::Other(String)` |

**Backward-compatibility alias:**
```rust
// In xet_runtime/src/core/errors.rs:
pub use crate::error::RuntimeError as MultithreadedRuntimeError;
```

**`From` implementations:**
- `From<tokio::task::JoinError>` — routes to `TaskPanic` / `TaskCanceled` / `Other`

**Not merged (retained as distinct types):**
- `SingleflightError<E>` — generic, used across crates (at `xet_runtime::utils::errors::SingleflightError`)
- `RwTaskLockError` — used as a generic bound (`E: From<RwTaskLockError>`) in `RwTaskLock<T, E>` (at `xet_runtime::utils::RwTaskLockError`)

**Removed:**
- `KeyError` — was unused

---

### 3.2 `FormatError` (crate: `xet-core-structures`)

**Canonical path:** `xet_core_structures::error::FormatError`  
**Re-exported at:** `xet_core_structures::FormatError`

Replaces: `MDBShardError`, `XorbObjectError`

| Old Type::Variant | New Variant | Category |
|---|---|---|
| `MDBShardError::IOError` | `FormatError::Io` | Common |
| `MDBShardError::InternalError` | `FormatError::Internal` | Common |
| `XorbObjectError::InternalIOError` | `FormatError::Io` | Common |
| `XorbObjectError::InternalError` | `FormatError::Internal` | Common |
| — | `FormatError::Other(String)` | Common |
| `MDBShardError::TruncatedHashCollisionError` | `FormatError::TruncatedHashCollision` | Shard |
| `MDBShardError::ShardVersionError` | `FormatError::ShardVersion` | Shard |
| `MDBShardError::BadFilename` | `FormatError::BadFilename` | Shard |
| `MDBShardError::ShardNotFound` | `FormatError::ShardNotFound` | Shard |
| `MDBShardError::FileNotFound` | `FormatError::FileNotFound` | Shard |
| `MDBShardError::QueryFailed` | `FormatError::QueryFailed` | Shard |
| `MDBShardError::SmudgeQueryPolicyError` | `FormatError::SmudgeQueryPolicy` | Shard |
| `MDBShardError::InvalidShard` | `FormatError::InvalidShard` | Shard |
| `XorbObjectError::InvalidRange` | `FormatError::InvalidRange` | XORB |
| `XorbObjectError::InvalidArguments` | `FormatError::InvalidArguments` | XORB |
| `XorbObjectError::FormatError` | `FormatError::Format` | XORB |
| `XorbObjectError::HashMismatch` | `FormatError::HashMismatch` | XORB |
| `XorbObjectError::CompressionError` | `FormatError::Compression` | XORB |
| `XorbObjectError::HashParsingError` | `FormatError::HashParsing` | XORB |
| `XorbObjectError::ChunkHeaderParseError` | `FormatError::ChunkHeaderParse` | XORB |
| — | `FormatError::Runtime(RuntimeError)` | Runtime |
| — | `FormatError::TaskRuntime(RwTaskLockError)` | Runtime |
| — | `FormatError::TaskJoin(JoinError)` | Runtime |

**Backward-compatibility aliases:**
```rust
// In xet_core_structures/src/metadata_shard/error.rs:
pub use crate::error::FormatError as MDBShardError;

// In xet_core_structures/src/xorb_object/error.rs:
pub use crate::error::{FormatError as XorbObjectError, Result, Validate};
```

**`From` implementations:**
- `From<std::io::Error>` → `Io`
- `From<lz4_flex::frame::Error>` → `Compression`
- `From<Infallible>` → `HashParsing`
- `From<xet_runtime::RuntimeError>` → `Runtime`
- `From<xet_runtime::utils::RwTaskLockError>` → `TaskRuntime`
- `From<tokio::task::JoinError>` → `TaskJoin`
- `From<DataHashHexParseError>` → `Other("Invalid hex input for DataHash")`
- `From<DataHashBytesParseError>` → `Other("Invalid bytes input for DataHash")`

**Helper methods:**
- `FormatError::other(impl ToString) -> Self`
- `FormatError::invalid_shard(impl ToString) -> Self`

**Trait:** `Validate<T>` is defined alongside `FormatError` and re-exported
from the `xorb_object::error` module.

---

### 3.3 `ClientError` (crate: `xet-client`)

**Canonical path:** `xet_client::error::ClientError`  
**Re-exported at:** `xet_client::ClientError`

Replaces: `CasClientError`, `HubClientError`, `CasTypesError`

| Old Type::Variant | New Variant |
|---|---|
| `CasClientError::XorbObjectError(XorbObjectError)` | `ClientError::FormatError(FormatError)` |
| `CasClientError::MDBShardError(MDBShardError)` | `ClientError::FormatError(FormatError)` |
| `CasClientError::ConfigurationError(String)` | `ClientError::ConfigurationError(String)` |
| `CasClientError::InvalidRange` | `ClientError::InvalidRange` |
| `CasClientError::InvalidArguments` | `ClientError::InvalidArguments` |
| `CasClientError::FileNotFound(MerkleHash)` | `ClientError::FileNotFound(MerkleHash)` |
| `CasClientError::IOError(io::Error)` | `ClientError::IOError(io::Error)` |
| `CasClientError::InvalidShardKey(String)` | `ClientError::InvalidShardKey(String)` |
| `CasClientError::InternalError(anyhow::Error)` | `ClientError::InternalError(anyhow::Error)` |
| `CasClientError::Other(String)` | `ClientError::Other(String)` |
| `CasClientError::ParseError(url::ParseError)` | `ClientError::ParseError(url::ParseError)` |
| `CasClientError::ReqwestMiddlewareError` | `ClientError::ReqwestMiddlewareError` |
| `CasClientError::ReqwestError(reqwest::Error, String)` | `ClientError::ReqwestError(reqwest::Error, String)` |
| `CasClientError::ShardDedupDBError(String)` | `ClientError::ShardDedupDBError(String)` |
| `CasClientError::XORBNotFound(MerkleHash)` | `ClientError::XORBNotFound(MerkleHash)` |
| `CasClientError::PresignedUrlExpirationError` | `ClientError::PresignedUrlExpirationError` |
| `CasClientError::Cloned(String)` | `ClientError::Cloned(String)` |
| `CasClientError::AuthError(AuthError)` | `ClientError::AuthError(AuthError)` |
| `HubClientError::CredentialHelper(anyhow::Error)` | `ClientError::CredentialHelper(anyhow::Error)` |
| `HubClientError::InvalidRepoType(String)` | `ClientError::InvalidRepoType(String)` |
| `CasTypesError::InvalidKey(String)` | `ClientError::InvalidKey(String)` |
| `ChunkCacheError::*` | `ClientError::CacheError(String)` (via `From`) |

**Backward-compatibility aliases:**
```rust
// In xet_client/src/cas_client/error.rs:
pub use crate::error::{ClientError as CasClientError, Result};

// In xet_client/src/hub_client/errors.rs:
pub use crate::error::ClientError as HubClientError;
// Also retains: HubClientError::credential_helper_error() helper method

// In xet_client/src/cas_types/error.rs:
pub use crate::error::ClientError as CasTypesError;
```

**`From` implementations:**
- `From<FormatError>` → `FormatError` (covers old `XorbObjectError` and `MDBShardError`)
- `From<reqwest::Error>` → `ReqwestError` (custom: strips query params from URL)
- `From<reqwest_middleware::Error>` → `ReqwestMiddlewareError`
- `From<url::ParseError>` → `ParseError`
- `From<AuthError>` → `AuthError`
- `From<anyhow::Error>` → `InternalError`
- `From<std::io::Error>` → `IOError`
- `From<AcquireError>` → `InternalError` (via `Self::internal()`)
- `From<SendError<T>>` → `InternalError` (via `Self::internal()`)
- `From<JoinError>` → `InternalError` (via `Self::internal()`)
- `From<TryFromIntError>` → `InternalError` (via `Self::internal()`)
- `From<PoisonError<T>>` → `InternalError` (via `Self::internal()`)
- `From<SingleflightError<ClientError>>` → unwraps `InternalError` variant, else `Other`
- `From<ChunkCacheError>` → `CacheError(e.to_string())`

**Helper methods:**
- `ClientError::internal<T: Debug>(value: T) -> Self`
- `ClientError::status(&self) -> Option<StatusCode>`

**Key change for explicit variant construction:**
```rust
// Old
CasClientError::XorbObjectError(e)
CasClientError::MDBShardError(e)

// New — both map through FormatError:
ClientError::FormatError(e)
// or use From:
ClientError::from(e)  // where e: FormatError
```

---

### 3.4 `DataError` (crate: `xet-data`)

**Canonical path:** `xet_data::error::DataError`  
**Re-exported at:** `xet_data::DataError`

Replaces: `DataProcessingError`

| Old Variant | New Variant |
|---|---|
| `DataProcessingError::MDBShardError(MDBShardError)` | `DataError::FormatError(FormatError)` |
| `DataProcessingError::CasClientError(CasClientError)` | `DataError::ClientError(ClientError)` |
| `DataProcessingError::XorbSerializationError(XorbObjectError)` | `DataError::FormatError(FormatError)` |
| `DataProcessingError::FileQueryPolicyError` | `DataError::FileQueryPolicyError` |
| `DataProcessingError::CASConfigError` | `DataError::CASConfigError` |
| `DataProcessingError::ShardConfigError` | `DataError::ShardConfigError` |
| `DataProcessingError::DedupConfigError` | `DataError::DedupConfigError` |
| `DataProcessingError::CleanTaskError` | `DataError::CleanTaskError` |
| `DataProcessingError::UploadTaskError` | `DataError::UploadTaskError` |
| `DataProcessingError::InternalError` | `DataError::InternalError` |
| `DataProcessingError::SyncError` | `DataError::SyncError` |
| `DataProcessingError::ChannelRecvError` | `DataError::ChannelRecvError` |
| `DataProcessingError::JoinError` | `DataError::JoinError` |
| `DataProcessingError::FileNotCleanedError` | `DataError::FileNotCleanedError` |
| `DataProcessingError::IOError` | `DataError::IOError` |
| `DataProcessingError::HashNotFound` | `DataError::HashNotFound` |
| `DataProcessingError::ParameterError` | `DataError::ParameterError` |
| `DataProcessingError::HashStringParsingFailure` | `DataError::HashStringParsingFailure` |
| `DataProcessingError::DeprecatedError` | `DataError::DeprecatedError` |
| `DataProcessingError::AuthError` | `DataError::AuthError` |
| `DataProcessingError::PermitAcquisitionError` | `DataError::PermitAcquisitionError` |
| `DataProcessingError::FileReconstructionError` | `DataError::FileReconstructionError` |
| — (new) | `DataError::RuntimeError(RuntimeError)` |

**Backward-compatibility alias:**
```rust
// In xet_data/src/processing/errors.rs:
pub use crate::error::DataError as DataProcessingError;
```

**`From` implementations:**
- `From<FormatError>` → `FormatError` (replaces old `From<MDBShardError>` and `From<XorbObjectError>`)
- `From<ClientError>` → `ClientError` (replaces old `From<CasClientError>`)
- `From<AuthError>` → `AuthError`
- `From<RuntimeError>` → `RuntimeError`
- `From<std::io::Error>` → `IOError`
- `From<FromUtf8Error>` → `FileNotCleanedError`
- `From<RecvError>` → `ChannelRecvError`
- `From<JoinError>` → `JoinError`
- `From<AcquireError>` → `PermitAcquisitionError`
- `From<DataHashHexParseError>` → `HashStringParsingFailure`
- `From<FileReconstructionError>` → `FileReconstructionError`
- `From<SingleflightError<DataError>>` → unwraps `InternalError`, else `InternalError(msg)`
- `From<ParutilsError<DataError>>` → routes `Join`/`Acquire`/`Task` variants

**Key change for explicit variant construction:**
```rust
// Old
DataProcessingError::CasClientError(e)    // e: CasClientError
DataProcessingError::MDBShardError(e)     // e: MDBShardError
DataProcessingError::XorbSerializationError(e) // e: XorbObjectError

// New
DataError::ClientError(e)    // e: ClientError (same type — CasClientError is now an alias)
DataError::FormatError(e)    // e: FormatError (same type — MDBShardError/XorbObjectError are aliases)
```

---

### 3.5 `XetError` (crate: `xet`)

**Canonical path:** `xet::error::XetError`  
**Re-exported at:** `xet::XetError`

Replaces: `SessionError`

This is the top-level public error type.  Variants are divided into
**session lifecycle** states and **user-facing categories**.

#### Session lifecycle variants

| Variant | When |
|---|---|
| `Aborted` | Session was aborted before the operation |
| `AlreadyCommitted` | `commit()` called more than once |
| `AlreadyFinished` | `finish()` called more than once |
| `InvalidTaskID(Ulid)` | Task ID does not match any queued file |

#### User-facing category variants

| Variant | Maps to (intended Python exception) | Description |
|---|---|---|
| `Authentication(String)` | `PermissionError` | Token refresh or credential failures |
| `Network(String)` | `ConnectionError` | DNS, timeouts, HTTP 5xx |
| `NotFound(String)` | `FileNotFoundError` | Requested resource does not exist |
| `DataIntegrity(String)` | `ValueError` | Hash mismatch, corrupt shard/xorb |
| `Configuration(String)` | `ValueError` | Bad config or arguments |
| `Io(String)` | `OSError` | Local filesystem I/O |
| `Cancelled(String)` | `KeyboardInterrupt` | SIGINT, semaphore closed |
| `Internal(String)` | `RuntimeError` | Panics, lock poison, bugs |

**Backward-compatibility alias:**
```rust
// In xet_pkg/src/xet_session/errors.rs:
pub use crate::error::XetError as SessionError;
```

#### `From` conversion routing

The `From` implementations inspect inner variants to choose the most specific
`XetError` category.  The full routing is documented below for downstream
agents that need to understand which low-level error maps to which user-facing
category.

**`From<RuntimeError>`:**
| `RuntimeError` variant | `XetError` category |
|---|---|
| `RuntimeInit(_)` | `Internal` |
| `TaskPanic(_)` | `Internal` |
| `TaskCanceled(_)` | `Cancelled` |
| `Other(_)` | `Internal` |

**`From<FormatError>`:**
| `FormatError` variant | `XetError` category |
|---|---|
| `Io(_)` | `Io` |
| `ShardNotFound(_)`, `FileNotFound(_)` | `NotFound` |
| `HashMismatch`, `TruncatedHashCollision(_)`, `InvalidShard(_)`, `ShardVersion(_)`, `ChunkHeaderParse`, `Format(_)`, `Compression(_)` | `DataIntegrity` |
| `InvalidRange`, `InvalidArguments`, `BadFilename(_)` | `Configuration` |
| `Runtime(re)` | routes through `RuntimeError` logic |
| `TaskRuntime(_)`, `TaskJoin(_)` | `Internal` |
| `SmudgeQueryPolicy(_)`, `QueryFailed(_)`, `HashParsing(_)`, `Other(_)`, `Internal(_)` | `Internal` |

**`From<ClientError>`:**
| `ClientError` variant | `XetError` category |
|---|---|
| `AuthError(_)` | `Authentication` |
| `ReqwestError(_, _)`, `ReqwestMiddlewareError(_)`, `PresignedUrlExpirationError` | `Network` |
| `FileNotFound(_)`, `XORBNotFound(_)` | `NotFound` |
| `ConfigurationError(_)`, `InvalidArguments`, `InvalidRange`, `InvalidShardKey(_)`, `InvalidKey(_)`, `InvalidRepoType(_)` | `Configuration` |
| `IOError(_)` | `Io` |
| `FormatError(fe)` | routes through `FormatError` logic |
| all others | `Internal` |

**`From<DataError>`:**
| `DataError` variant | `XetError` category |
|---|---|
| `AuthError(_)` | `Authentication` |
| `ClientError(ce)` | routes through `ClientError` logic |
| `FormatError(fe)` | routes through `FormatError` logic |
| `IOError(_)` | `Io` |
| `RuntimeError(re)` | routes through `RuntimeError` logic |
| `FileQueryPolicyError(_)`, `CASConfigError(_)`, `ShardConfigError(_)`, `DedupConfigError(_)`, `ParameterError(_)`, `DeprecatedError(_)` | `Configuration` |
| `HashNotFound` | `NotFound` |
| `HashStringParsingFailure(_)` | `DataIntegrity` |
| all others | `Internal` |

**Direct `From` implementations:**
| Source type | `XetError` category |
|---|---|
| `std::io::Error` | `Io` |
| `tokio::task::JoinError` | `Cancelled` if cancelled, else `Internal` |
| `tokio::sync::AcquireError` | `Cancelled` |
| `PoisonError<MutexGuard<T>>` | `Internal` |
| `PoisonError<RwLockWriteGuard<T>>` | `Internal` |
| `PoisonError<RwLockReadGuard<T>>` | `Internal` |

---

## 4. Changes to `git_xet` Error Type

`GitXetError` in `git_xet/src/errors.rs` was updated:

| Old | New |
|---|---|
| `HubClient(#[from] hub_client::HubClientError)` | `Client(#[from] ClientError)` |
| Manual `impl From<CasClientError> for GitXetError` | Removed (covered by `#[from] ClientError`) |
| `use cas_client::CasClientError;` | Removed |
| `use data::errors::DataProcessingError;` | `use xet_data::processing::errors::DataProcessingError;` |
| `use hub_client::HubClientError;` | `use xet_client::ClientError;` |

---

## 5. Changes to `hf_xet_wasm` Error Type

`DataProcessingError` in `wasm/hf_xet_wasm/src/errors.rs` was updated:

| Old | New |
|---|---|
| `CasClientError(#[from] CasClientError)` | `ClientError(#[from] ClientError)` |
| `XorbSerializationError(#[from] XorbObjectError)` | removed (covered by `FormatError`) |
| `MDBShardError(#[from] MDBShardError)` | `FormatError(#[from] FormatError)` |
| `use cas_client::CasClientError;` | `use xet_client::ClientError;` |
| `use mdb_shard::error::MDBShardError;` | removed |
| `use xorb_object::error::XorbObjectError;` | `use xet_core_structures::FormatError;` |

Since `MDBShardError` and `XorbObjectError` are both aliases for `FormatError`,
having both as `#[from]` would produce conflicting `From` impls.  They have
been merged into a single `FormatError(#[from] FormatError)` variant.

---

## 6. Changes to `hf_xet` (Python Bindings)

Import paths were updated from the old crate structure:

```rust
// Old
use xet_runtime::XetRuntime;
use xet_runtime::errors::MultithreadedRuntimeError;
use xet_runtime::sync_primatives::spawn_os_thread;

// New
use xet_runtime::core::XetRuntime;
use xet_runtime::RuntimeError;
use xet_runtime::core::sync_primatives::spawn_os_thread;
```

All occurrences of `MultithreadedRuntimeError` in function signatures and
`Result` types were replaced with `RuntimeError`.

---

## 7. Cargo.toml Dependency Updates

All `Cargo.toml` files that referenced old crate paths need updating.

### Path dependency changes

```toml
# Old (examples — actual relative paths vary)
cas_client = { path = "../cas_client" }
hub_client = { path = "../hub_client" }
cas_types  = { path = "../cas_types" }
chunk_cache = { path = "../chunk_cache" }
mdb_shard  = { path = "../mdb_shard" }
xorb_object = { path = "../xorb_object" }
merklehash = { path = "../merklehash" }
data       = { path = "../data" }
xet_runtime = { path = "../xet_runtime" }
utils      = { path = "../utils" }
xet_config = { path = "../xet_config" }
xet_logging = { path = "../xet_logging" }
error_printer = { path = "../error_printer" }
file_utils = { path = "../file_utils" }
deduplication = { path = "../deduplication" }
file_reconstruction = { path = "../file_reconstruction" }
progress_tracking = { path = "../progress_tracking" }
xet-session = { path = "../xet_session" }

# New
xet-runtime = { path = "../xet_runtime" }          # or "../../xet_runtime" from wasm/
xet-core-structures = { path = "../xet_core_structures" }
xet-client = { path = "../xet_client" }
xet-data = { path = "../xet_data" }
hf-xet = { path = "../xet_pkg" }
```

The `git_xet` crate is at `git_xet/` in the repository root, so its relative
paths use `../` (same as the other workspace crates).

---

## 8. Error Types Retained Without Merging

These error types were intentionally **not** merged into the package-level
enums because they serve specialised roles:

| Error Type | Location | Reason |
|---|---|---|
| `AuthError` | `xet_client::cas_client::auth` | Used in `TokenRefresher` trait return type; must remain distinct |
| `RwTaskLockError` | `xet_runtime::utils::rw_task_lock` | Generic bound `E: From<RwTaskLockError>` in `RwTaskLock<T, E>` requires distinct type |
| `SingleflightError<E>` | `xet_runtime::utils::errors` | Generic over error type `E`; cannot be flattened |
| `ChunkCacheError` | `xet_client::chunk_cache::error` | Distinct internal cache error; converted to `ClientError::CacheError(String)` via `From` |
| `FileReconstructionError` | `xet_data::file_reconstruction::error` | Uses `Arc`-wrapped variants for `Clone`; converted to `DataError::FileReconstructionError` via `From` |
| `GitLFSProtocolError` | `git_xet::lfs_agent_protocol::errors` | Git-specific, out of scope for core error hierarchy |

---

## 9. Summary of Variant Name Normalisations

When old error types were merged, variant names were normalised to remove
redundant suffixes.  If downstream code explicitly constructs or pattern-matches
on variants using the old alias names, these are the renames to apply:

### MDBShardError variants (now FormatError via alias)

| Old | New |
|---|---|
| `IOError(io::Error)` | `Io(io::Error)` |
| `TruncatedHashCollisionError(u64)` | `TruncatedHashCollision(u64)` |
| `ShardVersionError(String)` | `ShardVersion(String)` |
| `InternalError(anyhow::Error)` | `Internal(anyhow::Error)` |
| `QueryFailed(String)` | `QueryFailed(String)` (unchanged) |
| `SmudgeQueryPolicyError(String)` | `SmudgeQueryPolicy(String)` |

### XorbObjectError variants (now FormatError via alias)

| Old | New |
|---|---|
| `InternalIOError(io::Error)` | `Io(io::Error)` |
| `InternalError(anyhow::Error)` | `Internal(anyhow::Error)` |
| `FormatError(anyhow::Error)` | `Format(anyhow::Error)` |
| `CompressionError(lz4_flex::frame::Error)` | `Compression(lz4_flex::frame::Error)` |
| `HashParsingError(Infallible)` | `HashParsing(Infallible)` |
| `ChunkHeaderParseError` | `ChunkHeaderParse` |

### CasClientError variants (now ClientError via alias)

| Old | New |
|---|---|
| `XorbObjectError(XorbObjectError)` | `FormatError(FormatError)` |
| `MDBShardError(MDBShardError)` | `FormatError(FormatError)` (merged, both are FormatError) |

### DataProcessingError variants (now DataError via alias)

| Old | New |
|---|---|
| `MDBShardError(MDBShardError)` | `FormatError(FormatError)` |
| `CasClientError(CasClientError)` | `ClientError(ClientError)` |
| `XorbSerializationError(XorbObjectError)` | `FormatError(FormatError)` |

### SessionError variants (now XetError via alias)

| Old | New |
|---|---|
| `TaskJoinError(JoinError)` | Removed — use `XetError::from(join_error)` |
| `Runtime(MultithreadedRuntimeError)` | Removed — use `XetError::from(runtime_error)` |
| `SemaphoreClosed(AcquireError)` | Removed — use `XetError::from(acquire_error)` |
| `DataProcessing(DataProcessingError)` | Removed — use `XetError::from(data_error)` |
| `LockPoisoned(String)` | Removed — use `XetError::from(poison_error)` |
| `Io(io::Error)` | Removed — use `XetError::from(io_error)` |
| `Other(String)` | Use `XetError::Internal(msg)` or `XetError::other(msg)` |
| `Aborted` | `XetError::Aborted` (unchanged) |
| `AlreadyCommitted` | `XetError::AlreadyCommitted` (unchanged) |
| `AlreadyFinished` | `XetError::AlreadyFinished` (unchanged) |
| `InvalidTaskID(Ulid)` | `XetError::InvalidTaskID(Ulid)` (unchanged) |

The old `SessionError` wrapped lower-level errors as distinct variants with
`#[from]`.  The new `XetError` has explicit `From` implementations that
**classify** the incoming error into the appropriate user-facing category
instead.  Code that previously used `map_err(SessionError::TaskJoinError)`
should use `map_err(SessionError::from)` (or `map_err(XetError::from)`).

---

## 10. Serialisation Compatibility

No binary or JSON wire formats were changed.  The error type consolidation
only affects Rust-level types; serialised data on disk and over the network
is unaffected.
