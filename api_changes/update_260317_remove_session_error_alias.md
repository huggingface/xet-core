# SessionError alias removal and Python exception mapping updates

This change removes the `SessionError` alias (`pub use crate::error::XetError as SessionError`) from
the public `xet_session` API and standardizes all public signatures/docs on `xet::XetError`.

## Public Rust API changes

- `xet::xet_session::SessionError` no longer exists.
- All public `xet_session` APIs now return `Result<_, xet::XetError>` directly.
- Import path update for downstream callers:
  - `xet::xet_session::SessionError` -> `xet::XetError`

## Error categorization changes

- `ClientError::PresignedUrlExpirationError` now maps to `XetError::Authentication` (was mapped as network-related).
- Added `XetError::Timeout(String)` for timeout-class network failures.

## Python (`hf_xet`) behavior changes

- `hf_xet` now exposes package-specific exception classes registered on module init:
  - `hf_xet.XetAuthenticationError` (inherits from `PermissionError`)
  - `hf_xet.XetObjectNotFoundError` (inherits from `FileNotFoundError`)
- `From<XetError> for PyErr` now maps:
  - `Authentication` -> `XetAuthenticationError`
  - `NotFound` -> `XetObjectNotFoundError`
  - `Network` -> `ConnectionError`
  - `Timeout` -> `TimeoutError`

## Migration notes for downstream users

- Rust downstream code should replace `SessionError` type references with `XetError`.
- Python downstream code can continue catching base classes (`PermissionError`, `FileNotFoundError`), and can optionally
  catch the new `hf_xet.XetAuthenticationError` / `hf_xet.XetObjectNotFoundError` for tighter handling.
