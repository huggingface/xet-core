# Xorb uploads go through upload grants

**Date**: 2026-10-06
**Crates**: `xet-client`, `xet-runtime`

## What changed

`RemoteClient::upload_xorb` now uploads each xorb through an upload grant instead of
`POST /v1/xorbs/{prefix}/{hash}`:

1. `POST /v1/xorb-grants/{prefix}/{hash}` with the serialized length and CRC-64/NVME checksum,
   e.g. `{"length": 1234, "checksum": {"algo": "crc64nvme", "value": "18446744073709551615"}}`.
   The checksum value is a decimal string because CRC-64 values exceed 2^53.
   201 returns a grant; 200 means the xorb already exists and nothing is uploaded.
2. Upload the serialized xorb to the grant's URL with the grant's method and headers, without
   CAS credentials. 412 counts as already uploaded.
3. `POST /v1/xorb-commits/{prefix}/{hash}/grant/{grant_id}`. 404 means no data was uploaded for
   the grant; the client requests a new grant and uploads again, up to three grants.

A 404 from any grant request for a xorb makes the client upload that xorb through `/v1/xorbs`;
every xorb requests a grant first. Any other error after retries fails the upload; there is no
fallback. The `Client` trait and the `upload_xorb` signature are unchanged. The caller still
acquires the upload permit before calling `upload_xorb`, which holds it until the xorb is committed.

## New public items

- `xet_client::cas_types::{Checksum, XorbUploadGrantRequest, XorbUploadGrant}`: grant request and
  response types.
- `XetConfig.client.legacy_direct_xorb_upload` (`HF_XET_CLIENT_LEGACY_DIRECT_XORB_UPLOAD`,
  default `false`): when true, always upload through `/v1/xorbs`.
- `RetryWrapper::with_412_as_success()`: a 412 response is returned as `Ok` and reported to the
  connection permit as a completed transfer.
- `LocalTestServer::set_xorb_upload_grants_enabled(bool)`: turns the local server's grant API on
  or off (off answers 404).

## Local simulation server

The local server serves the grant flow: `POST /v1/xorb-grants/{prefix}/{hash}`,
`PUT /simulation/xorb-uploads/{grant_id}` (the grant URL), and
`POST /v1/xorb-commits/{prefix}/{hash}/grant/{grant_id}`. Grants are enabled by default, so
tests that upload through a `LocalTestServer` now use the grant path. Disable it with
`set_xorb_upload_grants_enabled(false)` or `/simulation/set_config?config=xorb_upload_grants&value=off`.

## New dependencies

In `xet-client`:

- `crc-fast` (default features off, `std` only), for the CRC-64/NVME checksum.
- `serde_with` (no features), to serialize the checksum value as a decimal string.

## Migration

No code changes are required. To keep the previous upload behavior, set
`HF_XET_CLIENT_LEGACY_DIRECT_XORB_UPLOAD=1` or `legacy_direct_xorb_upload = true` in the client
config. Browser (wasm) uploads use the grant path too, so the grant URL must allow cross-origin
uploads from the page's origin.
