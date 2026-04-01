# API Update: Simulation global dedup shard expiration and config controls (2026-03-28)

## Overview

This update adds simulation controls for global dedup shard expiration and
unifies additional simulation knobs under `/simulation/set_config`.

---

## 1. New `DirectAccessClient` API

`xet_client::cas_client::simulation::DirectAccessClient` now includes:

```rust
fn set_global_dedup_shard_expiration(&self, expiration: Option<Duration>);
```

When set, `query_for_global_dedup_shard` returns shard bytes with:

- file reconstruction section removed
- `shard_key_expiry` set to `now + expiration`

Passing `None` disables this behavior and returns full shards (previous
behavior).

---

## 2. New `MDBMinimalShard` API

`xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard` now
includes:

```rust
pub fn serialize_xorb_subset_with_expiry<W: Write>(
    &self,
    writer: &mut W,
    expiry: Option<SystemTime>,
    xorb_filter_fn: impl Fn(&MDBXorbInfoView) -> bool,
) -> Result<usize>;
```

This serializes a shard without file section data and writes an optional
`shard_key_expiry` footer value.

---

## 3. Expanded `/simulation/set_config` keys

The `/simulation/set_config?config=<k>&value=<v>` endpoint now supports:

- `global_dedup_shard_expiration` (seconds; `0` disables)
- `max_ranges_per_fetch` (usize)
- `disable_v2_reconstruction` (HTTP status code; `0` re-enables)
- `api_delay` (`(min,max)` duration format, for example `(50ms, 50ms)`)
- `url_expiration` (milliseconds)

`SimulationControlClient` now drives these controls through
`/simulation/set_config`.

---

## 4. Behavioral notes

- Expiry is assigned at dedup query time, not at shard upload time.
- Expiration is stored with second-level precision; sub-second durations are rounded up to one second.
- With expiration disabled, global dedup shard queries preserve prior behavior.

---

## 5. `DeletionControlableClient` changes

`verify_integrity` and `verify_all_reachable` now live on
`DeletionControlableClient` (previously on `DirectAccessClient`).
Implementors of `DeletionControlableClient` must provide both methods.
The `/simulation/verify_integrity` and `/simulation/verify_all_reachable`
HTTP routes now return 501 when no deletion client is configured.

---

## 6. Downstream impact

- Any external implementors of `DirectAccessClient` must implement
  `set_global_dedup_shard_expiration`.
- Any external implementors of `DeletionControlableClient` must implement
  `verify_integrity` and `verify_all_reachable`.
- Consumers of simulation control routes can use the new `set_config` keys to
  configure runtime behavior without direct in-process client access.
