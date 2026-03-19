# Simulation Client Fixes for GC Integration Testing

## Overview

This document describes four correctness fixes made to `xet_client`'s simulation
infrastructure (`LocalClient`, `DeletionControlableClient`, `LocalTestServerBuilder`,
and `verify_integrity`). The fixes were discovered while building GC integration tests
that run the full six-stage garbage-collection pipeline against a real HTTP server.

Each fix addresses a real data-correctness bug — not merely a testing convenience.
The bugs existed silently because the affected code paths were never exercised together
before multi-epoch GC testing.

---

## Fix 1: Soft-Delete in `LocalClient::delete_file_entry`

### Location
`xet_client/src/cas_client/simulation/local_client.rs` — `DeletionControlableClient` impl

### What changed
`delete_file_entry` previously loaded all shard data into memory, removed the file
entry, and rewrote every shard file to disk. The new implementation records deletion
state in a disk-backed LMDB table (`file_status_table`, keyed by file hash) and returns
immediately. No shard files are touched.

`list_file_shard_entries` and `get_file_reconstruction_info` (and related read paths)
filter out soft-deleted hashes before returning results. `compute_reconstruction_ranges`
returns `None` for soft-deleted files so reconstruction downloads of deleted files
correctly fail. Direct file access paths (`get_file_size` and `get_file_data`) also
reject soft-deleted files.

### Why it was wrong
A shard file's identity is its content hash. Rewriting the file (even if only to remove
one file entry) changes its hash. GC relies on the invariant that a shard's hash is
stable across epochs: Stage 1 snapshots shard hashes, and later stages (2, 4, 6) look
up shards by those hashes. If `delete_file_entry` rewrites a shard, its hash changes
between Stage 1 and Stage 4, causing Stage 4 to report "active shard X not found in
shards_all" and abort.

In other words: the old implementation broke the GC epoch pipeline every time any file
was deleted before GC ran.

### Struct change
```rust
pub struct LocalClient {
    // ... existing fields ...
    /// Disk-backed file deletion status (file_hash -> is_deleted).
    /// Soft deletion avoids shard rewrites and keeps shard hashes stable across epochs.
    file_status_table: heed::Database<SerdeBincode<MerkleHash>, SerdeBincode<bool>>,
    // _tmp_dir must remain last
    _tmp_dir: Option<TempDir>,
}
```

### Test coverage needed
- **`test_delete_file_entry_does_not_rewrite_shards`** — upload a file, record all
  shard hashes, call `delete_file_entry`, then assert every shard hash is unchanged.
- **`test_soft_delete_blocks_reconstruction`** — upload a file, delete it, attempt to
  reconstruct (download) it, assert reconstruction returns `None`/error.
- **`test_soft_delete_blocks_direct_file_access`** — after delete, assert both
  `get_file_size` and `get_file_data` return `FileNotFound`.
- **`test_soft_delete_is_idempotent`** — delete the same hash twice; assert no panic
  and the file remains hidden.
- **`test_soft_delete_read_delete_race_stable`** — run compact concurrent reads while
  deleting the file, then assert post-delete reads consistently return `FileNotFound`.
- **`test_deletion_status_persists_across_restart`** — verify delete status survives
  `LocalClient` restart when using a persistent directory.

---

## Fix 2: Add `remove_shard_dedup_entries` to `DeletionControlableClient`

### Location
- `xet_client/src/cas_client/simulation/deletion_controls.rs` — trait definition
- `xet_client/src/cas_client/simulation/local_client.rs` — `LocalClient` impl
- `xet_client/src/cas_client/simulation/local_server/simulation_handlers.rs` — HTTP route
- `xet_client/src/cas_client/simulation/local_server/simulation_control_client.rs` — HTTP client

### What changed
Added a new method to the `DeletionControlableClient` trait:

```rust
/// Removes all global-dedup table entries contributed by the given shard.
/// Called by GC Stage 4 before replacing or discarding a shard.
async fn remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()>;
```

**`LocalClient` implementation**: scans the LMDB global-dedup table
(`chunk_hash → shard_hash`), collects all entries whose *value* equals the target shard
hash, deletes them, and retries cleanup in bounded passes to tolerate interleaved writes.

**HTTP route**: `DELETE /simulation/shards/{hash}/dedup_entries` — calls the above
implementation via `deletion_client`.

**`SimulationControlClient`**: issues the corresponding HTTP DELETE request.

### Why it was missing
GC Stage 4 calls `connector.remove_shard_dedup_entries(old_shard_hash)` before
uploading a replacement shard. Without this method the GC `Connector` trait compiled
(the method was defined there) but no HTTP-backed implementation existed, so any
`SimulationConnector`-backed GC run would fail to compile and, once fixed, would fail
at runtime with 501 Not Implemented.

More importantly: skipping dedup removal leaves stale entries in the global-dedup
table. Future uploads that hash-match a chunk from a deleted shard will be told "you
already have this in shard X" — but shard X no longer exists. This causes silent data
loss: the upload skips writing the chunk, and the file becomes undownloadable.

### Test coverage needed
- **`test_remove_shard_dedup_entries_removes_correct_entries`** — upload two files
  sharing some chunks (to populate dedup entries), then call
  `remove_shard_dedup_entries(shard_A)`, then verify that `query_for_global_dedup_shard`
  returns `None` for chunks that were in shard A and still returns the correct shard for
  chunks only in shard B.
- **`test_remove_shard_dedup_entries_noop_on_unknown_hash`** — call with a shard hash
  that was never registered; assert `Ok(())` with no side effects.
- **`test_remove_shard_dedup_entries_via_http`** — same as above but through a live
  `LocalTestServer` via `SimulationControlClient`, verifying the HTTP route works
  end-to-end.
- **`test_builder_ephemeral_disk_deletion_wired`** — through
  `LocalTestServerBuilder::with_ephemeral_disk()`, verify dedup entries can be removed
  over HTTP (`remove_shard_dedup_entries`) and are observable via dedup query.
- **`test_gc_stage4_dedup_cleanup`** — run a GC epoch that triggers Stage 4 to
  replace a shard, then upload the same content again and verify the upload succeeds
  (not silently skipped due to stale dedup entries pointing to the deleted shard).

---

## Fix 3: `LocalTestServerBuilder` Must Wire `deletion_client`

### Location
`xet_client/src/cas_client/simulation/simulation_server.rs` — `LocalTestServerBuilder::start()`

### What changed
When `LocalTestServerBuilder` creates a `LocalClient` (for `with_ephemeral_disk()` or
`with_disk_location()`), it now casts it to `Arc<dyn DeletionControlableClient>` and
passes that to `LocalServer::from_client` as the second argument. Previously the second
argument was always `None`, making all `/simulation/` deletion-control routes return
`501 Not Implemented`.

```rust
// Before (broken):
let server = LocalServer::from_client(client.clone(), None, host, port);

// After (correct):
let (client, deletion_client) = match ... {
    disk-backed => {
        let lc: Arc<LocalClient> = ...;
        let dc: Arc<dyn DeletionControlableClient> = lc.clone();
        (lc as Arc<dyn DirectAccessClient>, Some(dc))
    }
    in_memory | pre_supplied => (client, None),
};
let server = LocalServer::from_client(client, deletion_client, host, port);
```

`MemoryClient` deliberately does not implement `DeletionControlableClient` (it has no
on-disk shard files to manipulate), so it correctly produces `None`.

### Why it was wrong
`LocalTestServer` is the public API for integration tests. All GC server tests use
`LocalTestServerBuilder::new().with_ephemeral_disk().start()`. Without wiring the
deletion client, every call to `list_shard_entries`, `delete_shard_entry`,
`remove_shard_dedup_entries`, `delete_file_entry`, or `verify_integrity` through the
HTTP layer returned 501. The GC `SimulationConnector` calls all five of these methods.

This made the entire `server_tests` integration suite fail immediately.

### Test coverage needed
- **`test_builder_disk_deletion_client_wired`** — start a server with
  `with_ephemeral_disk()`, upload a file, call `list_shard_entries` via
  `SimulationControlClient`, and assert it returns a non-empty list (not 501).
- **`test_builder_disk_location_deletion_client_wired`** — same but via
  `with_disk_location(path)`.
- **`test_builder_in_memory_deletion_client_not_wired`** — start a server with the
  default in-memory backend, call `list_shard_entries`, assert it returns 501 Not
  Implemented (expected: `MemoryClient` has no disk shards).
- **`test_builder_custom_client_no_deletion_client`** — pass a custom
  `Arc<dyn DirectAccessClient>` via `with_client(...)`, assert deletion routes return
  501 (the pre-supplied path retains `None`).

---

## Fix 4: Cross-Shard `verify_integrity` Check

### Location
`xet_client/src/cas_client/simulation/local_client.rs` — `DeletionControlableClient::verify_integrity`

### What changed
The old implementation checked integrity one shard at a time: for each file entry in
shard S, it required all referenced XORBs to be *also listed in shard S*. The new
implementation does a two-pass global check:

1. **Pass 1** — scan all shards, build a global `HashMap<xorb_hash, chunk_count>`, and
   verify every listed XORB has a file on disk.
2. **Pass 2** — for each non-soft-deleted file entry in any shard, verify each
   referenced XORB either:
   - Appears in the global index (with chunk-range validation), **or**
   - Exists as a raw XORB file on disk (dedup reference; chunk-range not validated).

   Returns an error only if the XORB is absent from both the global index and disk.

### Why it was wrong
The CAS global-dedup design allows a new file upload to reference XORBs that were
uploaded by a previous client and are indexed in a *different* shard. Concretely:

- Client A uploads a 5-chunk file → shard A contains the XORB entries and the file
  entry.
- Client B uploads a different file that reuses 3 of those same chunks via dedup →
  shard B contains only B's file entry; the XORB entries remain in shard A.

The old per-shard check saw shard B's file entry referencing XORBs that weren't in
shard B and reported an integrity error — a false positive. This made `verify_integrity`
unreliable whenever global dedup was active (i.e., always in realistic tests).

The new check also correctly skips soft-deleted files (Fix 1 integration): a
soft-deleted file's entry still exists on disk in its shard, but should not be
considered for integrity purposes.

### Correctness boundary
The new check intentionally does *not* validate chunk ranges for the cross-shard dedup
case (XORB exists on disk but is not listed in any shard). This is acceptable for the
simulation: if the XORB file exists, the data is present; a production system would
re-derive chunk counts by parsing the XORB footer. A future hardening pass could load
the XORB footer to validate chunk ranges in the dedup case too.

### Test coverage needed
- **`test_verify_integrity_cross_shard_dedup_ok`** — upload a file, remove shard-side
  XORB index entries while keeping XORB files on disk, call `verify_integrity`,
  assert `Ok(())`.
- **`test_verify_integrity_detects_missing_xorb_file`** — upload a file, delete the
  XORB file from disk, call `verify_integrity`, assert `Err(...)`.
- **`test_verify_integrity_detects_bad_chunk_range`** — create a shard whose file
  entry claims `chunk_index_end` beyond the XORB's actual chunk count (in the same
  shard so validation applies), call `verify_integrity`, assert `Err(...)`.
- **`test_verify_integrity_skips_soft_deleted_files`** — upload a deleted-target file
  and a surviving file, soft-delete the target, remove only the target XORBs from disk
  and shard metadata, then assert `verify_integrity` still passes and the surviving file
  remains readable.
- **`test_verify_integrity_after_gc_epoch`** — run a full multi-epoch GC cycle that
  deletes files and their XORBs, call `verify_integrity` after each epoch, assert all
  pass.

---

## Summary Table

| Fix | File(s) | Root cause | Observable failure |
|---|---|---|---|
| Soft-delete | `local_client.rs` | `delete_file_entry` rewrote shard files, changing their hashes | GC Stage 4 "shard not found" crash on first post-delete epoch |
| `remove_shard_dedup_entries` | `deletion_controls.rs`, `local_client.rs`, `simulation_handlers.rs`, `simulation_control_client.rs` | Method missing from trait/HTTP layer | Compile error; then 501 at runtime; then stale dedup causing silent data loss |
| `deletion_client` wiring | `simulation_server.rs` | `LocalServer::from_client` always received `None` | All `/simulation/` deletion routes returned 501 Not Implemented |
| Cross-shard integrity check | `local_client.rs` | `verify_integrity` assumed file+XORB entries are always in the same shard | False-positive integrity failures whenever global dedup was active |

---

## Relationship to Existing Tests

The existing `test_verify_integrity_detects_missing_cas_block_reference` and
`test_verify_integrity_detects_invalid_chunk_range` unit tests use `load_all_shard_data`
/ `write_shard_data_and_register` (which are now `#[cfg(test)]` helpers) to construct
deliberately broken shards. Those tests remain valid and should continue to pass under
the new cross-shard check because the broken XORB reference is always in the same shard
as the file entry in those tests (no dedup involved).

The `lc_tests` integration suite exercises Fixes 1, 2, and 4 end-to-end via
`LocalClient` directly (no HTTP). The `server_tests` integration suite exercises all
four fixes through the full HTTP stack using `SimulationControlClient` and
`LocalTestServerBuilder`.  Both suites must pass before merging.
