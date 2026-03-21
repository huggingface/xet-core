# API Update: Composable chunk hash range aggregation (2026-03-20)

## Overview

This PR adds a composable hash-range representation for merklehash aggregation so that large chunk streams can be hashed incrementally and merged without materializing all chunks in memory.

## API additions

- New public module: `xet_core_structures::merklehash::chunk_hash_range`
- New public type: `xet_core_structures::merklehash::ChunkHashRange` (re-exported from `merklehash::mod`)
- New public functions in `chunk_hash_range`:
  - `find_stable_start`
  - `find_stable_end`

## Behavior and compatibility

- Existing `xorb_hash`, `file_hash`, and `file_hash_with_salt` outputs are unchanged.
- This is an additive API update.  No existing public API was removed or renamed.
- The new `ChunkHashRange` path is intended for streaming/range composition use cases where O(log n) retained state is preferred over O(n) chunk retention.

## Notes for downstream users

- Existing callers do not need to change anything.
- New callers can construct partial ranges with `ChunkHashRange::new(...)`, merge with `ChunkHashRange::merge_two(...)` or `ChunkHashRange::merge(...)`, and request `final_hash()` only when both file boundaries are known.
