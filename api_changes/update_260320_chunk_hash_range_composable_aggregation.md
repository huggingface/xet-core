# API Update: Composable Merkle hash subtree aggregation (2026-03-20)

## Overview

This PR adds a composable subtree representation for merklehash aggregation so that large chunk streams can be hashed incrementally and merged without materializing all chunks in memory.

## API additions

- New public module: `xet_core_structures::merklehash::merkle_hash_subtree`
- New public type: `xet_core_structures::merklehash::MerkleHashSubtree` (re-exported from `merklehash::mod`)
- New public functions in `merkle_hash_subtree`:
  - `find_stable_start`
  - `find_stable_end`

## Behavior and compatibility

- Existing `xorb_hash`, `file_hash`, and `file_hash_with_salt` outputs are unchanged.
- This is an additive API update.  No existing public API was removed or renamed.
- The new `MerkleHashSubtree` path is intended for streaming/range composition use cases where O(log n) retained state is preferred over O(n) chunk retention.
- `MerkleHashSubtree` implements `Serialize`/`Deserialize` with hashes rendered as hex strings for cross-language JSON compatibility.

## Notes for downstream users

- Existing callers do not need to change anything.
- New callers can construct partial ranges with `MerkleHashSubtree::from_chunks(...)`, merge with `subtree.merge_into(&other)` or `MerkleHashSubtree::merge(&[...])`, and request `final_hash()` only when both file boundaries are known.
