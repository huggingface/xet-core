This update adds a new public deduplication helper for computing restart-safe chunk boundaries from existing chunk boundary metadata.

What changed
- Added `next_stable_chunk_boundary(starting_position, chunk_boundaries) -> Option<usize>`.
- Canonical implementation lives in `xet_core_structures::xorb_object::constants` (alongside the chunk-size constants it uses).
- Re-exported from `xet_data::deduplication` for convenience.
- The function scans forward from `starting_position` and returns the next chunk boundary that satisfies the stable-boundary condition:
  - two consecutive chunk sizes in `[2 * min_chunk, max_chunk - min_chunk)`,
  - where `min_chunk` and `max_chunk` are derived from chunking constants.

Why this matters
- Callers that already have chunk-boundary metadata can locate a stable resume boundary without re-reading file bytes.
- This enables deterministic alignment behavior for resumed/partial workflows that need chunk boundaries robust to prefix changes.
- The server-side `build_file_chunk_hashes_response` now extends dirty ranges to stable chunk boundaries before building windows, so that the client's rechunking around each dirty window is guaranteed to converge to the same chunk boundaries as the original file.

Usage notes
- `chunk_boundaries` should be monotonically increasing chunk-end offsets produced by the same chunking configuration.
- `starting_position` may be any byte offset (not necessarily a chunk boundary) and is used as the reference offset from which to search for the next stable chunk boundary.
