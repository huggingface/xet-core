This update makes streamed reconstructions start with small term blocks, so the first bytes of a stream arrive sooner.

What changed
- Added the config value `reconstruction.stream_initial_fetch_size` (default 2MB, environment variable `HF_XET_RECONSTRUCTION_STREAM_INITIAL_FETCH_SIZE`).
- `ReconstructionTermManager::new` takes a new last argument, `is_streaming: bool`.
  - When true, the two initial blocks are `stream_initial_fetch_size` and twice that size, capped at `min_reconstruction_fetch_size`.
  - When false, the initial blocks are unchanged (`min_reconstruction_fetch_size` and twice that size).
- `FileReconstructor::run` passes `is_streaming` through, so `reconstruct_to_stream` and `reconstruct_to_unordered_stream` (and the wasm `reconstruct_to_writer`, which streams) use the small initial blocks.

Why this matters
- A stream yields nothing until its first term is downloaded, and that term can span the whole first block (up to `min_reconstruction_fetch_size`, 256MB by default). With a 67MB first term, a laptop benchmark waited 9 to 27s for the first byte.

Usage notes
- Callers of `ReconstructionTermManager::new` must pass `is_streaming`; pass `false` to keep the previous behavior.
- Bounded ranges smaller than about `min_reconstruction_fetch_size` still go in one block: the first block extends to the end of the range when the remainder is below `min_reconstruction_fetch_size`.
