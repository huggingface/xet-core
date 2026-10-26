Streamed reconstructions now start with a small first term block so the first bytes arrive sooner.

What changed
- New config `reconstruction.stream_initial_fetch_size` (default 2MB, `HF_XET_RECONSTRUCTION_STREAM_INITIAL_FETCH_SIZE`), capped at `min_reconstruction_fetch_size`. `reconstruct_to_stream`, `reconstruct_to_unordered_stream` and the wasm `reconstruct_to_writer` use it for their first block.
- `ReconstructionTermManager::new` takes a trailing `is_streaming: bool`.

Why this matters
- A stream yields nothing until its first term is downloaded, and that term could span a whole 256MB first block.

Usage notes
- Callers of `ReconstructionTermManager::new` pass `false` to keep the previous behavior.
