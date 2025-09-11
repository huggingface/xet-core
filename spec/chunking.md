# Content-Defined Chunking Algorithm

The goal in chunking is to convert file data into smaller variable length chunks, approximately 64 KiB in length.
Chunks boundaries must be computed in a deterministic way such that chunking the same data in 2 different places yields chunks that can be deduplicated.

## Step-by-step algorithm (Gearhash-based CDC)

### Constant Parameters

- target_chunk_size: `64 KiB`
- MIN_CHUNK_SIZE: `8 KiB` (minimum chunk size)
- MAX_CHUNK_SIZE: `128 KiB` (maximum chunk size)
- MASK: `0xffff000000000000` (16 one-bits → boundary probability 1/2^16 per byte)
- TABLE[256]: table of 256 64-bit constants ([rust-gearhash-table])

### State

- h: 64-bit hash, initialized to 0
- start_offset: start offset of the current chunk, initialized to 0

### Per-byte update rule (Gearhash)

For each input byte `b`, update the hash with 64-bit wrapping arithmetic:

```text
h = (h << 1) + TABLE[b]
```

### Boundary test and size constraints

At each position after updating `h`, let `size = current_offset - start_offset + 1`.

- If `size < MIN_CHUNK_SIZE`: skip testing `MASK`; continue
- Else if `size >= MAX_CHUNK_SIZE`: force a boundary
- Else if `(h & MASK) == 0`: boundary at this position

When a boundary found or taken:

- Emit the chunk `[start_offset, current_offset + 1)`
- Set `start_offset = current_offset + 1`
- Reset `h = 0`

At end-of-file, if `start_offset < len(data)`, emit the final chunk `[start_offset, len(data))`.

### Pseudocode

```text
Inputs: (See above for constant parameters)
  data: byte array

State:
  h = 0
  start_offset = 0 // start of the "current chunk"

if len(data) < MIN_CHUNK_SIZE:
  emit chunk [0, len(data))
  done

for i in range(0, len(data)):
  b = data[i]
  h = (h << 1) + TABLE[b]      // 64-bit wrapping
  size = i + 1 - start_offset

  if size < MIN_CHUNK_SIZE:
    continue

  if size >= MAX_CHUNK_SIZE or (h & MASK) == 0:
    emit chunk [start_offset, i + 1)
    start_offset = i + 1
    h = 0

if start_offset < len(data):
  emit chunk [start_offset, len(data))
```

### Boundary probability and mask selection

Given that MASK has 16 one-bits, for a random 64-bit hash h, the chance that all those 16 bits are zero is 1 / 2^16. On average, that means you’ll see a match about once every 64 KiB.

### Properties

- Deterministic boundaries: same content → same chunks
- Locality: small edits only affect nearby boundaries
- Linear time and constant memory: single 64-bit state and counters

### Intuition and rationale

- The table `TABLE[256]` injects pseudo-randomness per byte value so that the evolving hash `h` behaves like a random 64-bit value with respect to the mask test. This makes boundaries content-defined yet statistically evenly spaced.
- The left shift `(h << 1)` amplifies recent bytes, helping small changes affect nearby positions without globally shifting all boundaries.
- Resetting `h` to 0 at each boundary prevents long-range carryover and keeps boundary decisions for each chunk statistically independent.

### Implementation notes

- Only reset `h` when you emit a boundary. This ensures chunking is stable even when streaming input in pieces.
- Apply the mask test only once `size >= MIN_CHUNK_SIZE`. This reduces the frequency of tiny chunks and stabilizes average chunk sizes.
- Force a boundary at `size >= MAX_CHUNK_SIZE` even if `(h & MASK) != 0`. This guarantees bounded chunk sizes and prevents pathological long chunks when matches are rare.
- Use 64-bit wrapping arithmetic for `(h << 1) + TABLE[b]`. This is the behavior in the reference implementation [rust-gearhash].

### Edge cases

- Tiny files: if `len(data) < MIN_CHUNK_SIZE`, the entire `data` is emitted as a single chunk.
- Long runs without a match: if no position matches `(h & MASK) == 0` before `MAX_CHUNK_SIZE`, a boundary is forced at `MAX_CHUNK_SIZE` to cap chunk size.

### Portability and determinism

- With a fixed `T[256]` table and mask, the algorithm is deterministic across platforms: same input → same chunk boundaries.
- Endianness does not affect behavior because updates are byte-wise and use scalar 64-bit operations.
- SIMD-accelerated implementations (when available) are optimizations only; they produce the same boundaries as the scalar path [rust-gearhash].

## Minimum-size skip-ahead (cut-point skipping optimization)

Computing and testing the rolling hash at every byte is expensive for large data, and early tests inside the first few bytes of a chunk are disallowed by the `MIN_CHUNK_SIZE` constraint anyway.
We are able to intentionally skip testing some data with cut-point skipping to accelerate scanning without affecting correctness.

The hash function by virtue of the use of 64 byte integer length and the bit shift (`(h << 1) + TABLE[b]`) causes the hash at any byte offset to only depend on the last 64 bytes.
With a Gear rolling hash window of 64 bytes, the first boundary test is deferred until at least `MIN_CHUNK_SIZE - 64 - 1` bytes into the chunk.
This ensures that, by the time the first boundary can be considered (at offset `MIN_CHUNK_SIZE`), at least one full hash window of bytes from the current chunk has influenced the hash state.

- Effect:
  - Distribution quality is preserved because the first admissible test uses a well-mixed hash (full window), avoiding bias from the earliest bytes.
  - Performance improves by avoiding per-byte hashing/judgment in the prefix where boundaries cannot be taken.
  - Correctness is preserved because boundaries are never allowed before `MIN_CHUNK_SIZE` and the hash produced at a testable offset is the same as the hash computed had we not skipped any bytes.
- Notes:
  - This is an optimization of the search procedure only; it does not change the boundary condition, mask, or emitted chunk set compared to a byte-by-byte implementation that simply refrains from taking boundaries before `MIN_CHUNK_SIZE`.
  - In the reference code, this appears as advancing the scan pointer by up to `MIN_CHUNK_SIZE - 64 - 1` before invoking the mask test loop.

## References

- rust-gearhash: Fast, SIMD-accelerated GEAR hashing for CDC [rust-gearhash]
- FastCDC paper (background and design rationale of CDC) [fastcdc-paper]

[rust-gearhash]: https://github.com/srijs/rust-gearhash
[rust-gearhash-table]: https://github.com/srijs/rust-gearhash/blob/adad44e7141cfd29d898cf6e0858f50b995db286/src/table.rs#L5
[fastcdc-paper]: https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia
