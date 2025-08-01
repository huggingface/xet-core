# Content-Defined Chunking Algorithm

## Overview

The first step in uploading a file is to convert the file into a series of chunks. In order to "chunk" a file we use a rolling hash using the gearhash algorithm over the contents of the file and then set chunk boundaries through a few conditions.

## Boundary Detection

### The Boundary Condition

Define MASK as the 64 bit number 0xffff000000000000 (bitwise 16 1's followed by 48 0's; 18446462598732840960)

The byte position when the rolling hash `& MASK == 0` (bitwise AND is equal to 0) is a chunk boundary.

### Boundary Probability

The mask is designed so that approximately 1 in every `target_chunk_size` positions will trigger a boundary, making the average chunk size equal to the target.

## Algorithm Flow

### 1. Initialize

- Set up gearhash with 64-byte window
- Calculate boundary mask from target chunk size
- Determine minimum and maximum chunk size limits

### 2. Process Data

For each byte position in the data:

1. **Check Size and End of File Constraints**
   - If below minimum size (< 8192): skip boundary detection
   - If at maximum size (131072): force boundary creation
   - If at end of file: force boundary creation

2. **Update Hash**
   - Slide the 64-byte window forward
   - Update gearhash value using rolling property

3. **Test Boundary Condition**
   - Apply mask to current hash value
   - If condition met: create chunk boundary
   - If not: continue to next position

### 3. Create Chunks

When a boundary is found:

- Output current chunk (from last boundary to current position)
- Reset for next chunk (set rolling hash to 0)
- Continue processing

## Size Constraints

### Minimum Chunk Size

- Prevents tiny chunks that hurt compression efficiency
- `target_size / 8` (8KiB)
- Algorithm skips boundary detection until this size is reached

### Maximum Chunk Size  

- Prevents unbounded chunk growth
- `target_size × 2` (128KiB)
- Forces boundary creation regardless of hash value

## Key Properties

### Determinism

- Same content always produces same chunk boundaries
- No randomization or external dependencies
- Boundaries depend only on the 64-byte content window

### Content Sensitivity

- Small changes affect only nearby boundaries
- Most chunk boundaries remain stable when content changes
- Maximizes deduplication effectiveness

### Efficiency

- Linear time complexity: O(n) for data size n
- Constant space: only needs 64-byte window and counters
- Fast rolling hash updates

## Algorithm Benefits

### For Deduplication

- Identical content produces identical chunks regardless of file position
- Small edits don't shift all subsequent chunk boundaries
- Optimal granularity for detecting duplicate content blocks

## Parallel Chunking (Advanced)

[Proof for explanation of parallel chunking](../deduplication/src/parallel%20chunking.pdf).
