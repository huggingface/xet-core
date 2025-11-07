//! Parallel content-defined chunking implementation
//!
//! This module provides a parallel implementation for finding
//! content-defined chunk boundaries in large files. It uses memory mapping,
//! multi-threading, and the gearhash library for boundary detection.

use std::fs::File;
use std::path::Path;

use bytes::Bytes;
#[cfg(feature = "parallel-chunking")]
use {
    crossbeam::{channel, thread},
    memmap2::MmapOptions,
};

use crate::constants::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CHUNK_SIZE};
use crate::Chunk;

#[cfg(feature = "parallel-chunking")]
mod parallel_impl {
    use super::*;

    // Configuration constants for parallel processing
    const FILE_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB file segments for processing
    const MAX_ANCHORS_PER_SEGMENT: usize = 100_000; // limit anchors per segment to control memory
    const MAX_ANCHORS_PER_THREAD: usize = 5_000; // stack array size for thread-local anchor collection
    const MAX_FINAL_ANCHORS: usize = 500_000; // global anchor limit for final boundary calculation
    const MAX_THREADS: usize = 32; // maximum threads to prevent resource exhaustion

    #[derive(Clone, Debug)]
    pub struct Range {
        pub start: u64,
        pub end: u64,
    }

    impl Range {
        #[inline]
        fn new(start: u64, end: u64) -> Self {
            Self { start, end }
        }

        #[inline]
        // Used in tests and debugging
        #[allow(dead_code)]
        pub fn len(&self) -> u64 {
            self.end - self.start
        }
    }

    struct ChunkWriter {
        ranges: Vec<Range>,
    }

    impl ChunkWriter {
        fn with_capacity(cap: usize) -> Self {
            Self {
                ranges: Vec::with_capacity(cap),
            }
        }

        #[inline]
        fn add(&mut self, start: u64, end: u64) {
            self.ranges.push(Range::new(start, end));
        }
    }

    #[inline]
    fn anchors_to_buffer(
        buf: &[u8],          // input buffer to search
        start: u64,          // search range start position
        end: u64,            // search range end position
        min_chunk_size: u64, // minimum chunk size for boundaries
        mask: u64,           // rolling hash mask for boundary detection
        anchors: &mut [u64], // output buffer for found anchor positions
        count: &mut u32,     // number of anchors found
    ) {
        let buffer_len = buf.len() as u64;
        let search_start = start.saturating_sub(min_chunk_size - 1);
        let search_end = buffer_len.min(end + (min_chunk_size - 1));
        if search_end < search_start || (search_end - search_start) < min_chunk_size {
            return;
        }

        let mut hasher = gearhash::Hasher::default();
        let mut current_idx = search_start;
        *count = 0;

        while current_idx < search_end && (*count as usize) < anchors.len() {
            // Safe cast: slice start is always within buffer bounds
            #[allow(clippy::cast_possible_truncation)]
            let slice = &buf[current_idx as usize..search_end as usize];
            if let Some(match_offset) = hasher.next_match(slice, mask) {
                let anchor_pos = current_idx + (match_offset as u64);
                if (anchor_pos - search_start + 1) >= min_chunk_size && anchor_pos >= start && anchor_pos < end {
                    anchors[*count as usize] = anchor_pos;
                    *count += 1;
                }
                current_idx = anchor_pos + 1;
            } else {
                break;
            }
        }
    }

    #[inline]
    fn lower_bound_u64(xs: &[u64], key: u64) -> usize {
        xs.partition_point(|&v| v < key)
    }

    #[derive(Clone, Copy)]
    struct Segment {
        start: u64,
        end: u64,
    }

    // Requires many arguments to execute the chunking algorithm
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn find_content_boundaries_parallel(
        buf: &[u8],          // input data to chunk
        target: u64,         // target chunk size in bytes
        min_div: u64,        // minimum chunk size divisor
        max_mul: u64,        // maximum chunk size multiplier
        min_chunk_size: u64, // minimum chunk size for hash window
        thread_count: u32,   // number of threads to use
    ) -> Vec<Range> {
        let buffer_len = buf.len() as u64;
        if buffer_len == 0 {
            return Vec::new();
        }

        let min_size = target / min_div;
        let max_size = target * max_mul;
        // Calculate rolling hash mask based on target chunk size
        let target_minus_one = target.wrapping_sub(1);
        let shift_amount = target_minus_one.leading_zeros();
        let mask = target_minus_one << shift_amount;

        // Buffer length fits in usize, safe to cast for capacity calculation
        #[allow(clippy::cast_possible_truncation)]
        let mut writer = ChunkWriter::with_capacity(((buffer_len / target) as usize).saturating_add(8));

        let mut final_anchors = Vec::<u64>::new();
        // Buffer size calculation for anchor capacity
        #[allow(clippy::cast_possible_truncation)]
        final_anchors.reserve_exact(MAX_FINAL_ANCHORS.min((buffer_len / min_size) as usize + 8));

        // Process file in segments to manage memory usage
        let mut segment_start = 0u64;
        while segment_start < buffer_len {
            let segment_end = (segment_start + FILE_SEGMENT_SIZE).min(buffer_len);
            // Segment size is controlled and within usize bounds
            #[allow(clippy::cast_possible_truncation)]
            let segment_buf = &buf[segment_start as usize..segment_end as usize];

            let mut segment_anchors = Vec::<u64>::new();
            segment_anchors.reserve_exact(MAX_ANCHORS_PER_SEGMENT);

            let thread_count_actual = match thread_count {
                0 => 1,
                _ => thread_count.min(u32::try_from(MAX_THREADS).unwrap_or(8)),
            } as usize;

            // Divide segment into equal ranges for each thread
            let thread_segments = (0..thread_count_actual)
                .map(|thread_idx| {
                    let start =
                        segment_start + (thread_idx as u64 * segment_buf.len() as u64) / thread_count_actual as u64;
                    let end = segment_start
                        + ((thread_idx as u64 + 1) * segment_buf.len() as u64) / thread_count_actual as u64;
                    Segment { start, end }
                })
                .collect::<Vec<_>>();

            let (sender, receiver) = channel::bounded::<(usize, Vec<u64>, u32)>(thread_count_actual);

            // Spawn threads to find anchors in parallel across segment ranges
            thread::scope(|scope| {
                for (thread_idx, segment) in thread_segments.iter().copied().enumerate() {
                    let buf_full = buf;
                    let sender = sender.clone();
                    scope.spawn(move |_| {
                        // Stack array size is controlled and reasonable for performance
                        #[allow(clippy::large_stack_arrays)]
                        let mut anchors_buf = [0u64; MAX_ANCHORS_PER_THREAD];
                        let mut count = 0u32;

                        anchors_to_buffer(
                            buf_full,
                            segment.start,
                            segment.end,
                            min_chunk_size,
                            mask,
                            &mut anchors_buf,
                            &mut count,
                        );

                        let anchors_vec = anchors_buf[..(count as usize)].to_vec();
                        let _ = sender.send((thread_idx, anchors_vec, count));
                    });
                }
                drop(sender);
            })
            .expect("threads joined");

            let mut thread_results = Vec::with_capacity(thread_count_actual);
            thread_results.resize_with(thread_count_actual, Vec::new);
            for _ in 0..thread_count_actual {
                if let Ok((thread_idx, anchors, _count)) = receiver.recv() {
                    thread_results[thread_idx] = anchors;
                }
            }

            for anchors in thread_results {
                if segment_anchors.len() >= MAX_ANCHORS_PER_SEGMENT {
                    break;
                }
                let remaining = MAX_ANCHORS_PER_SEGMENT - segment_anchors.len();
                segment_anchors.extend(anchors.into_iter().take(remaining));
            }

            let remaining = MAX_FINAL_ANCHORS - final_anchors.len();
            if remaining > 0 {
                let take = remaining.min(segment_anchors.len());
                final_anchors.extend_from_slice(&segment_anchors[..take]);
            }

            segment_start = segment_end;
        }

        final_anchors.sort_unstable();

        // Convert anchor positions to chunk boundary ranges
        let mut range_start = 0u64;
        let mut anchor_index = 0usize;
        while range_start < buffer_len {
            let min_end = (range_start + min_size).min(buffer_len);
            let max_end = (range_start + max_size).min(buffer_len);

            anchor_index = anchor_index.max(lower_bound_u64(&final_anchors, min_end));
            // Use anchor if available within max range, otherwise cut at max size
            let cut_point = if anchor_index < final_anchors.len() && final_anchors[anchor_index] <= max_end {
                final_anchors[anchor_index]
            } else {
                max_end
            };

            writer.add(range_start, cut_point);
            range_start = cut_point;
        }

        writer.ranges
    }
}

/// Parallel content-defined chunking implementation for large files
#[cfg(feature = "parallel-chunking")]
pub fn chunk_file_parallel<P: AsRef<Path>>(file_path: P, thread_count: Option<u32>) -> std::io::Result<Vec<Chunk>> {
    use parallel_impl::find_content_boundaries_parallel;

    let thread_count = thread_count.unwrap_or(0);
    let file = File::open(&file_path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();

    if size == 0 {
        return Ok(Vec::new());
    }

    // Use memory mapping for zero-copy file access
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let bytes: &[u8] = &mmap;

    // Map configuration constants to parallel implementation parameters
    let target = *TARGET_CHUNK_SIZE as u64;
    let min_div = *MINIMUM_CHUNK_DIVISOR as u64;
    let max_mul = *MAXIMUM_CHUNK_MULTIPLIER as u64;
    let min_chunk_size = 64u64; // Minimum for gear hash window

    // Auto-detect thread count if not specified
    let thread_count = if thread_count == 0 {
        num_cpus::get() as u32
    } else {
        thread_count
    };

    // Get content boundaries using parallel implementation
    let ranges = find_content_boundaries_parallel(bytes, target, min_div, max_mul, min_chunk_size, thread_count);

    // Convert ranges to Chunk objects with parallel hash computation
    let range_count = ranges.len();
    // Limit hash threads to available work and thread count
    let hash_thread_count = thread_count.min(u32::try_from(range_count).unwrap_or(u32::MAX)).max(1) as usize;

    let (hash_sender, hash_receiver) = channel::bounded::<(usize, Chunk)>(range_count);

    thread::scope(|scope| {
        let chunks_per_thread = range_count.div_ceil(hash_thread_count);

        for thread_idx in 0..hash_thread_count {
            let start_idx = thread_idx * chunks_per_thread;
            let end_idx = (start_idx + chunks_per_thread).min(range_count);

            if start_idx < range_count {
                let sender = hash_sender.clone();
                let ranges_ref = &ranges;
                scope.spawn(move |_| {
                    // Index needed for result ordering in parallel context
                    #[allow(clippy::needless_range_loop)]
                    for chunk_idx in start_idx..end_idx {
                        let range = &ranges_ref[chunk_idx];
                        // Range bounds are validated and within slice bounds
                        #[allow(clippy::cast_possible_truncation)]
                        let chunk_data = &bytes[range.start as usize..range.end as usize];
                        let chunk = Chunk::new(Bytes::copy_from_slice(chunk_data));
                        let _ = sender.send((chunk_idx, chunk));
                    }
                });
            }
        }
        drop(hash_sender);
    })
    .expect("Hash threads joined");

    // Collect chunks in original order using indices
    let mut chunks = vec![None; range_count];
    for _ in 0..range_count {
        if let Ok((chunk_idx, chunk)) = hash_receiver.recv() {
            chunks[chunk_idx] = Some(chunk);
        }
    }

    // Convert to final vector, filtering out any None values
    Ok(chunks.into_iter().flatten().collect())
}

/// Fallback when parallel content-defined chunking feature is not enabled
#[cfg(not(feature = "parallel-chunking"))]
pub fn chunk_file_parallel<P: AsRef<Path>>(_file_path: P, _thread_count: Option<u32>) -> std::io::Result<Vec<Chunk>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Parallel chunking feature not enabled. Enable with --features parallel-chunking",
    ))
}

#[cfg(test)]
#[cfg(feature = "parallel-chunking")]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_parallel_chunking_small_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![0u8; 1024]; // 1KB file
        temp_file.write_all(&data).unwrap();

        let result = chunk_file_parallel(temp_file.path(), None);
        assert!(result.is_ok()); // Now works for all file sizes

        let chunks = result.unwrap();
        assert!(!chunks.is_empty());

        // Verify data integrity
        let mut reconstructed = Vec::new();
        for chunk in &chunks {
            reconstructed.extend_from_slice(&chunk.data);
        }
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_parallel_chunking_large_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![42u8; 20 * 1024 * 1024]; // 20MB file
        temp_file.write_all(&data).unwrap();

        let result = chunk_file_parallel(temp_file.path(), None);
        assert!(result.is_ok());

        let chunks = result.unwrap();
        assert!(!chunks.is_empty());

        // Verify data integrity
        let mut reconstructed = Vec::new();
        for chunk in &chunks {
            reconstructed.extend_from_slice(&chunk.data);
        }
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_parallel_chunking_config() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![123u8; 2048]; // 2KB file
        temp_file.write_all(&data).unwrap();

        let result = chunk_file_parallel(temp_file.path(), Some(2));
        assert!(result.is_ok());
    }

    #[test]
    fn test_parallel_matches_reference() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![99u8; 5 * 1024 * 1024]; // 5MB file
        temp_file.write_all(&data).unwrap();

        let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(4)).unwrap();
        let reference_chunks = crate::Chunker::default().next_block(&data, true);

        assert_eq!(parallel_chunks.len(), reference_chunks.len());
        for (p, r) in parallel_chunks.iter().zip(reference_chunks.iter()) {
            assert_eq!(p.hash, r.hash);
            assert_eq!(p.data, r.data);
        }

        // Also verify that all chunks have the same hash since the data is uniform
        let expected_hash = reference_chunks[0].hash;
        for chunk in &parallel_chunks {
            assert_eq!(chunk.hash, expected_hash);
        }
    }

    #[test]
    fn test_parallel_matches_reference_random() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut data = vec![0u8; 10 * 1024 * 1024]; // 10MB
        rng.fill_bytes(&mut data);

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&data).unwrap();

        let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(8)).unwrap();
        let reference_chunks = crate::Chunker::default().next_block(&data, true);

        assert_eq!(parallel_chunks.len(), reference_chunks.len());
        for (p, r) in parallel_chunks.iter().zip(reference_chunks.iter()) {
            assert_eq!(p.hash, r.hash);
        }
    }

    #[test]
    fn test_parallel_matches_reference_repeating_pattern() {
        let pattern = vec![154, 52, 42, 34, 159, 75, 126, 224, 70, 236, 12, 196, 79, 236, 178, 124];
        let mut data = Vec::with_capacity(15 * 1024 * 1024);
        while data.len() < 15 * 1024 * 1024 {
            data.extend_from_slice(&pattern);
        }
        data.truncate(15 * 1024 * 1024);

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&data).unwrap();

        let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(16)).unwrap();
        let reference_chunks = crate::Chunker::default().next_block(&data, true);

        assert_eq!(parallel_chunks.len(), reference_chunks.len());
        for (p, r) in parallel_chunks.iter().zip(reference_chunks.iter()) {
            assert_eq!(p.hash, r.hash);
        }
    }

    #[test]
    fn test_parallel_matches_reference_mixed_entropy() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(123);
        let mut data = Vec::with_capacity(20 * 1024 * 1024);

        // Mix high and low entropy sections
        for _ in 0..20 {
            let mut random_section = vec![0u8; 256 * 1024];
            rng.fill_bytes(&mut random_section);
            data.extend_from_slice(&random_section);

            let uniform_section = vec![0xAA; 256 * 1024];
            data.extend_from_slice(&uniform_section);
        }

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&data).unwrap();

        let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(12)).unwrap();
        let reference_chunks = crate::Chunker::default().next_block(&data, true);

        assert_eq!(parallel_chunks.len(), reference_chunks.len());
        for (p, r) in parallel_chunks.iter().zip(reference_chunks.iter()) {
            assert_eq!(p.hash, r.hash);
        }
    }

    #[test]
    fn test_parallel_matches_reference_hash_window_pattern() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(999);

        // Create a 64-byte pattern (hash window size) - highly adversarial
        let mut pattern = vec![0u8; 64];
        rng.fill_bytes(&mut pattern);

        // Repeat over 100MB - large enough to stress boundaries
        let target_size = 100 * 1024 * 1024;
        let mut data = Vec::with_capacity(target_size);
        while data.len() < target_size {
            data.extend_from_slice(&pattern);
        }
        data.truncate(target_size);

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&data).unwrap();

        let reference_chunks = crate::Chunker::default().next_block(&data, true);

        // Test with different thread counts to vary boundary positions
        for thread_count in [4, 7, 13] {
            let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(thread_count)).unwrap();

            assert_eq!(parallel_chunks.len(), reference_chunks.len());
            for (p, r) in parallel_chunks.iter().zip(reference_chunks.iter()) {
                assert_eq!(p.hash, r.hash);
            }
        }
    }

    #[test]
    fn test_parallel_matches_reference_asymmetric_density() {
        use rand::{RngCore, SeedableRng};

        // Create a file with varying chunk boundary density
        // Early part has one pattern, late part has another
        let mut data = Vec::with_capacity(50 * 1024 * 1024);

        // First 25MB: pattern that may trigger frequent boundaries
        let mut rng1 = rand::rngs::StdRng::seed_from_u64(12345);
        for _ in 0..(25 * 1024) {
            let mut chunk = vec![0u8; 1024];
            rng1.fill_bytes(&mut chunk);
            data.extend_from_slice(&chunk);
        }

        // Next 25MB: different pattern that may have different boundary density
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(54321);
        for _ in 0..(25 * 1024) {
            let mut chunk = vec![0u8; 1024];
            rng2.fill_bytes(&mut chunk);
            data.extend_from_slice(&chunk);
        }

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&data).unwrap();

        let reference_chunks = crate::Chunker::default().next_block(&data, true);
        let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(16)).unwrap();

        assert_eq!(
            parallel_chunks.len(),
            reference_chunks.len(),
            "Chunk count mismatch: parallel={}, reference={}",
            parallel_chunks.len(),
            reference_chunks.len()
        );

        for (i, (p, r)) in parallel_chunks.iter().zip(reference_chunks.iter()).enumerate() {
            assert_eq!(
                p.hash, r.hash,
                "Chunk {} hash mismatch at boundary. Parallel may have dropped anchors from later threads.",
                i
            );
        }
    }

    #[test]
    fn test_parallel_matches_reference_boundary_sensitivity() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(7777);

        // Test multiple sizes to stress different thread boundary alignments
        for size_mb in [15, 17, 23, 31] {
            let mut data = vec![0u8; size_mb * 1024 * 1024];
            rng.fill_bytes(&mut data);

            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file.write_all(&data).unwrap();

            let reference_chunks = crate::Chunker::default().next_block(&data, true);

            // Test with prime thread counts to create irregular boundaries
            for thread_count in [3, 5, 11, 17] {
                let parallel_chunks = chunk_file_parallel(temp_file.path(), Some(thread_count)).unwrap();

                assert_eq!(
                    parallel_chunks.len(),
                    reference_chunks.len(),
                    "Size {}MB, threads {}: chunk count mismatch",
                    size_mb,
                    thread_count
                );

                for (i, (p, r)) in parallel_chunks.iter().zip(reference_chunks.iter()).enumerate() {
                    assert_eq!(
                        p.hash, r.hash,
                        "Size {}MB, threads {}, chunk {}: hash mismatch",
                        size_mb, thread_count, i
                    );
                }
            }
        }
    }
}
