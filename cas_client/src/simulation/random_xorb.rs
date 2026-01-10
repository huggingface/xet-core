//! Random XORB generation for testing with large files that are never fully materialized.
//!
//! This module provides `RandomXorb`, a structure that can generate XORB chunk data
//! on-the-fly using deterministic random seeds, allowing testing with massive files
//! without actually storing them in memory.

use bytes::Bytes;
use cas_object::{CAS_CHUNK_HEADER_LENGTH, CASChunkHeader, CasObject, CasObjectInfoV1, CompressionScheme};
use merklehash::{MerkleHash, compute_data_hash};
use rand::prelude::*;

/// Information about a single chunk in a RandomXorb.
#[derive(Clone, Debug)]
pub struct RandomChunkInfo {
    /// Random seed used to generate this chunk's data.
    pub seed: u64,
    /// Size of the uncompressed chunk data in bytes.
    pub size: u32,
    /// Cached hash of the chunk data.
    pub hash: MerkleHash,
}

/// A XORB that generates its chunk data on-the-fly from random seeds.
///
/// This allows testing with massive files without actually storing the data in memory.
/// Each chunk is defined by a seed and size, and the data is generated deterministically
/// using `SmallRng` when needed.
#[derive(Clone, Debug)]
pub struct RandomXorb {
    /// Information about each chunk.
    chunks: Vec<RandomChunkInfo>,
    /// Cached CasObject header/footer.
    cas_object: CasObject,
}

impl RandomXorb {
    /// Creates a new RandomXorb from a list of (seed, size) pairs.
    ///
    /// The chunk data is generated deterministically from each seed, and
    /// chunk hashes are computed and cached.
    pub fn new(chunk_specs: &[(u64, u32)]) -> Self {
        let chunks: Vec<RandomChunkInfo> = chunk_specs
            .iter()
            .map(|&(seed, size)| {
                let data = Self::generate_chunk_data_from_seed(seed, size);
                let hash = compute_data_hash(&data);
                RandomChunkInfo { seed, size, hash }
            })
            .collect();

        // Build the CasObject header/footer
        let cas_object = Self::build_cas_object(&chunks);

        Self { chunks, cas_object }
    }

    /// Creates a new RandomXorb from a seed, number of chunks, and chunk size.
    ///
    /// Each chunk gets a unique sub-seed derived from the main seed.
    pub fn from_seed(seed: u64, num_chunks: u32, chunk_size: u32) -> Self {
        use rand::prelude::*;

        let mut rng = SmallRng::seed_from_u64(seed);
        let chunk_specs: Vec<(u64, u32)> = (0..num_chunks)
            .map(|_| {
                let chunk_seed = rng.random::<u64>();
                (chunk_seed, chunk_size)
            })
            .collect();

        Self::new(&chunk_specs)
    }

    /// Builds a CasObject from chunk information.
    fn build_cas_object(chunks: &[RandomChunkInfo]) -> CasObject {
        let num_chunks = chunks.len() as u32;

        // Compute XORB hash from chunk hashes
        let xorb_hash = if chunks.is_empty() {
            MerkleHash::default()
        } else {
            let mut hash_data = Vec::with_capacity(chunks.len() * 32);
            for chunk in chunks {
                hash_data.extend_from_slice(chunk.hash.as_bytes());
            }
            compute_data_hash(&hash_data)
        };

        // Collect chunk hashes
        let chunk_hashes: Vec<MerkleHash> = chunks.iter().map(|c| c.hash).collect();

        // Compute chunk boundary offsets (physical layout with headers)
        // Each chunk has: header (8 bytes) + data (chunk.size bytes)
        let mut chunk_boundary_offsets = Vec::with_capacity(num_chunks as usize);
        let mut cumulative_offset = 0u32;
        for chunk in chunks {
            cumulative_offset += CAS_CHUNK_HEADER_LENGTH as u32 + chunk.size;
            chunk_boundary_offsets.push(cumulative_offset);
        }

        // Compute unpacked chunk offsets (uncompressed layout without headers)
        let mut unpacked_chunk_offsets = Vec::with_capacity(num_chunks as usize);
        let mut cumulative_unpacked = 0u32;
        for chunk in chunks {
            cumulative_unpacked += chunk.size;
            unpacked_chunk_offsets.push(cumulative_unpacked);
        }

        // Start with default and override the fields we need
        let mut info = CasObjectInfoV1::default();
        info.cashash = xorb_hash;
        info.chunk_hashes = chunk_hashes;
        info.chunk_boundary_offsets = chunk_boundary_offsets;
        info.unpacked_chunk_offsets = unpacked_chunk_offsets;
        info.num_chunks = num_chunks;

        // Fill in the offset fields
        info.fill_in_boundary_offsets();

        let info_length = info.serialized_length() as u32;

        CasObject { info, info_length }
    }

    /// Generates chunk data from a seed.
    fn generate_chunk_data_from_seed(seed: u64, size: u32) -> Vec<u8> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut data = vec![0u8; size as usize];
        rng.fill_bytes(&mut data);
        data
    }

    /// Returns the number of chunks in this XORB.
    pub fn num_chunks(&self) -> u32 {
        self.chunks.len() as u32
    }

    /// Returns the hash of the XORB.
    pub fn xorb_hash(&self) -> MerkleHash {
        self.cas_object.info.cashash
    }

    /// Returns the hash of a specific chunk.
    pub fn chunk_hash(&self, idx: u32) -> Option<MerkleHash> {
        self.chunks.get(idx as usize).map(|c| c.hash)
    }

    /// Returns the uncompressed size of a specific chunk.
    pub fn chunk_size(&self, idx: u32) -> Option<u32> {
        self.chunks.get(idx as usize).map(|c| c.size)
    }

    /// Returns the total uncompressed size of all chunks.
    pub fn total_uncompressed_size(&self) -> u64 {
        self.chunks.iter().map(|c| c.size as u64).sum()
    }

    /// Returns the total uncompressed size for a range of chunks [start, end).
    pub fn chunk_range_size(&self, start: u32, end: u32) -> u64 {
        (start..end).filter_map(|i| self.chunk_size(i)).map(|s| s as u64).sum()
    }

    /// Returns (hash, size) pairs for a range of chunks [start, end).
    /// This is useful for computing file hashes.
    pub fn chunk_hash_sizes(&self, start: u32, end: u32) -> Vec<(MerkleHash, u64)> {
        (start..end)
            .filter_map(|i| {
                let hash = self.chunk_hash(i)?;
                let size = self.chunk_size(i)? as u64;
                Some((hash, size))
            })
            .collect()
    }

    /// Returns the chunk hashes for a range of chunks [start, end).
    pub fn chunk_hashes_range(&self, start: u32, end: u32) -> Vec<MerkleHash> {
        (start..end).filter_map(|i| self.chunk_hash(i)).collect()
    }

    /// Generates and returns the raw data for a specific chunk.
    pub fn get_chunk_data(&self, idx: u32) -> Option<Bytes> {
        self.chunks
            .get(idx as usize)
            .map(|chunk| Bytes::from(Self::generate_chunk_data_from_seed(chunk.seed, chunk.size)))
    }

    /// Generates and returns the raw data for a range of chunks [start, end).
    pub fn get_chunk_range_data(&self, start: u32, end: u32) -> Option<Bytes> {
        if start >= end || end > self.num_chunks() {
            return None;
        }

        let mut data = Vec::new();
        for idx in start..end {
            let chunk = &self.chunks[idx as usize];
            let chunk_data = Self::generate_chunk_data_from_seed(chunk.seed, chunk.size);
            data.extend_from_slice(&chunk_data);
        }
        Some(Bytes::from(data))
    }

    /// Returns the CasObject header/footer for this XORB.
    ///
    /// Uses no compression (CompressionScheme::None) for all chunks.
    pub fn get_cas_object(&self) -> CasObject {
        self.cas_object.clone()
    }

    /// Returns the total serialized length of the XORB (chunks + footer).
    pub fn serialized_length(&self) -> u64 {
        let chunks_length: u64 = self.chunks.iter().map(|c| CAS_CHUNK_HEADER_LENGTH as u64 + c.size as u64).sum();

        let footer_length = self.cas_object.info.serialized_length() as u64 + 4; // +4 for info_length u32

        chunks_length + footer_length
    }

    /// Returns the serialized bytes for a range within the XORB.
    ///
    /// This generates the bytes on-the-fly, including chunk headers and data.
    /// The range is in terms of the serialized byte positions.
    pub fn get_serialized_range(&self, start: u64, end: u64) -> Bytes {
        let total_len = self.serialized_length();
        let end = end.min(total_len);

        if start >= end {
            return Bytes::new();
        }

        // Calculate where the footer starts
        let chunks_length: u64 = self.chunks.iter().map(|c| CAS_CHUNK_HEADER_LENGTH as u64 + c.size as u64).sum();

        let mut result = Vec::with_capacity((end - start) as usize);

        // Current position in the serialized stream
        let mut pos = 0u64;

        // Generate chunk data
        for chunk in &self.chunks {
            let chunk_serialized_len = CAS_CHUNK_HEADER_LENGTH as u64 + chunk.size as u64;
            let chunk_end = pos + chunk_serialized_len;

            if chunk_end > start && pos < end {
                // This chunk overlaps with our range
                let header = CASChunkHeader::new(CompressionScheme::None, chunk.size, chunk.size);
                let header_bytes = header_to_bytes(&header);
                let chunk_data = Self::generate_chunk_data_from_seed(chunk.seed, chunk.size);

                // Combine header and data
                let mut serialized_chunk = Vec::with_capacity(chunk_serialized_len as usize);
                serialized_chunk.extend_from_slice(&header_bytes);
                serialized_chunk.extend_from_slice(&chunk_data);

                // Extract the overlapping portion
                let overlap_start = start.saturating_sub(pos) as usize;
                let overlap_end = ((end - pos) as usize).min(serialized_chunk.len());

                if overlap_start < overlap_end {
                    result.extend_from_slice(&serialized_chunk[overlap_start..overlap_end]);
                }
            }

            pos = chunk_end;
            if pos >= end {
                break;
            }
        }

        // Generate footer if needed
        if end > chunks_length && pos < end {
            let mut footer_bytes = Vec::new();
            self.cas_object.info.serialize(&mut footer_bytes).unwrap();
            footer_bytes.extend_from_slice(&self.cas_object.info_length.to_le_bytes());

            let footer_start_in_stream = chunks_length;
            let overlap_start = start.saturating_sub(footer_start_in_stream) as usize;
            let overlap_end = ((end - footer_start_in_stream) as usize).min(footer_bytes.len());

            if overlap_start < overlap_end {
                result.extend_from_slice(&footer_bytes[overlap_start..overlap_end]);
            }
        }

        Bytes::from(result)
    }

    /// Returns the full serialized XORB.
    ///
    /// Note: This materializes the entire XORB, which may be large.
    /// Prefer `get_serialized_range` for partial access.
    pub fn get_full_serialized(&self) -> Bytes {
        self.get_serialized_range(0, self.serialized_length())
    }
}

/// Converts a CASChunkHeader to bytes.
fn header_to_bytes(header: &CASChunkHeader) -> [u8; CAS_CHUNK_HEADER_LENGTH] {
    let mut bytes = [0u8; CAS_CHUNK_HEADER_LENGTH];
    bytes[0] = 0; // version
    bytes[1..4].copy_from_slice(&header.get_compressed_length().to_le_bytes()[..3]);
    bytes[4] = CompressionScheme::None as u8;
    bytes[5..8].copy_from_slice(&header.get_uncompressed_length().to_le_bytes()[..3]);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_xorb_basic() {
        let specs = vec![(42, 1024), (123, 2048), (456, 512)];
        let xorb = RandomXorb::new(&specs);

        assert_eq!(xorb.num_chunks(), 3);
        assert!(xorb.chunk_hash(0).is_some());
        assert!(xorb.chunk_hash(1).is_some());
        assert!(xorb.chunk_hash(2).is_some());
        assert!(xorb.chunk_hash(3).is_none());

        // Verify deterministic generation
        let data1 = xorb.get_chunk_data(0).unwrap();
        let data2 = xorb.get_chunk_data(0).unwrap();
        assert_eq!(data1, data2);
        assert_eq!(data1.len(), 1024);
    }

    #[test]
    fn test_random_xorb_cas_object() {
        let specs = vec![(1, 100), (2, 200)];
        let xorb = RandomXorb::new(&specs);

        let cas = xorb.get_cas_object();
        assert_eq!(cas.info.num_chunks, 2);
        assert_eq!(cas.info.chunk_hashes.len(), 2);
        assert_eq!(cas.info.chunk_boundary_offsets.len(), 2);
        assert_eq!(cas.info.unpacked_chunk_offsets.len(), 2);

        // First chunk: header (8) + data (100) = 108
        assert_eq!(cas.info.chunk_boundary_offsets[0], 108);
        // Second chunk: 108 + header (8) + data (200) = 316
        assert_eq!(cas.info.chunk_boundary_offsets[1], 316);

        // Unpacked offsets (no headers)
        assert_eq!(cas.info.unpacked_chunk_offsets[0], 100);
        assert_eq!(cas.info.unpacked_chunk_offsets[1], 300);
    }

    #[test]
    fn test_random_xorb_chunk_range() {
        let specs = vec![(1, 100), (2, 200), (3, 300)];
        let xorb = RandomXorb::new(&specs);

        let range_data = xorb.get_chunk_range_data(0, 2).unwrap();
        assert_eq!(range_data.len(), 300); // 100 + 200

        let chunk0 = xorb.get_chunk_data(0).unwrap();
        let chunk1 = xorb.get_chunk_data(1).unwrap();
        assert_eq!(&range_data[..100], &chunk0[..]);
        assert_eq!(&range_data[100..], &chunk1[..]);
    }

    #[test]
    fn test_random_xorb_serialized_length() {
        let specs = vec![(1, 100)];
        let xorb = RandomXorb::new(&specs);

        let serialized = xorb.get_full_serialized();
        assert_eq!(serialized.len() as u64, xorb.serialized_length());
    }
}
