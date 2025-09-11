Deduplication crate: Public interfaces

This document lists the public interfaces (structs, traits, functions, and constants) exposed by the deduplication crate. It focuses on the surface API only.

Re-exports from the crate root
- Chunk: Basic unit containing content bytes and its Merkle hash.
- Chunker: Stateful chunk boundary detector and chunk producer.
- find_partitions(reader, file_size, target_chunk_size, min_partition_size, partition_scan_bytes) -> io::Result<Vec<usize>>: Partition finder for parallel chunking.
- DataAggregator: Builder that accumulates chunks and file-info into uploadable units.
- DeduplicationMetrics: Aggregated counters for bytes/chunks and upload sizes.
- FileDeduper<DataInterfaceType>: Orchestrator for per-file deduplication over a provided data interface.
- DeduplicationDataInterface: Trait defining the data-access/upload interface required by the deduper.
- RawXorbData: Container for a single upload unit (xorb) with its metadata.
- constants: Public constants used to configure chunking and xorb limits.
- test_utils: Test-only helpers for working with RawXorbData.

Structs and trait
- struct Chunk
  - Fields: hash: merklehash::MerkleHash, data: bytes::Bytes
  - Methods: new(data: Bytes) -> Chunk; also implements AsRef<[u8]>

- struct Chunker
  - Constructors: new(target_chunk_size: usize) -> Chunker; Default
  - Methods:
    - next_boundary(&mut self, data: &[u8]) -> Option<usize>
    - next(&mut self, data: &[u8], is_final: bool) -> (Option<Chunk>, usize)
    - next_block(&mut self, data: &[u8], is_final: bool) -> Vec<Chunk>
    - next_block_bytes(&mut self, data: &bytes::Bytes, is_final: bool) -> Vec<Chunk>
    - finish(&mut self) -> Option<Chunk>

- struct DataAggregator
  - Constructors: new(chunks: Vec<Chunk>, pending_file_info: mdb_shard::file_structs::MDBFileInfo, internally_referencing_entries: Vec<usize>, file_id: u64) -> DataAggregator
  - Query methods: is_empty(&self) -> bool; num_chunks(&self) -> usize; num_bytes(&self) -> usize
  - Methods:
    - finalize(self) -> (RawXorbData, Vec<(u64, MDBFileInfo, u64)>)
    - merge_in(&mut self, other: DataAggregator)

- struct DeduplicationMetrics
  - Fields (public counters):
    - total_bytes, deduped_bytes, new_bytes,
    - deduped_bytes_by_global_dedup, defrag_prevented_dedup_bytes,
    - total_chunks, deduped_chunks, new_chunks,
    - deduped_chunks_by_global_dedup, defrag_prevented_dedup_chunks,
    - xorb_bytes_uploaded, shard_bytes_uploaded, total_bytes_uploaded
  - Methods: merge_in(&mut self, other: &DeduplicationMetrics)

- trait DeduplicationDataInterface
  - Associated type: ErrorType
  - Methods (async):
    - chunk_hash_dedup_query(&self, query_hashes: &[merklehash::MerkleHash]) -> Result<Option<(usize, mdb_shard::file_structs::FileDataSequenceEntry, bool)>, ErrorType>
    - register_global_dedup_query(&mut self, chunk_hash: merklehash::MerkleHash) -> Result<(), ErrorType>
    - complete_global_dedup_queries(&mut self) -> Result<bool, ErrorType>
    - register_new_xorb(&mut self, xorb: RawXorbData) -> Result<(), ErrorType>
    - register_xorb_dependencies(&mut self, dependencies: &[progress_tracking::upload_tracking::FileXorbDependency])

- struct FileDeduper<DataInterfaceType: DeduplicationDataInterface>
  - Constructors: new(data_manager: DataInterfaceType, file_id: u64) -> FileDeduper<DataInterfaceType>
  - Methods (async unless noted):
    - process_chunks(&mut self, chunks: &[Chunk]) -> Result<DeduplicationMetrics, DataInterfaceType::ErrorType>
    - finalize(self, metadata_ext: Option<mdb_shard::file_structs::FileMetadataExt>) -> (merklehash::MerkleHash, DataAggregator, DeduplicationMetrics)

- struct RawXorbData
  - Constructors/creators:
    - from_chunks(chunks: &[Chunk], file_boundaries: Vec<usize>) -> RawXorbData
  - Accessors:
    - hash(&self) -> merklehash::MerkleHash
    - num_bytes(&self) -> usize
  - Public fields: data: Vec<bytes::Bytes>; cas_info: mdb_shard::cas_structs::MDBCASInfo; file_boundaries: Vec<usize>

Free functions
- find_partitions<R: std::io::Read + std::io::Seek>(reader: &mut R, file_size: usize, target_chunk_size: usize, min_partition_size: usize, partition_scan_bytes: usize) -> std::io::Result<Vec<usize>>

Modules and constants
- module constants (pub):
  - TARGET_CHUNK_SIZE: usize (reference-like configurable constant)
  - MINIMUM_CHUNK_DIVISOR: usize
  - MAXIMUM_CHUNK_MULTIPLIER: usize
  - MAX_XORB_BYTES: usize
  - MAX_XORB_CHUNKS: usize
  - MAX_CHUNK_SIZE: usize (lazy_static derived from TARGET_CHUNK_SIZE and MAXIMUM_CHUNK_MULTIPLIER)

Test utilities
- module test_utils (re-exported as raw_xorb_data::test_utils):
  - raw_xorb_to_vec(xorb: &RawXorbData) -> Vec<u8>
