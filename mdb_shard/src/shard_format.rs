use std::collections::{BTreeMap, HashMap};
use std::io::{copy, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::anyhow;
use merklehash::{HMACKey, MerkleHash};
use static_assertions::const_assert;
use tracing::debug;
use utils::serialization_utils::*;

use crate::cas_structs::*;
use crate::constants::*;
use crate::error::{MDBShardError, Result};
use crate::file_structs::*;
use crate::interpolation_search::search_on_sorted_u64s;
use crate::shard_in_memory::MDBInMemoryShard;
use crate::utils::{shard_expiry_time, truncate_hash};

// Same size for FileDataSequenceHeader and FileDataSequenceEntry
pub const MDB_FILE_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u32>();
const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileDataSequenceHeader>());
const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileDataSequenceEntry>());
const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileVerificationEntry>());
const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileMetadataExt>());
// Same size for CASChunkSequenceHeader and CASChunkSequenceEntry
const MDB_CAS_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u32>();
const_assert!(MDB_CAS_INFO_ENTRY_SIZE == size_of::<CASChunkSequenceHeader>());
const_assert!(MDB_CAS_INFO_ENTRY_SIZE == size_of::<CASChunkSequenceEntry>());

const MDB_SHARD_FOOTER_SIZE: i64 = size_of::<MDBShardFileFooter>() as i64;

const MDB_SHARD_HEADER_VERSION: u64 = 2;

const MDB_SHARD_FOOTER_VERSION: u64 = 1;

// At the start of each shard file, insert a tag plus a magic-number sequence of bytes to ensure
// that we are able to quickly identify a file as a shard file.

// FOR NOW: Change the header tag to include BETA.  When we're ready to
const MDB_SHARD_HEADER_TAG: [u8; 32] = [
    b'H', b'F', b'R', b'e', b'p', b'o', b'M', b'e', b't', b'a', b'D', b'a', b't', b'a', 0, 85, 105, 103, 69, 106, 123,
    129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
];

#[inline]
pub fn current_timestamp() -> u64 {
    // Get the seconds since the epoc as u64
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Debug, PartialEq)]
pub struct MDBShardFileHeader {
    // Header to be determined?  "XetHub MDB Shard File Version 1"
    pub tag: [u8; 32],
    pub version: u64,
    pub footer_size: u64,
}

impl Default for MDBShardFileHeader {
    fn default() -> Self {
        Self {
            tag: MDB_SHARD_HEADER_TAG,
            version: MDB_SHARD_HEADER_VERSION,
            footer_size: MDB_SHARD_FOOTER_SIZE as u64,
        }
    }
}

impl MDBShardFileHeader {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        writer.write_all(&MDB_SHARD_HEADER_TAG)?;
        write_u64(writer, self.version)?;
        write_u64(writer, self.footer_size)?;

        Ok(size_of::<MDBShardFileHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut tag = [0u8; 32];
        reader.read_exact(&mut tag)?;

        if tag != MDB_SHARD_HEADER_TAG {
            return Err(MDBShardError::ShardVersionError(
                "File does not appear to be a valid Merkle DB Shard file (Wrong Magic Number).".to_owned(),
            ));
        }

        Ok(Self {
            tag,
            version: read_u64(reader)?,
            footer_size: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MDBShardFileFooter {
    pub version: u64,
    pub file_info_offset: u64,
    pub cas_info_offset: u64,

    // Lookup tables.  These come after the info sections to allow the shard to be partially
    // read without needing to read the footer.
    pub file_lookup_offset: u64,
    pub file_lookup_num_entry: u64,
    pub cas_lookup_offset: u64,
    pub cas_lookup_num_entry: u64,
    pub chunk_lookup_offset: u64,
    pub chunk_lookup_num_entry: u64,

    // HMAC key protection for the chunk hashes.  If zero, then no key.
    pub chunk_hash_hmac_key: HMACKey,

    // The creation time of this shard, in seconds since the epoc
    pub shard_creation_timestamp: u64,

    // The time, in seconds since the epoc, after which this shard is no longer assumed to be valid.
    // Locally created shards do not have an expiry.
    pub shard_key_expiry: u64,

    // More locations to stick in here if needed.
    pub _buffer: [u64; 6],
    pub stored_bytes_on_disk: u64,
    pub materialized_bytes: u64,
    pub stored_bytes: u64,
    pub footer_offset: u64, // Always last.
}

impl Default for MDBShardFileFooter {
    fn default() -> Self {
        Self {
            version: MDB_SHARD_FOOTER_VERSION,
            file_info_offset: 0,
            cas_info_offset: 0,
            file_lookup_offset: 0,
            file_lookup_num_entry: 0,
            cas_lookup_offset: 0,
            cas_lookup_num_entry: 0,
            chunk_lookup_offset: 0,
            chunk_lookup_num_entry: 0,
            chunk_hash_hmac_key: HMACKey::default(), // No HMAC key

            // On serialization, this is set to current time if this is zero.
            shard_creation_timestamp: 0,
            shard_key_expiry: u64::MAX,
            _buffer: [0u64; 6],
            stored_bytes_on_disk: 0,
            materialized_bytes: 0,
            stored_bytes: 0,
            footer_offset: 0,
        }
    }
}

impl MDBShardFileFooter {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        write_u64(writer, self.version)?;
        write_u64(writer, self.file_info_offset)?;
        write_u64(writer, self.cas_info_offset)?;
        write_u64(writer, self.file_lookup_offset)?;
        write_u64(writer, self.file_lookup_num_entry)?;
        write_u64(writer, self.cas_lookup_offset)?;
        write_u64(writer, self.cas_lookup_num_entry)?;
        write_u64(writer, self.chunk_lookup_offset)?;
        write_u64(writer, self.chunk_lookup_num_entry)?;
        write_hash(writer, &self.chunk_hash_hmac_key)?;
        write_u64(writer, self.shard_creation_timestamp)?;
        write_u64(writer, self.shard_key_expiry)?;
        write_u64s(writer, &self._buffer)?;
        write_u64(writer, self.stored_bytes_on_disk)?;
        write_u64(writer, self.materialized_bytes)?;
        write_u64(writer, self.stored_bytes)?;
        write_u64(writer, self.footer_offset)?;

        Ok(size_of::<MDBShardFileFooter>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let version = read_u64(reader)?;

        // Do a version check as a simple guard against using this in an old repository
        if version != MDB_SHARD_FOOTER_VERSION {
            return Err(MDBShardError::ShardVersionError(format!(
                "Error: Expected footer version {MDB_SHARD_FOOTER_VERSION}, got {version}"
            )));
        }

        let mut obj = Self {
            version,
            file_info_offset: read_u64(reader)?,
            cas_info_offset: read_u64(reader)?,
            file_lookup_offset: read_u64(reader)?,
            file_lookup_num_entry: read_u64(reader)?,
            cas_lookup_offset: read_u64(reader)?,
            cas_lookup_num_entry: read_u64(reader)?,
            chunk_lookup_offset: read_u64(reader)?,
            chunk_lookup_num_entry: read_u64(reader)?,
            chunk_hash_hmac_key: read_hash(reader)?,
            shard_creation_timestamp: read_u64(reader)?,
            shard_key_expiry: read_u64(reader)?,
            ..Default::default()
        };
        read_u64s(reader, &mut obj._buffer)?;
        obj.stored_bytes_on_disk = read_u64(reader)?;
        obj.materialized_bytes = read_u64(reader)?;
        obj.stored_bytes = read_u64(reader)?;
        obj.footer_offset = read_u64(reader)?;

        Ok(obj)
    }
}

/// File info.  This is a list of the file hash content to be downloaded.
///
/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry.
///
/// [
///     FileDataSequenceHeader, // u32 index in File lookup directs here.
///     [
///         FileDataSequenceEntry,
///     ],
/// ], // Repeats per file.
///
///
/// ----------------------------------------------------------------------------
///
/// CAS info.  This is a list of chunks in order of appearance in the CAS chunks.
///
/// Each CAS consists of a CASChunkSequenceHeader following
/// a sequence of CASChunkSequenceEntry.
///
/// [
///     CASChunkSequenceHeader, // u32 index in CAS lookup directs here.
///     [
///         CASChunkSequenceEntry, // (u32, u32) index in Chunk lookup directs here.
///     ],
/// ], // Repeats per CAS.
///
/// ----------------------------------------------------------------------------
///  
/// File info lookup.  This is a lookup of a truncated file hash to the
/// location index in the File info section.
///
/// Sorted Vec<(u64, u32)> on the u64.
///
/// The first entry is the u64 truncated file hash, and the next entry is the
/// index in the file info section of the element that starts the file reconstruction section.
///
/// ----------------------------------------------------------------------------
///
/// CAS info lookup.  This is a lookup of a truncated CAS hash to the
/// location index in the CAS info section.
///
/// Sorted Vec<(u64, u32)> on the u64.
///
/// The first entry is the u64 truncated CAS block hash, and the next entry is the
/// index in the cas info section of the element that starts the cas entry section.
///
/// ----------------------------------------------------------------------------
///
/// Chunk info lookup. This is a lookup of a truncated CAS chunk hash to the
/// location in the CAS info section.
///
/// Sorted Vec<(u64, (u32, u32))> on the u64.
///
/// The first entry is the u64 truncated CAS chunk in a CAS block, the first u32 is the index
/// in the CAS info section that is the start of the CAS block, and the subsequent u32 gives
/// the offset index of the chunk in that CAS block.

#[derive(Clone, Default, Debug, PartialEq)]
pub struct MDBShardInfo {
    pub header: MDBShardFileHeader,
    pub metadata: MDBShardFileFooter,
}

impl MDBShardInfo {
    pub fn load_from_reader<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let mut obj = Self::default();

        // Move cursor to beginning of shard file.
        reader.rewind()?;
        obj.header = MDBShardFileHeader::deserialize(reader)?;

        // Move cursor to end of shard file minus footer size.
        reader.seek(SeekFrom::End(-MDB_SHARD_FOOTER_SIZE))?;
        obj.metadata = MDBShardFileFooter::deserialize(reader)?;

        Ok(obj)
    }

    pub fn serialize_from<W: Write>(writer: &mut W, mdb: &MDBInMemoryShard, expiry: Option<Duration>) -> Result<Self> {
        let mut shard = MDBShardInfo::default();

        let mut bytes_pos: usize = 0;

        // Write shard header.
        bytes_pos += shard.header.serialize(writer)?;

        // Write file info.
        shard.metadata.file_info_offset = bytes_pos as u64;
        let ((file_lookup_keys, file_lookup_vals), bytes_written) =
            Self::convert_and_save_file_info(writer, &mdb.file_content)?;
        bytes_pos += bytes_written;

        // Write CAS info.
        shard.metadata.cas_info_offset = bytes_pos as u64;
        let ((cas_lookup_keys, cas_lookup_vals), (chunk_lookup_keys, chunk_lookup_vals), bytes_written) =
            Self::convert_and_save_cas_info(writer, &mdb.cas_content)?;
        bytes_pos += bytes_written;

        // Write file info lookup table.
        shard.metadata.file_lookup_offset = bytes_pos as u64;
        shard.metadata.file_lookup_num_entry = file_lookup_keys.len() as u64;
        for (&e1, &e2) in file_lookup_keys.iter().zip(file_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2)?;
        }
        bytes_pos += size_of::<u64>() * file_lookup_keys.len() + size_of::<u32>() * file_lookup_vals.len();

        // Release memory.
        drop(file_lookup_keys);
        drop(file_lookup_vals);

        // Write cas info lookup table.
        shard.metadata.cas_lookup_offset = bytes_pos as u64;
        shard.metadata.cas_lookup_num_entry = cas_lookup_keys.len() as u64;
        for (&e1, &e2) in cas_lookup_keys.iter().zip(cas_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2)?;
        }
        bytes_pos += size_of::<u64>() * cas_lookup_keys.len() + size_of::<u32>() * cas_lookup_vals.len();

        // Write chunk lookup table.
        shard.metadata.chunk_lookup_offset = bytes_pos as u64;
        shard.metadata.chunk_lookup_num_entry = chunk_lookup_keys.len() as u64;
        for (&e1, &e2) in chunk_lookup_keys.iter().zip(chunk_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2.0)?;
            write_u32(writer, e2.1)?;
        }
        bytes_pos += size_of::<u64>() * chunk_lookup_keys.len() + size_of::<u64>() * chunk_lookup_vals.len();

        // Update repo size information.
        shard.metadata.stored_bytes_on_disk = mdb.stored_bytes_on_disk();
        shard.metadata.materialized_bytes = mdb.materialized_bytes();
        shard.metadata.stored_bytes = mdb.stored_bytes();

        // Update footer offset.
        shard.metadata.footer_offset = bytes_pos as u64;

        if let Some(shard_valid_for) = expiry {
            // Use this to cause things to not be valid after a certain while.
            shard.metadata.shard_key_expiry = shard_expiry_time(shard_valid_for);
        }

        // Write shard footer.
        shard.metadata.serialize(writer)?;

        Ok(shard)
    }

    #[allow(clippy::type_complexity)]
    fn convert_and_save_file_info<W: Write>(
        writer: &mut W,
        file_content: &BTreeMap<MerkleHash, MDBFileInfo>,
    ) -> Result<(
        (Vec<u64>, Vec<u32>), // File Lookup Info
        usize,                // Bytes used for File Content Info
    )> {
        // File info lookup table.
        let mut file_lookup_keys = Vec::<u64>::with_capacity(file_content.len());
        let mut file_lookup_vals = Vec::<u32>::with_capacity(file_content.len());

        let mut index: u32 = 0;
        let mut bytes_written = 0;

        for (file_hash, content) in file_content {
            file_lookup_keys.push(truncate_hash(file_hash));
            file_lookup_vals.push(index);

            let bytes = content.serialize(writer)?;

            bytes_written += bytes;
            debug_assert!(bytes % MDB_FILE_INFO_ENTRY_SIZE == 0);
            index += (bytes / MDB_FILE_INFO_ENTRY_SIZE) as u32;
        }

        // Serialize a single bookend entry as a guard for sequential reading.
        bytes_written += FileDataSequenceHeader::bookend().serialize(writer)?;

        // No need to sort because BTreeMap is ordered and we truncate by the first 8 bytes.
        Ok(((file_lookup_keys, file_lookup_vals), bytes_written))
    }

    #[allow(clippy::type_complexity)]
    fn convert_and_save_cas_info<W: Write>(
        writer: &mut W,
        cas_content: &BTreeMap<MerkleHash, Arc<MDBCASInfo>>,
    ) -> Result<(
        (Vec<u64>, Vec<u32>),        // CAS Lookup Info
        (Vec<u64>, Vec<(u32, u32)>), // Chunk Lookup Info
        usize,                       // Bytes used for CAS Content Info
    )> {
        // CAS info lookup table.
        let mut cas_lookup_keys = Vec::<u64>::with_capacity(cas_content.len());
        let mut cas_lookup_vals = Vec::<u32>::with_capacity(cas_content.len());

        // Chunk lookup table.
        let mut chunk_lookup_keys = Vec::<u64>::with_capacity(cas_content.len()); // may grow
        let mut chunk_lookup_vals = Vec::<(u32, u32)>::with_capacity(cas_content.len()); // may grow

        let mut index: u32 = 0;
        let mut bytes_written = 0;

        for (cas_hash, content) in cas_content {
            cas_lookup_keys.push(truncate_hash(cas_hash));
            cas_lookup_vals.push(index);

            bytes_written += content.metadata.serialize(writer)?;

            for (i, chunk) in content.chunks.iter().enumerate() {
                bytes_written += chunk.serialize(writer)?;

                chunk_lookup_keys.push(truncate_hash(&chunk.chunk_hash));
                chunk_lookup_vals.push((index, i as u32));
            }

            index += 1 + content.chunks.len() as u32;
        }

        // Serialize a single bookend entry as a guard for sequential reading.
        bytes_written += CASChunkSequenceHeader::bookend().serialize(writer)?;

        // No need to sort cas_lookup_ because BTreeMap is ordered and we truncate by the first 8 bytes.

        // Sort chunk lookup table by key.
        let mut chunk_lookup_combined = chunk_lookup_keys.iter().zip(chunk_lookup_vals.iter()).collect::<Vec<_>>();

        chunk_lookup_combined.sort_unstable_by_key(|&(k, _)| k);

        Ok((
            (cas_lookup_keys, cas_lookup_vals),
            (
                chunk_lookup_combined.iter().map(|&(k, _)| *k).collect(),
                chunk_lookup_combined.iter().map(|&(_, v)| *v).collect(),
            ),
            bytes_written,
        ))
    }

    pub fn chunk_hashes_protected(&self) -> bool {
        self.metadata.chunk_hash_hmac_key != HMACKey::default()
    }

    pub fn chunk_hmac_key(&self) -> Option<HMACKey> {
        if self.metadata.chunk_hash_hmac_key == HMACKey::default() {
            None
        } else {
            Some(self.metadata.chunk_hash_hmac_key)
        }
    }

    pub fn get_file_info_index_by_hash<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_hash: &MerkleHash,
        dest_indices: &mut [u32; 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.file_lookup_offset,
            self.metadata.file_lookup_num_entry,
            truncate_hash(file_hash),
            read_u32::<R>,
            dest_indices,
        )?;

        // Assume no more than 8 collisions.
        if num_indices < dest_indices.len() {
            Ok(num_indices)
        } else {
            Err(MDBShardError::TruncatedHashCollisionError(truncate_hash(file_hash)))
        }
    }

    pub fn get_cas_info_index_by_hash<R: Read + Seek>(
        &self,
        reader: &mut R,
        cas_hash: &MerkleHash,
        dest_indices: &mut [u32; 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.cas_lookup_offset,
            self.metadata.cas_lookup_num_entry,
            truncate_hash(cas_hash),
            read_u32::<R>,
            dest_indices,
        )?;

        // Assume no more than 8 collisions.
        if num_indices < dest_indices.len() {
            Ok(num_indices)
        } else {
            Err(MDBShardError::TruncatedHashCollisionError(truncate_hash(cas_hash)))
        }
    }

    pub fn get_cas_info_index_by_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        unkeyed_chunk_hash: &MerkleHash,
        dest_indices: &mut [(u32, u32); 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.chunk_lookup_offset,
            self.metadata.chunk_lookup_num_entry,
            truncate_hash(&self.keyed_chunk_hash(unkeyed_chunk_hash)),
            |reader| (Ok((read_u32(reader)?, read_u32(reader)?))),
            dest_indices,
        )?;

        // Chunk lookup hashes are Ok to have (many) collisions,
        // we will use a subset of collisions to do dedup.
        if num_indices == dest_indices.len() {
            debug!(
                "Found {:?} or more collisions when searching for truncated hash {:?}",
                dest_indices.len(),
                truncate_hash(unkeyed_chunk_hash)
            );
        }

        Ok(num_indices)
    }

    /// Reads the file info from a specific index.  Note that this is the position
    pub fn read_file_info<R: Read + Seek>(&self, reader: &mut R, file_entry_index: u32) -> Result<MDBFileInfo> {
        reader.seek(SeekFrom::Start(
            self.metadata.file_info_offset + (MDB_FILE_INFO_ENTRY_SIZE as u64) * (file_entry_index as u64),
        ))?;

        let Some(mdb_file) = MDBFileInfo::deserialize(reader)? else {
            return Err(MDBShardError::InternalError(anyhow!("invalid file entry index")));
        };

        Ok(mdb_file)
    }

    pub fn read_all_file_info_sections<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<MDBFileInfo>> {
        let mut ret = Vec::<MDBFileInfo>::with_capacity(self.num_file_entries());

        reader.seek(SeekFrom::Start(self.metadata.file_info_offset))?;

        while let Some(mdb_file) = MDBFileInfo::deserialize(reader)? {
            ret.push(mdb_file);
        }

        Ok(ret)
    }

    pub fn read_all_cas_blocks<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<(CASChunkSequenceHeader, u64)>> {
        // Reads all the cas blocks, returning a list of the cas info and the
        // starting position of that cas block.

        let mut cas_blocks =
            Vec::<(CASChunkSequenceHeader, u64)>::with_capacity(self.metadata.cas_lookup_num_entry as usize);

        reader.seek(SeekFrom::Start(self.metadata.cas_info_offset))?;

        loop {
            let pos = reader.stream_position()?;
            let cas_block = CASChunkSequenceHeader::deserialize(reader)?;
            if cas_block.is_bookend() {
                break;
            }
            let n = cas_block.num_entries;
            cas_blocks.push((cas_block, pos));

            reader.seek(SeekFrom::Current((size_of::<CASChunkSequenceEntry>() as i64) * (n as i64)))?;
        }
        Ok(cas_blocks)
    }

    /// Returns the keyed chunk hash for the shard.
    #[inline]
    pub fn keyed_chunk_hash(&self, chunk_hash: impl AsRef<MerkleHash>) -> MerkleHash {
        let chunk_hash = *chunk_hash.as_ref();
        if self.metadata.chunk_hash_hmac_key != HMACKey::default() {
            chunk_hash.hmac(self.metadata.chunk_hash_hmac_key)
        } else {
            chunk_hash
        }
    }

    /// Returns a vector holding all the chunk hashes along with their (cas idx, entry idx) locations
    pub fn read_all_cas_blocks_full<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<MDBCASInfo>> {
        let mut ret = Vec::with_capacity(self.num_cas_entries());

        let (cas_info_start, _cas_info_end) = self.cas_info_byte_range();

        reader.seek(SeekFrom::Start(cas_info_start))?;

        while let Some(cas_info) = MDBCASInfo::deserialize(reader)? {
            debug_assert!(reader.stream_position()? < _cas_info_end);
            ret.push(cas_info);
        }
        debug_assert_eq!(reader.stream_position()?, _cas_info_end);

        Ok(ret)
    }

    pub fn read_full_cas_lookup<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<(u64, u32)>> {
        // Reads all the cas blocks, returning a list of the cas info and the
        // starting position of that cas block.

        let mut cas_lookup: Vec<(u64, u32)> = Vec::with_capacity(self.metadata.cas_lookup_num_entry as usize);

        reader.seek(SeekFrom::Start(self.metadata.cas_lookup_offset))?;

        for _ in 0..self.metadata.cas_lookup_num_entry {
            let trunc_cas_hash: u64 = read_u64(reader)?;
            let idx: u32 = read_u32(reader)?;
            cas_lookup.push((trunc_cas_hash, idx));
        }

        Ok(cas_lookup)
    }

    // Given a file pointer, returns the information needed to reconstruct the file.
    // The information is stored in the destination vector dest_results.  The function
    // returns true if the file hash was found, and false otherwise.
    pub fn get_file_reconstruction_info<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_hash: &MerkleHash,
    ) -> Result<Option<MDBFileInfo>> {
        // Search in file info lookup table.
        let mut dest_indices = [0u32; 8];
        let num_indices = self.get_file_info_index_by_hash(reader, file_hash, &mut dest_indices)?;

        // Check each file info if the file hash matches.
        for &file_entry_index in dest_indices.iter().take(num_indices) {
            let mdb_file_info = self.read_file_info(reader, file_entry_index)?;
            if mdb_file_info.metadata.file_hash == *file_hash {
                return Ok(Some(mdb_file_info));
            }
        }

        Ok(None)
    }

    // Performs a query of block hashes against a known block hash, matching
    // as many of the values in query_hashes as possible.  It returns the number
    // of entries matched from the input hashes, the CAS block hash of the match,
    // and the range matched from that block.
    // In this case, a location hint is given to the function.  It will only return a
    // match from that point
    pub fn chunk_hash_dedup_query_direct<R: Read + Seek>(
        &self,
        reader: &mut R,
        unkeyed_query_hashes: &[MerkleHash],
        cas_entry_index: u32,
        cas_chunk_offset: u32,
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        if unkeyed_query_hashes.is_empty() {
            return Ok(None);
        }

        reader.seek(SeekFrom::Start(
            self.metadata.cas_info_offset + (MDB_CAS_INFO_ENTRY_SIZE as u64) * (cas_entry_index as u64),
        ))?;

        let cas_header = CASChunkSequenceHeader::deserialize(reader)?;

        if cas_chunk_offset != 0 {
            // Jump forward to the chunk at chunk_offset.
            reader.seek(SeekFrom::Current(MDB_CAS_INFO_ENTRY_SIZE as i64 * cas_chunk_offset as i64))?;
        }

        // Now, read in data while the query hashes match.
        let first_chunk = CASChunkSequenceEntry::deserialize(reader)?;
        if first_chunk.chunk_hash != self.keyed_chunk_hash(unkeyed_query_hashes[0]) {
            return Ok(None);
        }

        let mut n_bytes = first_chunk.unpacked_segment_bytes;

        // Read everything else until the CAS block end.
        let mut end_idx = 0;
        for i in 1.. {
            if cas_chunk_offset as usize + i == cas_header.num_entries as usize {
                end_idx = i;
                break;
            }

            let chunk = CASChunkSequenceEntry::deserialize(reader)?;

            if i == unkeyed_query_hashes.len() || chunk.chunk_hash != self.keyed_chunk_hash(unkeyed_query_hashes[i]) {
                end_idx = i;
                break;
            }

            n_bytes += chunk.unpacked_segment_bytes;
        }

        Ok(Some((
            end_idx,
            FileDataSequenceEntry {
                cas_hash: cas_header.cas_hash,
                cas_flags: cas_header.cas_flags,
                unpacked_segment_bytes: n_bytes,
                chunk_index_start: cas_chunk_offset,
                chunk_index_end: cas_chunk_offset + end_idx as u32,
            },
        )))
    }

    // Performs a query of block hashes against a known block hash, matching
    // as many of the values in query_hashes as possible.  It returns the number
    // of entries matched from the input hashes, the CAS block hash of the match,
    // and the range matched from that block.
    pub fn chunk_hash_dedup_query<R: Read + Seek>(
        &self,
        reader: &mut R,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        if query_hashes.is_empty() || self.metadata.chunk_lookup_num_entry == 0 {
            return Ok(None);
        }

        // Lookup CAS block from chunk lookup.
        let mut dest_indices = [(0u32, 0u32); 8];
        let num_indices = self.get_cas_info_index_by_chunk(reader, &query_hashes[0], &mut dest_indices)?;

        // Sequentially match chunks in that block.
        for &(cas_index, chunk_offset) in dest_indices.iter().take(num_indices) {
            if let Some(cas) = self.chunk_hash_dedup_query_direct(reader, query_hashes, cas_index, chunk_offset)? {
                return Ok(Some(cas));
            }
        }
        Ok(None)
    }

    pub fn read_all_truncated_hashes<R: Read + Seek>(&self, reader: &mut R) -> Result<Vec<(u64, (u32, u32))>> {
        let mut ret;
        if self.metadata.chunk_lookup_num_entry != 0 {
            ret = Vec::with_capacity(self.metadata.chunk_lookup_num_entry as usize);
            reader.seek(SeekFrom::Start(self.metadata.chunk_lookup_offset))?;

            for _ in 0..self.metadata.chunk_lookup_num_entry {
                ret.push((read_u64(reader)?, (read_u32(reader)?, read_u32(reader)?)));
            }
        } else {
            let (cas_info_start, cas_info_end) = self.cas_info_byte_range();

            // We don't have the lookup table, so
            let n_elements_cap = (cas_info_end - cas_info_start) as usize / size_of::<CASChunkSequenceEntry>();

            ret = Vec::with_capacity(n_elements_cap);

            let mut cas_index = 0;

            reader.seek(SeekFrom::Start(cas_info_start))?;
            while reader.stream_position()? < cas_info_end {
                let cas_header = CASChunkSequenceHeader::deserialize(reader)?;

                for chunk_index in 0..cas_header.num_entries {
                    let chunk = CASChunkSequenceEntry::deserialize(reader)?;
                    ret.push((truncate_hash(&chunk.chunk_hash), (cas_index, chunk_index)));
                }
                cas_index += 1 + cas_header.num_entries;
            }
        }

        Ok(ret)
    }

    pub fn num_cas_entries(&self) -> usize {
        self.metadata.cas_lookup_num_entry as usize
    }

    pub fn num_file_entries(&self) -> usize {
        self.metadata.file_lookup_num_entry as usize
    }

    pub fn total_num_chunks(&self) -> usize {
        self.metadata.chunk_lookup_num_entry as usize
    }

    pub fn file_info_byte_range(&self) -> (u64, u64) {
        (self.metadata.file_info_offset, self.metadata.cas_info_offset)
    }

    pub fn cas_info_byte_range(&self) -> (u64, u64) {
        (self.metadata.cas_info_offset, self.metadata.file_lookup_offset)
    }

    pub fn file_lookup_byte_range(&self) -> (u64, u64) {
        (self.metadata.file_lookup_offset, self.metadata.cas_lookup_offset)
    }

    pub fn cas_lookup_byte_range(&self) -> (u64, u64) {
        (self.metadata.cas_lookup_offset, self.metadata.chunk_lookup_offset)
    }

    pub fn chuck_lookup_byte_range(&self) -> (u64, u64) {
        (self.metadata.chunk_lookup_offset, self.metadata.footer_offset)
    }

    /// Returns the number of bytes in the shard
    pub fn num_bytes(&self) -> u64 {
        self.metadata.footer_offset + size_of::<MDBShardFileFooter>() as u64
    }

    pub fn stored_bytes_on_disk(&self) -> u64 {
        self.metadata.stored_bytes_on_disk
    }

    pub fn materialized_bytes(&self) -> u64 {
        self.metadata.materialized_bytes
    }

    pub fn stored_bytes(&self) -> u64 {
        self.metadata.stored_bytes
    }

    /// returns the number of bytes that is fixed and not part of any content; i.e. would be part of an empty shard.
    pub fn non_content_byte_size() -> u64 {
        (size_of::<MDBShardFileFooter>() + size_of::<MDBShardFileHeader>()) as u64 // Header and footer
        + size_of::<FileDataSequenceHeader>() as u64 // Guard block for scanning.
        + size_of::<CASChunkSequenceHeader>() as u64 // Guard block for scanning.
    }

    pub fn print_report(&self) {
        // File info bytes.
        let (file_info_start, file_info_end) = self.file_info_byte_range();
        eprintln!("Byte size of file info: {}, ({file_info_start} - {file_info_end})", file_info_end - file_info_start);

        // Cas info bytes.
        let (cas_info_start, cas_info_end) = self.cas_info_byte_range();
        eprintln!("Byte size of cas info: {}, ({cas_info_start} - {cas_info_end})", cas_info_end - cas_info_start);

        // File lookup bytes.
        let (file_lookup_start, file_lookup_end) = self.file_lookup_byte_range();
        eprintln!(
            "Byte size of file lookup: {}, ({file_lookup_start} - {file_lookup_end})",
            file_lookup_end - file_lookup_start
        );

        // CAS lookup bytes.
        let (cas_lookup_start, cas_lookup_end) = self.cas_lookup_byte_range();
        eprintln!(
            "Byte size of cas lookup: {}, ({cas_lookup_start} - {cas_lookup_end})",
            cas_lookup_end - cas_lookup_start
        );

        // Chunk lookup bytes.
        let (chunk_lookup_start, chunk_lookup_end) = self.chuck_lookup_byte_range();
        eprintln!(
            "Byte size of chunk lookup: {}, ({chunk_lookup_start} - {chunk_lookup_end})",
            chunk_lookup_end - chunk_lookup_start
        );
    }

    /// Read all file info from shard and return a collection of
    /// (file_hash, (byte_start, byte_end) for file_data_sequence_entry,
    /// Option<(byte_start, byte_end)> for file_verification_entry,
    /// and an Option<MerkleHash> for the SHA if it is present.
    #[allow(clippy::type_complexity)]
    pub fn read_file_info_ranges<R: Read + Seek>(
        reader: &mut R,
    ) -> Result<Vec<(MerkleHash, (u64, u64), Option<(u64, u64)>, Option<MerkleHash>)>> {
        let mut ret = Vec::new();

        let _shard_header = MDBShardFileHeader::deserialize(reader)?;

        loop {
            let header = FileDataSequenceHeader::deserialize(reader)?;

            if header.is_bookend() {
                break;
            }

            let byte_start = reader.stream_position()?;
            reader.seek(SeekFrom::Current(header.num_entries as i64 * size_of::<FileDataSequenceEntry>() as i64))?;
            let byte_end = reader.stream_position()?;

            let data_sequence_entry_byte_range = (byte_start, byte_end);

            let verification_entry_byte_range = if header.contains_verification() {
                let byte_start = byte_end;
                reader
                    .seek(SeekFrom::Current(header.num_entries as i64 * size_of::<FileVerificationEntry>() as i64))?;
                let byte_end = reader.stream_position()?;

                Some((byte_start, byte_end))
            } else {
                None
            };

            let sha256 = if header.contains_metadata_ext() {
                let metadata_ext = FileMetadataExt::deserialize(reader)?;
                Some(metadata_ext.sha256)
            } else {
                None
            };

            ret.push((header.file_hash, data_sequence_entry_byte_range, verification_entry_byte_range, sha256));
        }

        Ok(ret)
    }

    /// Returns a list of chunk hashes for the global dedup service.
    /// The chunk hashes are either multiple of 'hash_filter_modulues',
    /// or the hash of the first chunk of a file present in the shard.
    pub fn filter_cas_chunks_for_global_dedup<R: Read + Seek>(reader: &mut R) -> Result<Vec<MerkleHash>> {
        let mut ret = Vec::new();

        // First, go through and get all of the cas chunks.  This allows us to form the lookup for the CAS block
        // hashes later.
        let shard = MDBShardInfo::load_from_reader(reader)?;

        let cas_chunks = shard.read_all_cas_blocks_full(reader)?;
        let mut cas_block_lookup = HashMap::<MerkleHash, usize>::with_capacity(cas_chunks.len());

        for (i, cas_info) in cas_chunks.iter().enumerate() {
            cas_block_lookup.insert(cas_info.metadata.cas_hash, i);
            for chunk in cas_info.chunks.iter() {
                if hash_is_global_dedup_eligible(&chunk.chunk_hash) {
                    ret.push(chunk.chunk_hash);
                }
            }
        }

        // Now, go through all the files present, collecting the first chunks of the files.
        // TODO: break this out into a utility if needed.
        let files = shard.read_all_file_info_sections(reader)?;

        for fi in files {
            let Some(entry) = fi.segments.first() else {
                continue;
            };

            let Some(cas_block_index) = cas_block_lookup.get(&entry.cas_hash) else {
                continue;
            };

            // Scan the cas entries to get the proper index
            let first_chunk_hash = cas_chunks[*cas_block_index].chunks[entry.chunk_index_start as usize].chunk_hash;

            ret.push(first_chunk_hash);
        }

        Ok(ret)
    }

    /// Export the current shard as an hmac keyed shard, returning the number of bytes written
    #[allow(clippy::too_many_arguments)]
    pub fn export_as_keyed_shard<R: Read + Seek, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
        hmac_key: HMACKey,
        key_valid_for: std::time::Duration,
        include_file_info: bool,
        include_cas_lookup_table: bool,
        include_chunk_lookup_table: bool,
    ) -> Result<usize> {
        Self::export_as_keyed_shard_impl(
            reader,
            writer,
            hmac_key,
            key_valid_for,
            include_file_info,
            include_cas_lookup_table,
            include_chunk_lookup_table,
            Some(self),
        )
    }

    /// Export the current shard as an hmac keyed shard,
    #[allow(clippy::too_many_arguments)]
    pub fn export_as_keyed_shard_streaming<R: Read + Seek, W: Write>(
        reader: &mut R,
        writer: &mut W,
        hmac_key: HMACKey,
        key_valid_for: std::time::Duration,
        include_file_info: bool,
        include_cas_lookup_table: bool,
        include_chunk_lookup_table: bool,
    ) -> Result<usize> {
        Self::export_as_keyed_shard_impl(
            reader,
            writer,
            hmac_key,
            key_valid_for,
            include_file_info,
            include_cas_lookup_table,
            include_chunk_lookup_table,
            None,
        )
    }
    /// Internal implementation of exporting the current shard as an hmac keyed shard,
    #[allow(clippy::too_many_arguments)]
    fn export_as_keyed_shard_impl<R: Read + Seek, W: Write>(
        reader: &mut R,
        writer: &mut W,
        hmac_key: HMACKey,
        key_valid_for: std::time::Duration,
        include_file_info: bool,
        include_cas_lookup_table: bool,
        include_chunk_lookup_table: bool,
        // Pass this in when we have it so we can use debug asserts for verification checking in tests.
        self_verification: Option<&Self>,
    ) -> Result<usize> {
        // The footer at the end that will hold each of these sections.
        let mut out_footer = MDBShardFileFooter::default();

        // Read in the header, verifying all the information.
        let in_header = MDBShardFileHeader::deserialize(reader)?;

        // Dump out the header.
        let mut byte_pos = 0;
        byte_pos += in_header.serialize(writer)?;

        // Read in all the file information.
        out_footer.file_info_offset = byte_pos as u64;

        // Possibly save the lookup info here.
        let mut file_lookup = Vec::<(u64, u32)>::new();

        // Index of entry for lookup table
        let mut index: u32 = 0;

        // materialized bytes for later storage

        let mut materialized_bytes = 0;

        loop {
            let file_metadata = FileDataSequenceHeader::deserialize(reader)?;

            if file_metadata.is_bookend() {
                // Serialize the bookend struct and move on.
                byte_pos += file_metadata.serialize(writer)?;
                break;
            }

            let num_entries = file_metadata.num_entries as usize;

            let mut n_extended_bytes = 0;

            if file_metadata.contains_verification() {
                n_extended_bytes += num_entries * size_of::<FileVerificationEntry>();
            }

            if file_metadata.contains_metadata_ext() {
                n_extended_bytes += size_of::<FileMetadataExt>();
            }

            if include_file_info {
                byte_pos += file_metadata.serialize(writer)?;

                // Need to read in the metadata values so we can calculate the materialized bytes
                for _ in 0..num_entries {
                    let entry = FileDataSequenceEntry::deserialize(reader)?;
                    materialized_bytes += entry.unpacked_segment_bytes as u64;
                    byte_pos += entry.serialize(writer)?;
                }

                // Okay to just copy the rest of values over as there is nothing different between the two shards
                // up to this point.
                if n_extended_bytes != 0 {
                    byte_pos += copy(&mut reader.take(n_extended_bytes as u64), writer)? as usize;
                }

                // Put in the lookup information
                file_lookup.push((truncate_hash(&file_metadata.file_hash), index));
                index += (1 + num_entries + n_extended_bytes / MDB_FILE_INFO_ENTRY_SIZE) as u32;
            } else {
                // Discard values until the next reader break.
                copy(&mut reader.take(n_extended_bytes as u64), &mut std::io::sink())?;
            }
        }

        if let Some(self_) = self_verification {
            debug_assert_eq!(reader.stream_position()?, self_.metadata.cas_info_offset);
        }

        let mut cas_lookup = Vec::<(u64, u32)>::new();
        let mut chunk_lookup = Vec::<(u64, (u32, u32))>::new();

        // Now deal with all the cas information
        out_footer.cas_info_offset = byte_pos as u64;

        let mut cas_index = 0;
        let mut stored_bytes_on_disk = 0;
        let mut stored_bytes = 0;

        loop {
            let cas_metadata = CASChunkSequenceHeader::deserialize(reader)?;

            // All metadata gets serialized.
            byte_pos += cas_metadata.serialize(writer)?;

            if cas_metadata.is_bookend() {
                break;
            }

            if include_cas_lookup_table {
                cas_lookup.push((truncate_hash(&cas_metadata.cas_hash), cas_index));
            }

            for chunk_index in 0..cas_metadata.num_entries {
                let mut chunk = CASChunkSequenceEntry::deserialize(reader)?;

                // MAke sure we don't actually put things into an unusable state.
                if hmac_key != HMACKey::default() {
                    chunk.chunk_hash = chunk.chunk_hash.hmac(hmac_key);
                }

                if include_chunk_lookup_table {
                    chunk_lookup.push((truncate_hash(&chunk.chunk_hash), (cas_index, chunk_index)));
                }

                byte_pos += chunk.serialize(writer)?;
            }

            cas_index += 1 + cas_metadata.num_entries;
            stored_bytes_on_disk += cas_metadata.num_bytes_on_disk as u64;
            stored_bytes += cas_metadata.num_bytes_in_cas as u64;
        }

        if let Some(self_) = self_verification {
            debug_assert_eq!(reader.stream_position()?, self_.metadata.file_lookup_offset);
        }

        // Copy over all the file lookup information if that's appropriate.
        out_footer.file_lookup_offset = byte_pos as u64;

        if include_file_info {
            if let Some(self_) = self_verification {
                debug_assert_eq!(file_lookup.len(), self_.metadata.file_lookup_num_entry as usize);
            }

            for &(key, idx) in file_lookup.iter() {
                write_u64(writer, key)?;
                write_u32(writer, idx)?;
            }

            byte_pos += file_lookup.len() * (size_of::<u64>() + size_of::<u32>());

            out_footer.file_lookup_num_entry = file_lookup.len() as u64;
        } else {
            out_footer.file_lookup_num_entry = 0;
        }

        // CAS lookup section.
        out_footer.cas_lookup_offset = byte_pos as u64;

        if include_cas_lookup_table {
            if let Some(self_) = self_verification {
                debug_assert_eq!(cas_lookup.len(), self_.metadata.cas_lookup_num_entry as usize);
            }

            for &(key, idx) in cas_lookup.iter() {
                write_u64(writer, key)?;
                write_u32(writer, idx)?;
            }

            byte_pos += cas_lookup.len() * (size_of::<u64>() + size_of::<u32>());

            out_footer.cas_lookup_num_entry = cas_lookup.len() as u64;
        } else {
            out_footer.cas_lookup_num_entry = 0;
        }

        out_footer.chunk_lookup_offset = byte_pos as u64;

        // Chunk lookup section.
        if include_chunk_lookup_table {
            // This one is different now that it's hmac keyed, so we need to rebuild it.
            chunk_lookup.sort_by_key(|s| s.0);

            for &(h, (a, b)) in chunk_lookup.iter() {
                write_u64(writer, h)?;
                write_u32(writer, a)?;
                write_u32(writer, b)?;
            }

            byte_pos += chunk_lookup.len() * (size_of::<u64>() + 2 * size_of::<u32>());

            out_footer.chunk_lookup_num_entry = chunk_lookup.len() as u64;
        } else {
            out_footer.chunk_lookup_num_entry = 0;
        }

        out_footer.chunk_hash_hmac_key = hmac_key;

        // Add in the timestamps.
        let creation_time = std::time::SystemTime::now();

        out_footer.shard_creation_timestamp = creation_time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        out_footer.shard_key_expiry = creation_time
            .add(key_valid_for)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Copy over the stored information elsewhere
        out_footer.materialized_bytes = materialized_bytes;
        out_footer.stored_bytes_on_disk = stored_bytes_on_disk;
        out_footer.stored_bytes = stored_bytes;

        // And we're done here!
        out_footer.footer_offset = byte_pos as u64;

        // Write out the footer at the end.
        byte_pos += out_footer.serialize(writer)?;

        // Return the number of bytes written.
        Ok(byte_pos)
    }
}

pub mod test_routines {
    use std::cmp::min;
    use std::io::{Cursor, Read, Seek};
    use std::mem::size_of;

    use merklehash::MerkleHash;
    use rand::rngs::{SmallRng, StdRng};
    use rand::{Rng, SeedableRng};

    use super::FileVerificationEntry;
    use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
    use crate::error::Result;
    use crate::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo};
    use crate::shard_format::MDBShardInfo;
    use crate::shard_in_memory::MDBInMemoryShard;
    use crate::streaming_shard::MDBMinimalShard;

    pub fn simple_hash(n: u64) -> MerkleHash {
        MerkleHash::from([n, 1, 0, 0])
    }
    pub fn rng_hash(seed: u64) -> MerkleHash {
        let mut rng = SmallRng::seed_from_u64(seed);
        MerkleHash::from([rng.random(), rng.random(), rng.random(), rng.random()])
    }

    pub fn convert_to_file(shard: &MDBInMemoryShard) -> Result<Vec<u8>> {
        let mut buffer = Vec::<u8>::new();

        MDBShardInfo::serialize_from(&mut buffer, shard, None)?;

        Ok(buffer)
    }

    #[allow(clippy::type_complexity)]
    pub fn gen_specific_shard(
        cas_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
        verifications: Option<&[&[u64]]>,
        metadata_exts: Option<&[u64]>,
    ) -> Result<MDBInMemoryShard> {
        let mut shard = MDBInMemoryShard::default();

        for (hash, chunks) in cas_nodes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0u32;

            for (h, s) in chunks.iter() {
                cas_block.push(CASChunkSequenceEntry::new(simple_hash(*h), pos, *s));
                pos += s
            }

            shard.add_cas_block(MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(simple_hash(*hash), chunks.len(), pos),
                chunks: cas_block,
            })?;
        }

        if let Some(exts) = metadata_exts {
            assert_eq!(file_nodes.len(), exts.len());
        }

        if let Some(verification) = verifications {
            assert_eq!(file_nodes.len(), verification.len());

            for (i, ((file_hash, segments), verification)) in file_nodes.iter().zip(verification.iter()).enumerate() {
                assert_eq!(segments.len(), verification.len());

                let file_contents: Vec<_> = segments
                    .iter()
                    .map(|(h, (lb, ub))| FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub))
                    .collect();

                let verification = verification
                    .iter()
                    .map(|v: &u64| FileVerificationEntry::new(simple_hash(*v)))
                    .collect();

                let metadata_ext = metadata_exts.map(|exts| FileMetadataExt::new(simple_hash(exts[i])));

                shard.add_file_reconstruction_info(MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(
                        simple_hash(*file_hash),
                        segments.len(),
                        true,
                        metadata_ext.is_some(),
                    ),
                    segments: file_contents,
                    verification,
                    metadata_ext,
                })?;
            }
        } else {
            for (i, (file_hash, segments)) in file_nodes.iter().enumerate() {
                let file_contents: Vec<_> = segments
                    .iter()
                    .map(|(h, (lb, ub))| FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub))
                    .collect();

                let metadata_ext = metadata_exts.map(|exts| FileMetadataExt::new(simple_hash(exts[i])));

                shard.add_file_reconstruction_info(MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(
                        simple_hash(*file_hash),
                        segments.len(),
                        false,
                        metadata_ext.is_some(),
                    ),
                    segments: file_contents,
                    verification: vec![],
                    metadata_ext,
                })?;
            }
        }

        Ok(shard)
    }

    pub fn gen_random_shard(
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> Result<MDBInMemoryShard> {
        gen_random_shard_impl(
            seed,
            cas_block_sizes,
            file_chunk_range_sizes,
            contains_verification,
            contains_metadata_ext,
            false,
        )
    }

    pub fn gen_random_shard_with_cas_references(
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> Result<MDBInMemoryShard> {
        gen_random_shard_impl(
            seed,
            cas_block_sizes,
            file_chunk_range_sizes,
            contains_verification,
            contains_metadata_ext,
            true,
        )
    }

    pub fn gen_random_shard_impl(
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
        contains_verification: bool,
        contains_metadata_ext: bool,
        files_cross_reference_cas: bool,
    ) -> Result<MDBInMemoryShard> {
        // generate the cas content stuff.
        let mut shard = MDBInMemoryShard::default();
        let mut rng = StdRng::seed_from_u64(seed);

        let mut cas_nodes = Vec::new();

        for cas_block_size in cas_block_sizes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0u32;

            for _ in 0..*cas_block_size {
                cas_block.push(CASChunkSequenceEntry::new(rng_hash(rng.random()), rng.random_range(10000..20000), pos));
                pos += rng.random_range(10000..20000);
            }

            let cas_block = MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(rng_hash(rng.random()), *cas_block_size, pos),
                chunks: cas_block,
            };

            if files_cross_reference_cas {
                cas_nodes.push(cas_block.clone());
            }

            shard.add_cas_block(cas_block)?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_info = if files_cross_reference_cas {
                gen_random_file_info_with_cas_references(
                    &mut rng,
                    &cas_nodes,
                    file_block_size,
                    contains_verification,
                    contains_metadata_ext,
                )
            } else {
                gen_random_file_info(&mut rng, file_block_size, contains_verification, contains_metadata_ext)
            };

            shard.add_file_reconstruction_info(file_info)?;
        }

        Ok(shard)
    }

    pub fn gen_random_file_info(
        rng: &mut StdRng,
        file_block_size: &usize,
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> MDBFileInfo {
        let file_hash = rng_hash(rng.random());

        let file_contents: Vec<_> = (0..*file_block_size)
            .map(|_| {
                let lb = rng.random_range(0..10000);
                let ub = lb + rng.random_range(0..10000);
                FileDataSequenceEntry::new(rng_hash(rng.random()), ub - lb, lb, ub)
            })
            .collect();

        let verification = if contains_verification {
            file_contents
                .iter()
                .map(|_| FileVerificationEntry::new(rng_hash(rng.random())))
                .collect()
        } else {
            vec![]
        };

        let metadata_ext = contains_metadata_ext.then(|| rng_hash(rng.random())).map(FileMetadataExt::new);

        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash,
                *file_block_size,
                contains_verification,
                metadata_ext.is_some(),
            ),
            segments: file_contents,
            verification,
            metadata_ext,
        }
    }

    pub fn gen_random_file_info_with_cas_references(
        rng: &mut StdRng,
        cas_nodes: &[MDBCASInfo],
        file_block_size: &usize,
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> MDBFileInfo {
        let file_hash = rng_hash(rng.random()); // Not verified at the moment.

        let file_contents: Vec<_> = (0..*file_block_size)
            .map(|_| {
                let cas_idx = rng.random_range(0..cas_nodes.len());
                let cas_block = &cas_nodes[cas_idx];
                let start_idx = rng.random_range(0..cas_block.chunks.len());
                let end_idx = rng.random_range((start_idx + 1)..=(cas_nodes[cas_idx].chunks.len()));

                FileDataSequenceEntry::new(
                    cas_block.metadata.cas_hash,
                    cas_block.chunks[start_idx..end_idx]
                        .iter()
                        .map(|c| c.unpacked_segment_bytes)
                        .sum(),
                    start_idx as u32,
                    end_idx as u32,
                )
            })
            .collect();

        let verification = if contains_verification {
            file_contents
                .iter()
                .map(|_| FileVerificationEntry::new(rng_hash(rng.random())))
                .collect()
        } else {
            vec![]
        };

        let metadata_ext = contains_metadata_ext.then(|| rng_hash(rng.random())).map(FileMetadataExt::new);

        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash,
                *file_block_size,
                contains_verification,
                metadata_ext.is_some(),
            ),
            segments: file_contents,
            verification,
            metadata_ext,
        }
    }

    pub fn verify_mdb_shard(shard: &MDBInMemoryShard) -> Result<()> {
        let buffer = convert_to_file(shard)?;

        verify_mdb_shards_match(shard, Cursor::new(&buffer))
    }

    pub fn verify_mdb_shards_match<R: Read + Seek>(mem_shard: &MDBInMemoryShard, shard_info: R) -> Result<()> {
        let mut cursor = shard_info;
        // Now, test that the results on queries from the
        let shard_file = MDBShardInfo::load_from_reader(&mut cursor)?;

        // Test on the minimal shard format as well
        cursor.rewind()?;
        let min_shard = MDBMinimalShard::from_reader(&mut cursor, true, true).unwrap();

        let mem_size = mem_shard.shard_file_size();
        let disk_size = shard_file.num_bytes();

        assert_eq!(mem_size, disk_size);
        assert_eq!(mem_shard.materialized_bytes(), shard_file.materialized_bytes());
        assert_eq!(mem_shard.stored_bytes(), shard_file.stored_bytes());

        for (k, cas_block) in mem_shard.cas_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..cas_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the cas_block.
                let query_hashes_1: Vec<MerkleHash> = cas_block.chunks[i..(i + 3).min(cas_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = i as u32;
                let ub = min(i + 3, cas_block.chunks.len()) as u32;

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = shard_file.chunk_hash_dedup_query(&mut cursor, query_hashes)?.unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct CAS block hash as the second part of the
                    assert_eq!(result_m.1.cas_hash, *k);
                    assert_eq!(result_f.1.cas_hash, *k);

                    // Make sure the bounds are correct
                    assert_eq!((result_m.1.chunk_index_start, result_m.1.chunk_index_end), (lb, ub));
                    assert_eq!((result_f.1.chunk_index_start, result_f.1.chunk_index_end), (lb, ub));

                    // Make sure everything else equal.
                    assert_eq!(result_m, result_f);
                }
            }
        }

        // Test get file reconstruction info.
        // Against some valid hashes,
        let mut query_hashes: Vec<MerkleHash> = mem_shard.file_content.iter().map(|file| *file.0).collect();
        // and a few (very likely) invalid somes.
        for i in 0..3 {
            query_hashes.push(rng_hash(1000000 + i as u64));
        }

        for k in query_hashes.iter() {
            let result_m = mem_shard.get_file_reconstruction_info(k);
            let result_f = shard_file.get_file_reconstruction_info(&mut cursor, k)?;

            // Make sure two queries return same results.
            assert_eq!(result_m, result_f);

            // Make sure retriving the expected file.
            if result_m.is_some() {
                assert_eq!(result_m.unwrap().metadata.file_hash, *k);
                assert_eq!(result_f.unwrap().metadata.file_hash, *k);
            }
        }

        // Make sure the cas blocks and chunks are correct.
        let cas_blocks_full = shard_file.read_all_cas_blocks_full(&mut cursor)?;

        // Make sure the cas blocks are correct
        let cas_blocks = shard_file.read_all_cas_blocks(&mut cursor)?;
        assert_eq!(cas_blocks.len(), min_shard.num_cas());

        for (i, (cas_block, pos)) in cas_blocks.into_iter().enumerate() {
            let cas = mem_shard.cas_content.get(&cas_block.cas_hash).unwrap();

            assert_eq!(cas_block.num_entries, cas.chunks.len() as u32);

            cursor.seek(std::io::SeekFrom::Start(pos))?;

            let read_cas = MDBCASInfo::deserialize(&mut cursor)?.unwrap();
            assert_eq!(read_cas.metadata, cas_block);

            assert_eq!(&read_cas, cas.as_ref());
            assert_eq!(&cas_blocks_full[i], cas.as_ref());

            let m_cas_chunk = min_shard.cas(i);
            assert_eq!(m_cas_chunk.header(), &cas_blocks_full[i].metadata);
            assert_eq!(m_cas_chunk.num_entries(), cas_blocks_full[i].chunks.len());
            for j in 0..m_cas_chunk.num_entries() {
                assert_eq!(cas_blocks_full[i].chunks[j], m_cas_chunk.chunk(j));
            }
        }

        // Make sure the file info section is good
        {
            cursor.seek(std::io::SeekFrom::Start(0))?;
            let file_info = MDBShardInfo::read_file_info_ranges(&mut cursor)?;

            assert_eq!(file_info.len(), mem_shard.file_content.len());
            assert_eq!(file_info.len(), min_shard.num_files());

            for (i, (file_hash, data_byte_range, verification_byte_range, _metadata_ext_byte_range)) in
                file_info.into_iter().enumerate()
            {
                let true_fi = mem_shard.file_content.get(&file_hash).unwrap();

                // Check FileDataSequenceEntry
                let (byte_start, byte_end) = data_byte_range;
                cursor.seek(std::io::SeekFrom::Start(byte_start))?;

                let num_entries = (byte_end - byte_start) / (size_of::<FileDataSequenceEntry>() as u64);

                // No leftovers
                assert_eq!(num_entries * (size_of::<FileDataSequenceEntry>() as u64), (byte_end - byte_start));

                assert_eq!(num_entries, true_fi.segments.len() as u64);

                // Minimal view is good
                let m_file_info = min_shard.file(i);
                assert_eq!(m_file_info.header(), &true_fi.metadata);
                assert_eq!(m_file_info.num_entries(), true_fi.segments.len());

                for j in 0..true_fi.metadata.num_entries as usize {
                    let pos = byte_start + (j * size_of::<FileDataSequenceEntry>()) as u64;

                    cursor.seek(std::io::SeekFrom::Start(pos))?;

                    let fie = FileDataSequenceEntry::deserialize(&mut cursor)?;

                    assert_eq!(true_fi.segments[j], fie);
                    assert_eq!(m_file_info.entry(j), true_fi.segments[j])
                }

                // Check FileVerificationEntry if exists
                if let Some((byte_start, byte_end)) = verification_byte_range {
                    cursor.seek(std::io::SeekFrom::Start(byte_start))?;

                    let num_entries = (byte_end - byte_start) / (size_of::<FileVerificationEntry>() as u64);

                    // No leftovers
                    assert_eq!(num_entries * (size_of::<FileVerificationEntry>() as u64), (byte_end - byte_start));

                    assert_eq!(num_entries, true_fi.verification.len() as u64);

                    for j in 0..true_fi.metadata.num_entries as usize {
                        let pos = byte_start + (j * size_of::<FileVerificationEntry>()) as u64;

                        cursor.seek(std::io::SeekFrom::Start(pos))?;

                        let fie = FileVerificationEntry::deserialize(&mut cursor)?;

                        assert_eq!(true_fi.verification[j], fie);
                        assert_eq!(true_fi.verification[j], m_file_info.verification(j));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_routines::*;
    use crate::error::Result;

    #[test]
    fn test_simple() -> Result<()> {
        let shard = gen_random_shard(0, &[1, 1], &[1], false, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 1], &[1], true, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 1], &[1], false, true)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 1], &[1], true, true)?;
        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_specific() -> Result<()> {
        let mem_shard_1 = gen_specific_shard(&[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])], None, None)?;
        verify_mdb_shard(&mem_shard_1)?;

        let mem_shard_1 = gen_specific_shard(&[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])], Some(&[&[25]]), None)?;
        verify_mdb_shard(&mem_shard_1)?;

        let mem_shard_1 = gen_specific_shard(&[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])], None, Some(&[38]))?;
        verify_mdb_shard(&mem_shard_1)?;

        let mem_shard_1 =
            gen_specific_shard(&[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])], Some(&[&[25]]), Some(&[38]))?;
        verify_mdb_shard(&mem_shard_1)?;

        Ok(())
    }

    #[test]
    fn test_multiple() -> Result<()> {
        let shard = gen_random_shard(0, &[1], &[1, 1], false, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, true)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, true)?;
        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_corner_cases_empty() -> Result<()> {
        let shard = gen_random_shard(0, &[0], &[0], false, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[0], &[0], true, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[0], &[0], false, true)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[0], &[0], true, true)?;
        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_corner_cases_empty_entries() -> Result<()> {
        let shard = gen_random_shard(0, &[5, 6, 0, 10, 0], &[3, 4, 5, 0, 4, 0], false, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[5, 6, 0, 10, 0], &[3, 4, 5, 0, 4, 0], true, false)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[5, 6, 0, 10, 0], &[3, 4, 5, 0, 4, 0], false, true)?;
        verify_mdb_shard(&shard)?;

        let shard = gen_random_shard(0, &[5, 6, 0, 10, 0], &[3, 4, 5, 0, 4, 0], true, true)?;
        verify_mdb_shard(&shard)?;

        Ok(())
    }
}
