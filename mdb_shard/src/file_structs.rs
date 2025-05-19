use std::fmt::Debug;
use std::io::{self, Cursor, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

use merklehash::data_hash::hex;
use merklehash::MerkleHash;
use serde::Serialize;
use utils::serialization_utils::*;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
use crate::error::MDBShardError;
use crate::shard_file::MDB_FILE_INFO_ENTRY_SIZE;

pub const MDB_DEFAULT_FILE_FLAG: u32 = 0;
pub const MDB_FILE_FLAG_WITH_VERIFICATION: u32 = 1 << 31;
pub const MDB_FILE_FLAG_VERIFICATION_MASK: u32 = 1 << 31;
pub const MDB_FILE_FLAG_WITH_METADATA_EXT: u32 = 1 << 30;
pub const MDB_FILE_FLAG_METADATA_EXT_MASK: u32 = 1 << 30;

/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry, maybe a sequence
/// of FileVerificationEntry, and maybe a FileMetadataExt
/// determined by file flags.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
// Already the case, but making it explicit here to avoid compiler
// complaints on the offset_of calls.
#[repr(C)]
pub struct FileDataSequenceHeader {
    #[serde(with = "hex::serde")]
    pub file_hash: MerkleHash,
    pub file_flags: u32,
    pub num_entries: u32,
    pub _unused: u64,
}

impl FileDataSequenceHeader {
    pub fn new<I: TryInto<u32>>(
        file_hash: MerkleHash,
        num_entries: I,
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> Self
    where
        <I as TryInto<u32>>::Error: std::fmt::Debug,
    {
        let verification_flag = if contains_verification {
            MDB_FILE_FLAG_WITH_VERIFICATION
        } else {
            Default::default()
        };
        let metadata_ext_flag = if contains_metadata_ext {
            MDB_FILE_FLAG_WITH_METADATA_EXT
        } else {
            Default::default()
        };
        let file_flags = MDB_DEFAULT_FILE_FLAG | verification_flag | metadata_ext_flag;
        Self {
            file_hash,
            file_flags,
            num_entries: num_entries.try_into().unwrap(),
            #[cfg(test)]
            _unused: 126846135456846514u64,
            #[cfg(not(test))]
            _unused: 0,
        }
    }

    pub fn bookend() -> Self {
        Self {
            // The bookend file hash is all 1s
            file_hash: [!0u64; 4].into(),
            ..Default::default()
        }
    }

    pub fn is_bookend(&self) -> bool {
        self.file_hash == [!0u64; 4].into()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.file_hash)?;
            write_u32(writer, self.file_flags)?;
            write_u32(writer, self.num_entries)?;
            write_u64(writer, self._unused)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<FileDataSequenceHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = std::io::Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            file_hash: read_hash(reader)?,
            file_flags: read_u32(reader)?,
            num_entries: read_u32(reader)?,
            _unused: read_u64(reader)?,
        })
    }

    pub fn contains_metadata_ext(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_METADATA_EXT_MASK) != 0
    }

    pub fn contains_verification(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_VERIFICATION_MASK) != 0
    }

    /// Get the number of info entries following the header in this shard,
    /// this includes "FileDataSequenceEntry"s, "FileVerificationEntry"s, and "FileMetadataExt".
    pub fn num_info_entry_following(&self) -> u32 {
        let num_metadata_ext = if self.contains_metadata_ext() { 1 } else { 0 };
        if self.contains_verification() {
            self.num_entries * 2 + num_metadata_ext
        } else {
            self.num_entries + num_metadata_ext
        }
    }

    /// Verifies that the two headers correspond to the same file. Checks that
    /// the file hashes are the same and that the number of entries are the
    /// same.
    #[inline]
    pub fn verify_same_file(header1: &Self, header2: &Self) {
        debug_assert_eq!(header1.file_hash, header2.file_hash, "hashes don't match");
        debug_assert_eq!(header1.num_entries, header2.num_entries, "num entries for same hash don't match");
    }

    /// Compares the flags of headers A and B to determine if either bitmap is a superset
    /// of the other. Can return 4 possible responses:
    /// 1. SuperA if A is a superset of B (e.g. A has validation and B has nothing)
    /// 2. SuperB if B is a superset of A (e.g. B has metadata_ext and A has nothing)
    /// 3. Neither if neither A nor B are supersets of each other (e.g. A has only validation, and B has only
    ///    metadata_ext)
    /// 4. Equal if both A and B have the same flags set.
    pub fn compare_flag_superset(header_a: &Self, header_b: &Self) -> SupersetResult {
        let flags0 = header_a.file_flags;
        let flags1 = header_b.file_flags;
        if flags0 == flags1 {
            SupersetResult::Equal
        } else if flags0 & flags1 == flags1 {
            SupersetResult::SuperA
        } else if flags1 & flags0 == flags0 {
            SupersetResult::SuperB
        } else {
            SupersetResult::Neither
        }
    }
}

/// Result enum for comparing header flags for supersets.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SupersetResult {
    SuperA,
    SuperB,
    Neither,
    Equal,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[repr(C)] // Just making this explicit
pub struct FileDataSequenceEntry {
    // maps to one or more CAS chunk(s)
    #[serde(with = "hex::serde")]
    pub cas_hash: MerkleHash,
    pub cas_flags: u32,
    pub unpacked_segment_bytes: u32,
    pub chunk_index_start: u32,
    pub chunk_index_end: u32,
}

impl FileDataSequenceEntry {
    pub fn new<I1: TryInto<u32>>(
        cas_hash: MerkleHash,
        unpacked_segment_bytes: I1,
        chunk_index_start: I1,
        chunk_index_end: I1,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            cas_hash,
            cas_flags: MDB_DEFAULT_FILE_FLAG,
            unpacked_segment_bytes: unpacked_segment_bytes.try_into().unwrap(),
            chunk_index_start: chunk_index_start.try_into().unwrap(),
            chunk_index_end: chunk_index_end.try_into().unwrap(),
        }
    }

    pub fn from_cas_entries<I1: TryInto<u32>>(
        metadata: &CASChunkSequenceHeader,
        chunks: &[CASChunkSequenceEntry],
        chunk_index_start: I1,
        chunk_index_end: I1,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        if chunks.is_empty() {
            return Self::default();
        }

        Self {
            cas_hash: metadata.cas_hash,
            cas_flags: metadata.cas_flags,
            unpacked_segment_bytes: chunks.iter().map(|sb| sb.unpacked_segment_bytes).sum(),
            chunk_index_start: chunk_index_start.try_into().unwrap(),
            chunk_index_end: chunk_index_end.try_into().unwrap(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.cas_hash)?;
            write_u32(writer, self.cas_flags)?;
            write_u32(writer, self.unpacked_segment_bytes)?;
            write_u32(writer, self.chunk_index_start)?;
            write_u32(writer, self.chunk_index_end)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<FileDataSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<FileDataSequenceEntry>()];
        reader.read_exact(&mut v[..])?;

        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            cas_hash: read_hash(reader)?,
            cas_flags: read_u32(reader)?,
            unpacked_segment_bytes: read_u32(reader)?,
            chunk_index_start: read_u32(reader)?,
            chunk_index_end: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileVerificationEntry {
    #[serde(with = "hex::serde")]
    pub range_hash: MerkleHash,
    pub _unused: [u64; 2],
}

impl FileVerificationEntry {
    pub fn new(range_hash: MerkleHash) -> Self {
        Self {
            range_hash,
            _unused: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];

        {
            let mut writer = Cursor::new(&mut buf[..]);
            write_hash(&mut writer, &self.range_hash)?;
            write_u64s(&mut writer, &self._unused)?;
        }

        writer.write_all(&buf)?;

        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;

        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            range_hash: read_hash(reader)?,
            _unused: Default::default(),
        })
    }
}

/// Extended metadata about the file (e.g. sha256). Stored in the FileInfo when the
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileMetadataExt {
    #[serde(with = "hex::serde")]
    pub sha256: MerkleHash,
    pub _unused: [u64; 2],
}

impl FileMetadataExt {
    pub fn new(sha256: MerkleHash) -> Self {
        Self {
            sha256,
            _unused: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];

        {
            let mut writer = Cursor::new(&mut buf[..]);
            write_hash(&mut writer, &self.sha256)?;
            write_u64s(&mut writer, &self._unused)?;
        }

        writer.write_all(&buf)?;

        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;

        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            sha256: read_hash(reader)?,
            _unused: Default::default(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct MDBFileInfo {
    pub metadata: FileDataSequenceHeader,
    pub segments: Vec<FileDataSequenceEntry>,
    pub verification: Vec<FileVerificationEntry>,
    pub metadata_ext: Option<FileMetadataExt>,
}

impl MDBFileInfo {
    pub fn num_bytes(&self) -> u64 {
        size_of::<FileDataSequenceHeader>() as u64
            + self.metadata.num_info_entry_following() as u64 * MDB_FILE_INFO_ENTRY_SIZE as u64
    }

    /// The size of the file if unpacked.
    pub fn file_size(&self) -> u64 {
        self.segments.iter().map(|fse| fse.unpacked_segment_bytes as u64).sum()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        if self.contains_verification() {
            debug_assert!(self.segments.len() == self.verification.len());
        }

        let mut bytes_written = 0;

        bytes_written += self.metadata.serialize(writer)?;

        for file_segment in self.segments.iter() {
            bytes_written += file_segment.serialize(writer)?;
        }

        if self.contains_verification() {
            for verification in self.verification.iter() {
                bytes_written += verification.serialize(writer)?;
            }
        }
        if let Some(metadata_ext) = self.metadata_ext.as_ref() {
            bytes_written += metadata_ext.serialize(writer)?;
        }

        Ok(bytes_written)
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Option<Self>, std::io::Error> {
        let metadata = FileDataSequenceHeader::deserialize(reader)?;

        // This is the single bookend entry as a guard for sequential reading.
        if metadata.is_bookend() {
            return Ok(None);
        }

        let num_entries = metadata.num_entries as usize;

        let mut segments = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            segments.push(FileDataSequenceEntry::deserialize(reader)?);
        }

        let mut verification = Vec::with_capacity(num_entries);
        if metadata.contains_verification() {
            for _ in 0..num_entries {
                verification.push(FileVerificationEntry::deserialize(reader)?);
            }
        }
        let metadata_ext = metadata
            .contains_metadata_ext()
            .then(|| FileMetadataExt::deserialize(reader))
            .transpose()?;

        Ok(Some(Self {
            metadata,
            segments,
            verification,
            metadata_ext,
        }))
    }

    pub fn contains_verification(&self) -> bool {
        self.metadata.contains_verification()
    }

    pub fn contains_metadata_ext(&self) -> bool {
        self.metadata.contains_metadata_ext()
    }

    /// Merges the content of other into the content of self if needed.
    /// After this call, self will have the verification info and metadata
    /// extension if they exist in the other object but not this one.
    pub fn merge_from(&mut self, other: &Self) -> Result<(), MDBShardError> {
        FileDataSequenceHeader::verify_same_file(&self.metadata, &other.metadata);
        if self.contains_verification() != other.contains_verification() && other.contains_verification() {
            // self doesn't have verification. Copy from other
            self.metadata.file_flags |= MDB_FILE_FLAG_WITH_VERIFICATION;
            self.verification.clone_from(&other.verification);
        }
        if self.contains_metadata_ext() != other.contains_metadata_ext() && other.contains_metadata_ext() {
            // self doesn't have metadata extension. Copy from other
            self.metadata.file_flags |= MDB_FILE_FLAG_WITH_METADATA_EXT;
            self.metadata_ext.clone_from(&other.metadata_ext);
        }
        Ok(())
    }
}

pub struct MDBFileInfoView {
    header: FileDataSequenceHeader,
    data: Arc<[u8]>, // reference counted read-only vector
    offset: usize,
}

impl MDBFileInfoView {
    /// Creates a new view of an MDBFileInfo object from a slice, checking it
    /// for the correct bounds.  Copies should be optimized out.
    pub fn new(data: Arc<[u8]>, offset: usize) -> std::io::Result<Self> {
        let header = FileDataSequenceHeader::deserialize(&mut Cursor::new(&data[offset..]))?;
        Self::from_data_and_header(header, data, offset)
    }

    pub fn from_data_and_header(
        header: FileDataSequenceHeader,
        data: Arc<[u8]>,
        offset: usize,
    ) -> std::io::Result<Self> {
        // Verify the correct number of entries
        let n = header.num_entries as usize;
        let contains_verification = header.contains_verification();
        let contains_metadata_ext = header.contains_metadata_ext();

        let n_structs = 1 + n // The header and the followup entries 
            + (if contains_verification { n } else { 0 }) // verification entries 
            + (if contains_metadata_ext { 1 } else { 0 });

        if data.len() < offset + n_structs * MDB_FILE_INFO_ENTRY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Provided slice too small to read MDBFileInfoView",
            ));
        }

        Ok(Self { header, data, offset })
    }
    pub fn header(&self) -> &FileDataSequenceHeader {
        &self.header
    }

    #[inline]
    pub fn num_entries(&self) -> usize {
        self.header.num_entries as usize
    }

    #[inline]
    pub fn file_hash(&self) -> MerkleHash {
        self.header.file_hash
    }

    #[inline]
    pub fn file_flags(&self) -> u32 {
        self.header.file_flags
    }

    #[inline]
    pub fn contains_metadata_ext(&self) -> bool {
        self.header.contains_metadata_ext()
    }

    #[inline]
    pub fn contains_verification(&self) -> bool {
        self.header.contains_verification()
    }

    #[inline]
    pub fn entry(&self, idx: usize) -> FileDataSequenceEntry {
        debug_assert!(idx < self.num_entries());

        FileDataSequenceEntry::deserialize(&mut Cursor::new(
            &self.data[(self.offset + (1 + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
        ))
        .expect("bookkeeping error on data bounds for entry")
    }

    #[inline]
    pub fn verification(&self, idx: usize) -> FileVerificationEntry {
        debug_assert!(self.contains_verification());
        debug_assert!(idx < self.num_entries());

        FileVerificationEntry::deserialize(&mut Cursor::new(
            &self.data[(self.offset + (1 + self.num_entries() + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
        ))
        .expect("bookkeeping error on data bounds for verification")
    }

    pub fn byte_size(&self) -> usize {
        let n = self.num_entries();
        let n_structs = 1 + n // The header and the followup entries 
            + (if self.contains_verification() { n } else { 0 }) // verification entries 
            + (if self.contains_metadata_ext() { 1 } else { 0 });

        n_structs * MDB_FILE_INFO_ENTRY_SIZE
    }

    #[inline]
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let n_bytes = self.byte_size();
        writer.write_all(&self.data[self.offset..(self.offset + n_bytes)])?;
        Ok(n_bytes)
    }
}

#[cfg(test)]
mod tests {
    use itertools::{iproduct, Itertools};
    use rand::prelude::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::shard_file::test_routines::simple_hash;
    use crate::shard_format::test_routines::gen_random_file_info;

    #[test]
    fn test_serde_has_metadata_ext() {
        let seed = 3;
        let mut rng = StdRng::seed_from_u64(seed);
        let file_info = gen_random_file_info(&mut rng, &2, true, true);

        assert!(file_info.metadata_ext.is_some());
        assert_eq!(file_info.metadata.num_info_entry_following(), file_info.metadata.num_entries * 2 + 1);

        let size = file_info.num_bytes();
        let mut buffer = Vec::new();
        let bytes_written = file_info.serialize(&mut buffer).unwrap();
        assert_eq!(bytes_written as u64, size);
        assert_eq!(buffer.len(), bytes_written);

        let new_info = MDBFileInfo::deserialize(&mut &buffer[..]).unwrap().unwrap(); // Result<Option<Self>>
        assert_eq!(file_info, new_info);
    }

    #[test]
    fn test_compare_flags() {
        let hash = simple_hash(42);
        let bool_cases = vec![false, true];
        // get 4 types of flags: 00, 01, 10, 11
        let cases = iproduct!(bool_cases.clone(), bool_cases)
            .map(|(has_validation, has_metadata_ext)| {
                FileDataSequenceHeader::new(hash, 5, has_validation, has_metadata_ext)
            })
            .collect_vec();
        // expected results from comparing these
        let expected = vec![
            SupersetResult::Equal,   // 00, 00
            SupersetResult::SuperB,  // 00, 01
            SupersetResult::SuperB,  // 00, 10
            SupersetResult::SuperB,  // 00, 11
            SupersetResult::SuperA,  // 01, 00
            SupersetResult::Equal,   // 01, 01
            SupersetResult::Neither, // 01, 10
            SupersetResult::SuperB,  // 01, 11
            SupersetResult::SuperA,  // 10, 00
            SupersetResult::Neither, // 10, 01
            SupersetResult::Equal,   // 10, 10
            SupersetResult::SuperB,  // 10, 11
            SupersetResult::SuperA,  // 11, 00
            SupersetResult::SuperA,  // 11, 01
            SupersetResult::SuperA,  // 11, 10
            SupersetResult::Equal,   // 11, 11
        ];

        let results = cases
            .iter()
            .flat_map(|a| cases.iter().map(|b| FileDataSequenceHeader::compare_flag_superset(a, b)))
            .collect_vec();

        assert_eq!(expected, results);
    }
}
