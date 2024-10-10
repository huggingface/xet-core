use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
use crate::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
use merklehash::MerkleHash;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use utils::serialization_utils::*;

pub const MDB_DEFAULT_FILE_FLAG: u32 = 0;
pub const MDB_FILE_FLAG_WITH_VERIFICATION: u32 = 1 << 31;
pub const MDB_FILE_FLAG_VERIFICATION_MASK: u32 = 1 << 31;

/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry and maybe a sequence
/// of FileVerificationEntry, determined by file flags.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileDataSequenceHeader {
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
    ) -> Self
    where
        <I as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            file_hash,
            file_flags: MDB_DEFAULT_FILE_FLAG
                | match contains_verification {
                    true => MDB_FILE_FLAG_WITH_VERIFICATION,
                    false => 0,
                },
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

    pub fn contains_verification(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_VERIFICATION_MASK) != 0
    }

    /// Get the number of info entries following the header in this shard,
    /// this includes "FileDataSequenceEntry"s and "FileVerificationEntry"s.
    pub fn num_info_entry_following(&self) -> u32 {
        if self.contains_verification() {
            self.num_entries * 2
        } else {
            self.num_entries
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileDataSequenceEntry {
    // maps to one or more CAS chunk(s)
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileVerificationEntry {
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBFileInfo {
    pub metadata: FileDataSequenceHeader,
    pub segments: Vec<FileDataSequenceEntry>,
    pub verification: Vec<FileVerificationEntry>,
}

impl MDBFileInfo {
    pub fn num_bytes(&self) -> u64 {
        size_of::<FileDataSequenceHeader>() as u64
            + self.metadata.num_info_entry_following() as u64 * MDB_FILE_INFO_ENTRY_SIZE as u64
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

        Ok(Some(Self {
            metadata,
            segments,
            verification,
        }))
    }

    pub fn contains_verification(&self) -> bool {
        self.metadata.contains_verification()
    }
}
