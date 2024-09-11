use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
use crate::serialization_utils::*;
use merklehash::MerkleHash;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;

pub const MDB_DEFAULT_FILE_FLAG: u32 = 0;

/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileDataSequenceHeader {
    pub file_hash: MerkleHash,
    pub file_flags: u32,
    pub num_entries: u32,
    pub _unused: u64,
}

impl FileDataSequenceHeader {
    pub fn new<I: TryInto<u32>>(file_hash: MerkleHash, num_entries: I) -> Self
    where
        <I as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            file_hash,
            file_flags: MDB_DEFAULT_FILE_FLAG,
            num_entries: num_entries.try_into().unwrap(),
            #[cfg(test)]
            _unused: 126846135456846514u64,
            #[cfg(not(test))]
            _unused: 0,
        }
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
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileDataSequenceEntry {
    // maps to one or more CAS chunk(s)
    pub cas_hash: MerkleHash,
    pub cas_flags: u32,
    pub unpacked_segment_bytes: u32,
    pub chunk_byte_range_start: u32,
    pub chunk_byte_range_end: u32,
}

impl FileDataSequenceEntry {
    pub fn new<I1: TryInto<u32>, I2: TryInto<u32>>(
        cas_hash: MerkleHash,
        unpacked_segment_bytes: I1,
        chunk_byte_range_start: I2,
        chunk_byte_range_end: I2,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
        <I2 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            cas_hash,
            cas_flags: MDB_DEFAULT_FILE_FLAG,
            unpacked_segment_bytes: unpacked_segment_bytes.try_into().unwrap(),
            chunk_byte_range_start: chunk_byte_range_start.try_into().unwrap(),
            chunk_byte_range_end: chunk_byte_range_end.try_into().unwrap(),
        }
    }

    pub fn from_cas_entries(
        metadata: &CASChunkSequenceHeader,
        chunks: &[CASChunkSequenceEntry],
        chunk_byte_range_end: u32,
    ) -> Self {
        if chunks.is_empty() {
            return Self::default();
        }

        Self {
            cas_hash: metadata.cas_hash,
            cas_flags: metadata.cas_flags,
            unpacked_segment_bytes: chunks.iter().map(|sb| sb.unpacked_segment_bytes).sum(),
            chunk_byte_range_start: chunks[0].chunk_byte_range_start,
            chunk_byte_range_end,
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
            write_u32(writer, self.chunk_byte_range_start)?;
            write_u32(writer, self.chunk_byte_range_end)?;
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
            chunk_byte_range_start: read_u32(reader)?,
            chunk_byte_range_end: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBFileInfo {
    pub metadata: FileDataSequenceHeader,
    pub segments: Vec<FileDataSequenceEntry>,
}

impl MDBFileInfo {
    pub fn num_bytes(&self) -> u64 {
        (size_of::<FileDataSequenceHeader>()
            + self.segments.len() * size_of::<FileDataSequenceEntry>()) as u64
    }
}
