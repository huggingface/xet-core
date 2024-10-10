use merklehash::MerkleHash;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::mem::size_of;
use utils::serialization_utils::*;

pub const MDB_DEFAULT_CAS_FLAG: u32 = 0;

/// Each CAS consists of a CASChunkSequenceHeader following
/// a sequence of CASChunkSequenceEntry.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CASChunkSequenceHeader {
    pub cas_hash: MerkleHash,
    pub cas_flags: u32,
    pub num_entries: u32,
    pub num_bytes_in_cas: u32,
    pub num_bytes_on_disk: u32, // the size after CAS block compression
}

impl CASChunkSequenceHeader {
    pub fn new<I1: TryInto<u32>, I2: TryInto<u32> + Copy>(
        cas_hash: MerkleHash,
        num_entries: I1,
        num_bytes_in_cas: I2,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
        <I2 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            cas_hash,
            cas_flags: MDB_DEFAULT_CAS_FLAG,
            num_entries: num_entries.try_into().unwrap(),
            num_bytes_in_cas: num_bytes_in_cas.try_into().unwrap(),
            num_bytes_on_disk: num_bytes_in_cas.try_into().unwrap(),
        }
    }

    pub fn bookend() -> Self {
        Self {
            // Use all 1s to denote a bookend hash.
            cas_hash: [!0u64; 4].into(),
            ..Default::default()
        }
    }

    pub fn is_bookend(&self) -> bool {
        self.cas_hash == [!0u64; 4].into()
    }

    pub fn new_with_compression<I1: TryInto<u32>, I2: TryInto<u32> + Copy>(
        cas_hash: MerkleHash,
        num_entries: I1,
        num_bytes_in_cas: I2,
        num_bytes_on_disk: I2,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
        <I2 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            cas_hash,
            cas_flags: MDB_DEFAULT_CAS_FLAG,
            num_entries: num_entries.try_into().unwrap(),
            num_bytes_in_cas: num_bytes_in_cas.try_into().unwrap(),
            num_bytes_on_disk: num_bytes_on_disk.try_into().unwrap(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.cas_hash)?;
            write_u32(writer, self.cas_flags)?;
            write_u32(writer, self.num_entries)?;
            write_u32(writer, self.num_bytes_in_cas)?;
            write_u32(writer, self.num_bytes_on_disk)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = std::io::Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            cas_hash: read_hash(reader)?,
            cas_flags: read_u32(reader)?,
            num_entries: read_u32(reader)?,
            num_bytes_in_cas: read_u32(reader)?,
            num_bytes_on_disk: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CASChunkSequenceEntry {
    pub chunk_hash: MerkleHash,
    pub unpacked_segment_bytes: u32,
    pub chunk_byte_range_start: u32,
    pub _unused: u64,
}

impl CASChunkSequenceEntry {
    pub fn new<I1: TryInto<u32>, I2: TryInto<u32>>(
        chunk_hash: MerkleHash,
        unpacked_segment_bytes: I1,
        chunk_byte_range_start: I2,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
        <I2 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            chunk_hash,
            unpacked_segment_bytes: unpacked_segment_bytes.try_into().unwrap(),
            chunk_byte_range_start: chunk_byte_range_start.try_into().unwrap(),
            #[cfg(test)]
            _unused: 216944691646848u64,
            #[cfg(not(test))]
            _unused: 0,
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.chunk_hash)?;
            write_u32(writer, self.chunk_byte_range_start)?;
            write_u32(writer, self.unpacked_segment_bytes)?;
            write_u64(writer, self._unused)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<CASChunkSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = std::io::Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            chunk_hash: read_hash(reader)?,
            chunk_byte_range_start: read_u32(reader)?,
            unpacked_segment_bytes: read_u32(reader)?,
            _unused: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBCASInfo {
    pub metadata: CASChunkSequenceHeader,
    pub chunks: Vec<CASChunkSequenceEntry>,
}

impl MDBCASInfo {
    pub fn num_bytes(&self) -> u64 {
        (size_of::<CASChunkSequenceHeader>()
            + self.chunks.len() * size_of::<CASChunkSequenceEntry>()) as u64
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Option<Self>, std::io::Error> {
        let metadata = CASChunkSequenceHeader::deserialize(reader)?;

        // This is the single bookend entry as a guard for sequential reading.
        if metadata.is_bookend() {
            return Ok(None);
        }

        let mut chunks = Vec::with_capacity(metadata.num_entries as usize);
        for _ in 0..metadata.num_entries {
            chunks.push(CASChunkSequenceEntry::deserialize(reader)?);
        }

        Ok(Some(Self { metadata, chunks }))
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut n_out_bytes = 0;
        n_out_bytes += self.metadata.serialize(writer)?;

        for chunk in self.chunks.iter() {
            n_out_bytes += chunk.serialize(writer)?;
        }

        Ok(n_out_bytes)
    }
}
