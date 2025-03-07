use std::fmt::Debug;
use std::io::{self, Cursor, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

use merklehash::MerkleHash;
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
    pub num_bytes_on_disk: u32,
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
            num_bytes_on_disk: 0,
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
        (size_of::<CASChunkSequenceHeader>() + self.chunks.len() * size_of::<CASChunkSequenceEntry>()) as u64
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

    pub fn chunks_and_boundaries(&self) -> Vec<(MerkleHash, u32)> {
        self.chunks
            .iter()
            .map(|entry| (entry.chunk_hash, entry.chunk_byte_range_start + entry.unpacked_segment_bytes))
            .collect()
    }
}

pub struct MDBCASInfoView {
    header: CASChunkSequenceHeader,
    data: Arc<[u8]>, // reference counted read-only vector
    offset: usize,
}

impl MDBCASInfoView {
    pub fn new(data: Arc<[u8]>, offset: usize) -> io::Result<Self> {
        let mut reader = std::io::Cursor::new(&data[offset..]);
        let header = CASChunkSequenceHeader::deserialize(&mut reader)?;

        Self::from_data_and_header(header, data, offset)
    }

    pub fn from_data_and_header(header: CASChunkSequenceHeader, data: Arc<[u8]>, offset: usize) -> io::Result<Self> {
        let n = header.num_entries as usize;

        let n_bytes = size_of::<CASChunkSequenceHeader>() + n * size_of::<CASChunkSequenceEntry>();

        if data.len() < offset + n_bytes {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Provided slice too small to read Cas Info"));
        }

        Ok(Self { header, data, offset })
    }

    pub fn header(&self) -> &CASChunkSequenceHeader {
        &self.header
    }

    pub fn cas_hash(&self) -> MerkleHash {
        self.header.cas_hash
    }

    pub fn num_entries(&self) -> usize {
        self.header.num_entries as usize
    }

    pub fn chunk(&self, idx: usize) -> CASChunkSequenceEntry {
        debug_assert!(idx < self.num_entries());

        CASChunkSequenceEntry::deserialize(&mut Cursor::new(
            &self.data
                [(self.offset + size_of::<CASChunkSequenceHeader>() + idx * size_of::<CASChunkSequenceEntry>())..],
        ))
        .expect("bookkeeping error on data bounds")
    }

    #[inline]
    pub fn byte_size(&self) -> usize {
        let n = self.num_entries();
        size_of::<CASChunkSequenceHeader>() + n * size_of::<CASChunkSequenceEntry>()
    }

    #[inline]
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let n_bytes = self.byte_size();
        writer.write_all(&self.data[self.offset..(self.offset + n_bytes)])?;
        Ok(n_bytes)
    }
}
