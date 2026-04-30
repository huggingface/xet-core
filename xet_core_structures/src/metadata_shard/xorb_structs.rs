use std::io::{Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;

use super::hash_is_global_dedup_eligible;
use crate::merklehash::MerkleHash;
use crate::serialization_utils::*;

pub const MDB_DEFAULT_XORB_FLAG: u32 = 0;

pub const MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG: u32 = 1 << 31;

/// Each XORB consists of a XorbChunkSequenceHeader following
/// a sequence of XorbChunkSequenceEntry.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct XorbChunkSequenceHeader {
    pub xorb_hash: MerkleHash,
    pub xorb_flags: u32,
    pub num_entries: u32,
    pub num_bytes_in_xorb: u32,
    pub num_bytes_on_disk: u32,
}

impl XorbChunkSequenceHeader {
    pub fn new<I1: TryInto<u32>, I2: TryInto<u32> + Copy>(
        xorb_hash: MerkleHash,
        num_entries: I1,
        num_bytes_in_xorb: I2,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
        <I2 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            xorb_hash,
            xorb_flags: MDB_DEFAULT_XORB_FLAG,
            num_entries: num_entries.try_into().unwrap(),
            num_bytes_in_xorb: num_bytes_in_xorb.try_into().unwrap(),
            num_bytes_on_disk: 0,
        }
    }

    pub fn bookend() -> Self {
        Self {
            // Use all 1s to denote a bookend hash.
            xorb_hash: [!0u64; 4].into(),
            ..Default::default()
        }
    }

    pub fn is_bookend(&self) -> bool {
        self.xorb_hash == [!0u64; 4].into()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.xorb_hash)?;
            write_u32(writer, self.xorb_flags)?;
            write_u32(writer, self.num_entries)?;
            write_u32(writer, self.num_bytes_in_xorb)?;
            write_u32(writer, self.num_bytes_on_disk)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            xorb_hash: read_hash(reader)?,
            xorb_flags: read_u32(reader)?,
            num_entries: read_u32(reader)?,
            num_bytes_in_xorb: read_u32(reader)?,
            num_bytes_on_disk: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct XorbChunkSequenceEntry {
    pub chunk_hash: MerkleHash,
    pub chunk_byte_range_start: u32,
    pub unpacked_segment_bytes: u32,
    pub flags: u32,
    pub _unused: u32,
}

impl XorbChunkSequenceEntry {
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
            flags: 0,
            _unused: 0,
        }
    }

    /// Mark this chunk as a candidate for population in the global dedup table.
    pub fn with_global_dedup_flag(self, is_global_dedup_chunk: bool) -> Self {
        if is_global_dedup_chunk {
            Self {
                flags: self.flags | MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG,
                ..self
            }
        } else {
            Self {
                flags: self.flags & !MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG,
                ..self
            }
        }
    }

    // Is this chunk eligible for a global dedup query?
    pub fn is_global_dedup_eligible(&self) -> bool {
        (self.flags & MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG) != 0 || hash_is_global_dedup_eligible(&self.chunk_hash)
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.chunk_hash)?;
            write_u32(writer, self.chunk_byte_range_start)?;
            write_u32(writer, self.unpacked_segment_bytes)?;
            write_u32(writer, self.flags)?;
            write_u32(writer, self._unused)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<XorbChunkSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            chunk_hash: read_hash(reader)?,
            chunk_byte_range_start: read_u32(reader)?,
            unpacked_segment_bytes: read_u32(reader)?,
            flags: read_u32(reader)?,
            _unused: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBXorbInfo {
    pub metadata: XorbChunkSequenceHeader,
    pub chunks: Vec<XorbChunkSequenceEntry>,
}

impl MDBXorbInfo {
    pub fn num_bytes(&self) -> u64 {
        (size_of::<XorbChunkSequenceHeader>() + self.chunks.len() * size_of::<XorbChunkSequenceEntry>()) as u64
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Option<Self>, std::io::Error> {
        let metadata = XorbChunkSequenceHeader::deserialize(reader)?;

        // This is the single bookend entry as a guard for sequential reading.
        if metadata.is_bookend() {
            return Ok(None);
        }

        let mut chunks = Vec::with_capacity(metadata.num_entries as usize);
        for _ in 0..metadata.num_entries {
            chunks.push(XorbChunkSequenceEntry::deserialize(reader)?);
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

#[derive(Clone, Debug, PartialEq)]
pub struct MDBXorbInfoView {
    header: XorbChunkSequenceHeader,
    data: Bytes, // reference counted read-only vector
}

impl MDBXorbInfoView {
    pub fn new(data: Bytes) -> std::io::Result<Self> {
        let mut reader = Cursor::new(&data);
        let header = XorbChunkSequenceHeader::deserialize(&mut reader)?;

        Self::from_data_and_header(header, data)
    }

    pub fn from_data_and_header(header: XorbChunkSequenceHeader, data: Bytes) -> std::io::Result<Self> {
        let n = header.num_entries as usize;

        let n_bytes = size_of::<XorbChunkSequenceHeader>() + n * size_of::<XorbChunkSequenceEntry>();

        if data.len() < n_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Provided slice too small to read Xorb Info",
            ));
        }

        Ok(Self { header, data })
    }

    pub fn header(&self) -> &XorbChunkSequenceHeader {
        &self.header
    }

    pub fn xorb_hash(&self) -> MerkleHash {
        self.header.xorb_hash
    }

    pub fn num_entries(&self) -> usize {
        self.header.num_entries as usize
    }

    pub fn chunk(&self, idx: usize) -> XorbChunkSequenceEntry {
        debug_assert!(idx < self.num_entries());

        XorbChunkSequenceEntry::deserialize(&mut Cursor::new(
            &self.data[(size_of::<XorbChunkSequenceHeader>() + idx * size_of::<XorbChunkSequenceEntry>())..],
        ))
        .expect("bookkeeping error on data bounds")
    }

    #[inline]
    pub fn byte_size(&self) -> usize {
        let n = self.num_entries();
        size_of::<XorbChunkSequenceHeader>() + n * size_of::<XorbChunkSequenceEntry>()
    }

    #[inline]
    pub fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        let n_bytes = self.byte_size();
        writer.write_all(&self.data[..n_bytes])?;
        Ok(n_bytes)
    }

    #[inline]
    pub fn serialize_with_chunk_rewrite<W: Write>(
        &self,
        writer: &mut W,
        chunk_rewrite_fn: impl Fn(usize, XorbChunkSequenceEntry) -> XorbChunkSequenceEntry,
    ) -> std::io::Result<usize> {
        let mut n_out_bytes = 0;
        n_out_bytes += self.header.serialize(writer)?;

        for idx in 0..self.num_entries() {
            let rewritten_chunk = chunk_rewrite_fn(idx, self.chunk(idx));
            n_out_bytes += rewritten_chunk.serialize(writer)?;
        }

        Ok(n_out_bytes)
    }
}

impl From<&MDBXorbInfoView> for MDBXorbInfo {
    fn from(view: &MDBXorbInfoView) -> Self {
        let chunks: Vec<XorbChunkSequenceEntry> = (0..view.num_entries()).map(|i| view.chunk(i)).collect();
        let total_bytes = chunks
            .last()
            .map(|c| c.chunk_byte_range_start + c.unpacked_segment_bytes)
            .unwrap_or(0);
        MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(view.xorb_hash(), chunks.len() as u32, total_bytes),
            chunks,
        }
    }
}
