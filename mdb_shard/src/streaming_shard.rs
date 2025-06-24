use std::io::{copy, Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;
use futures::AsyncRead;
use futures_util::io::AsyncReadExt;
use itertools::Itertools;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfoView};
use crate::error::{MDBShardError, Result};
use crate::file_structs::{FileDataSequenceHeader, MDBFileInfoView};
use crate::shard_file::{current_timestamp, MDB_FILE_INFO_ENTRY_SIZE};
use crate::{MDBShardFileFooter, MDBShardFileHeader};

/// Runs through a shard file info section, calling the specified callback function for each entry.
///
/// Assumes that the reader is at the start of the file info section, and on return, the
/// reader will be at the end of the file info section.
pub fn process_shard_file_info_section<R: Read, FileFunc>(reader: &mut R, mut file_callback: FileFunc) -> Result<()>
where
    FileFunc: FnMut(MDBFileInfoView) -> Result<()>,
{
    // Iterate through the file metadata section, calling the file callback function for each one.
    loop {
        let header = FileDataSequenceHeader::deserialize(reader)?;

        if header.is_bookend() {
            break;
        }

        let n = header.num_entries as usize;

        let mut n_entries = n;

        if header.contains_verification() {
            n_entries += n;
        }

        if header.contains_metadata_ext() {
            n_entries += 1;
        }

        let n_bytes = n_entries * MDB_FILE_INFO_ENTRY_SIZE;

        let mut file_data = Vec::with_capacity(size_of::<FileDataSequenceHeader>() + n_bytes);

        header.serialize(&mut file_data)?;
        copy(&mut reader.take(n_bytes as u64), &mut file_data)?;

        file_callback(MDBFileInfoView::from_data_and_header(header, Bytes::from(file_data))?)?;
    }

    Ok(())
}

/// Runs through a shard cas info section and processes each entry, calling the
/// specified callback function for each entry.
///
/// Assumes that the reader is at the start of the cas info section, and on return, the
/// reader will be at the end of the cas info section.
pub fn process_shard_cas_info_section<R: Read, CasFunc>(reader: &mut R, mut cas_callback: CasFunc) -> Result<()>
where
    CasFunc: FnMut(MDBCASInfoView) -> Result<()>,
{
    loop {
        let header = CASChunkSequenceHeader::deserialize(reader)?;

        if header.is_bookend() {
            break;
        }

        let n_bytes = (header.num_entries as usize) * size_of::<CASChunkSequenceEntry>();

        let mut cas_data = Vec::with_capacity(size_of::<CASChunkSequenceHeader>() + n_bytes);

        header.serialize(&mut cas_data)?;
        copy(&mut reader.take(n_bytes as u64), &mut cas_data)?;

        cas_callback(MDBCASInfoView::from_data_and_header(header, Bytes::from(cas_data))?)?;
    }
    Ok(())
}

// Async versions of the above

pub async fn process_shard_file_info_section_async<R: AsyncRead + Unpin, FileFunc>(
    reader: &mut R,
    mut file_callback: FileFunc,
) -> Result<()>
where
    FileFunc: FnMut(MDBFileInfoView) -> Result<()>,
{
    loop {
        // Read header
        let mut header_buf = [0u8; size_of::<FileDataSequenceHeader>()];

        reader.read_exact(&mut header_buf).await?;

        let header = FileDataSequenceHeader::deserialize(&mut Cursor::new(&header_buf[..]))?;
        if header.is_bookend() {
            break;
        }

        let n = header.num_entries as usize;
        let mut n_entries = n;

        if header.contains_verification() {
            n_entries += n;
        }

        if header.contains_metadata_ext() {
            n_entries += 1;
        }

        let n_bytes = n_entries * MDB_FILE_INFO_ENTRY_SIZE;
        let total_len = size_of::<FileDataSequenceHeader>() + n_bytes;

        // Prepare buffer for entire record: header + data
        let mut file_data = Vec::with_capacity(total_len);
        file_data.extend_from_slice(&header_buf); // put header data first
        file_data.resize(total_len, 0); // enlarge to full size

        // Read the remainder of the data
        reader.read_exact(&mut file_data[size_of::<FileDataSequenceHeader>()..]).await?;

        // Call the callback with the assembled view
        file_callback(MDBFileInfoView::from_data_and_header(header, Bytes::from(file_data))?)?;
    }

    Ok(())
}

pub async fn process_shard_cas_info_section_async<R: AsyncRead + Unpin, CasFunc>(
    reader: &mut R,
    mut cas_callback: CasFunc,
) -> Result<()>
where
    CasFunc: FnMut(MDBCASInfoView) -> Result<()>,
{
    loop {
        // Read header
        let mut header_buf = [0u8; size_of::<CASChunkSequenceHeader>()];
        reader.read_exact(&mut header_buf).await?;

        let header = CASChunkSequenceHeader::deserialize(&mut Cursor::new(&header_buf[..]))?;
        if header.is_bookend() {
            break;
        }

        let n_bytes = (header.num_entries as usize) * size_of::<CASChunkSequenceEntry>();
        let total_len = size_of::<CASChunkSequenceHeader>() + n_bytes;

        let mut cas_data = Vec::with_capacity(total_len);
        cas_data.extend_from_slice(&header_buf); // Insert the header we read
        cas_data.resize(total_len, 0);

        // Read the remainder of the CAS chunk data
        reader.read_exact(&mut cas_data[size_of::<CASChunkSequenceHeader>()..]).await?;

        // Invoke callback
        cas_callback(MDBCASInfoView::from_data_and_header(header, Bytes::from(cas_data))?)?;
    }

    Ok(())
}

// A minimal shard loaded in memory that could be useful by themselves.  In addition, this provides a testing surface
// for the above iteration routines.
#[derive(Clone, Debug, PartialEq)]
pub struct MDBMinimalShard {
    file_info_views: Vec<MDBFileInfoView>,
    cas_info_views: Vec<MDBCASInfoView>,
}

impl MDBMinimalShard {
    pub fn from_reader<R: Read>(reader: &mut R, include_files: bool, include_cas: bool) -> Result<Self> {
        // Check the header; not needed except for version verification.
        let _ = MDBShardFileHeader::deserialize(reader)?;

        let mut file_info_views = Vec::<MDBFileInfoView>::new();
        process_shard_file_info_section(reader, |fiv: MDBFileInfoView| {
            // register the offset here to the file entries
            if include_files {
                file_info_views.push(fiv);
            }
            Ok(())
        })?;

        let mut cas_info_views = Vec::<MDBCASInfoView>::new();
        if include_cas {
            process_shard_cas_info_section(reader, |civ: MDBCASInfoView| {
                cas_info_views.push(civ);
                Ok(())
            })?;
        }

        Ok(Self {
            file_info_views,
            cas_info_views,
        })
    }

    pub async fn from_reader_async<R: AsyncRead + Unpin>(
        reader: &mut R,
        include_files: bool,
        include_cas: bool,
    ) -> Result<Self> {
        Self::from_reader_async_with_custom_callbacks(reader, include_files, include_cas, |_| Ok(()), |_| Ok(())).await
    }

    pub async fn from_reader_async_with_custom_callbacks<R: AsyncRead + Unpin, FileFunc, CasFunc>(
        reader: &mut R,
        include_files: bool,
        include_cas: bool,
        mut file_callback: FileFunc,
        mut cas_callback: CasFunc,
    ) -> Result<Self>
    where
        FileFunc: FnMut(&MDBFileInfoView) -> Result<()>,
        CasFunc: FnMut(&MDBCASInfoView) -> Result<()>,
    {
        // Check the header; not needed except for version verification.
        let mut buf = [0u8; size_of::<MDBShardFileHeader>()];
        reader.read_exact(&mut buf[..]).await?;
        let _ = MDBShardFileHeader::deserialize(&mut Cursor::new(&buf))?;

        let mut file_info_views = Vec::<MDBFileInfoView>::new();
        process_shard_file_info_section_async(reader, |fiv: MDBFileInfoView| {
            // register the offset here to the file entries
            if include_files {
                file_callback(&fiv)?;
                file_info_views.push(fiv);
            }
            Ok(())
        })
        .await?;
        // if only some files have verification, then we consider this shard invalid
        // either all files have verification or no files have verification
        if !file_info_views.is_empty() && !file_info_views.iter().map(|fiv| fiv.contains_verification()).all_equal() {
            return Err(MDBShardError::invalid_shard("only some files contain verification"));
        }

        // CAS stuff
        let mut cas_info_views = Vec::<MDBCASInfoView>::new();
        if include_cas {
            process_shard_cas_info_section_async(reader, |civ: MDBCASInfoView| {
                cas_callback(&civ)?;
                cas_info_views.push(civ);
                Ok(())
            })
            .await?;
        }

        Ok(Self {
            file_info_views,
            cas_info_views,
        })
    }

    pub fn has_file_verification(&self) -> bool {
        let Some(file_info_view) = self.file_info_views.first() else {
            return false;
        };
        file_info_view.contains_verification()
    }

    pub fn num_files(&self) -> usize {
        self.file_info_views.len()
    }

    pub fn file(&self, index: usize) -> Option<&MDBFileInfoView> {
        self.file_info_views.get(index)
    }

    pub fn num_cas(&self) -> usize {
        self.cas_info_views.len()
    }

    pub fn cas(&self, index: usize) -> Option<&MDBCASInfoView> {
        self.cas_info_views.get(index)
    }

    // returns 0 if with_verification is true but the shard has no verification information.
    pub fn serialized_size(&self, with_verification: bool) -> usize {
        if with_verification && !self.has_file_verification() {
            return 0;
        }
        size_of::<MDBShardFileHeader>()
            + self
                .file_info_views
                .iter()
                .fold(0, |acc, fiv| acc + fiv.byte_size(with_verification))
            + size_of::<FileDataSequenceHeader>() // bookend of file section
            + self.cas_info_views.iter().fold(0, |acc, civ| acc + civ.byte_size())
            + size_of::<CASChunkSequenceHeader>() // bookend for cas info section
            + size_of::<MDBShardFileFooter>()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W, with_verification: bool) -> Result<usize> {
        let mut bytes = 0;

        bytes += MDBShardFileHeader::default().serialize(writer)?;

        // Now, to serialize this correctly, we need to go through and calculate all the stored information
        // as given in the file and cas section
        let mut stored_bytes_on_disk = 0;
        let mut stored_bytes = 0;
        let mut materialized_bytes = 0;

        let fs_start = bytes as u64;
        for file_info in &self.file_info_views {
            for j in 0..file_info.num_entries() {
                let segment_info = file_info.entry(j);
                materialized_bytes += segment_info.unpacked_segment_bytes as u64;
            }
            bytes += file_info.serialize(writer, with_verification)?;
        }
        bytes += FileDataSequenceHeader::bookend().serialize(writer)?;

        let cs_start = bytes as u64;
        for cas_info in &self.cas_info_views {
            stored_bytes_on_disk += cas_info.header().num_bytes_on_disk as u64;
            stored_bytes += cas_info.header().num_bytes_in_cas as u64;

            bytes += cas_info.serialize(writer)?;
        }
        bytes += CASChunkSequenceHeader::bookend().serialize(writer)?;

        let footer_start = bytes as u64;

        // Now fill out the footer and write it out.
        bytes += MDBShardFileFooter {
            file_info_offset: fs_start,
            cas_info_offset: cs_start,
            file_lookup_offset: footer_start,
            file_lookup_num_entry: 0,
            cas_lookup_offset: footer_start,
            cas_lookup_num_entry: 0,
            chunk_lookup_offset: footer_start,
            chunk_lookup_num_entry: 0,
            shard_creation_timestamp: current_timestamp(),
            shard_key_expiry: 0,
            stored_bytes_on_disk,
            materialized_bytes,
            stored_bytes,
            footer_offset: footer_start,
            ..Default::default()
        }
        .serialize(writer)?;

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use anyhow::Result;

    use super::MDBMinimalShard;
    use crate::cas_structs::MDBCASInfo;
    use crate::file_structs::MDBFileInfo;
    use crate::shard_file::test_routines::{convert_to_file, gen_random_shard};
    use crate::shard_in_memory::MDBInMemoryShard;
    use crate::MDBShardInfo;

    fn verify_serialization(min_shard: &MDBMinimalShard, mem_shard: &MDBInMemoryShard) -> Result<()> {
        for verification in [true, false] {
            // compute size, with verification if possible only
            let size = min_shard.serialized_size(min_shard.has_file_verification() && verification);
            assert_ne!(0, size);

            // if lacking verification, assert that getting the size with verification returns 0
            if !min_shard.has_file_verification() {
                assert_eq!(0, min_shard.serialized_size(true))
            }

            // Now verify that the serialized version is the same too.
            let mut reloaded_shard = Vec::new();
            let serialize_result = min_shard.serialize(&mut reloaded_shard, verification);
            if !min_shard.has_file_verification() && verification && min_shard.num_files() > 0 {
                assert!(serialize_result.is_err());
                continue;
            }
            assert!(serialize_result.is_ok());
            let serialized_len = serialize_result?;
            assert_eq!(reloaded_shard.len(), serialized_len);
            assert_eq!(size, serialized_len);

            let si = MDBShardInfo::load_from_reader(&mut Cursor::new(&reloaded_shard)).unwrap();

            let file_info: Vec<MDBFileInfo> =
                si.read_all_file_info_sections(&mut Cursor::new(&reloaded_shard)).unwrap();
            let mem_file_info: Vec<_> = mem_shard.file_content.clone().into_values().collect();

            for (i, (read, mem)) in file_info.iter().zip(mem_file_info.iter()).enumerate() {
                assert!(read.equal_accepting_no_verification(mem), "i: {i} verification = {verification}");
            }

            let cas_info: Vec<MDBCASInfo> = si.read_all_cas_blocks_full(&mut Cursor::new(&reloaded_shard)).unwrap();
            let mem_cas_info: Vec<_> = mem_shard.cas_content.clone().into_values().collect();

            assert_eq!(cas_info.len(), mem_cas_info.len(), "verification = {verification}");

            for i in 0..cas_info.len() {
                assert_eq!(&cas_info[i], mem_cas_info[i].as_ref(), "verification = {verification}");
            }
        }

        Ok(())
    }

    async fn verify_minimal_shard(mem_shard: &MDBInMemoryShard) -> Result<()> {
        let buffer = convert_to_file(mem_shard)?;

        {
            let min_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&buffer), true, true).unwrap();
            let min_shard_async = MDBMinimalShard::from_reader_async(&mut &buffer[..], true, true).await.unwrap();

            assert_eq!(min_shard, min_shard_async);

            verify_serialization(&min_shard, mem_shard).unwrap();
        }

        {
            // Test we're good on the ones without cas entries.
            let min_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&buffer), true, false).unwrap();
            let min_shard_async = MDBMinimalShard::from_reader_async(&mut &buffer[..], true, false).await.unwrap();

            assert_eq!(min_shard, min_shard_async);

            let mut file_only_memshard = mem_shard.clone();
            file_only_memshard.cas_content.clear();
            file_only_memshard.chunk_hash_lookup.clear();

            verify_serialization(&min_shard, &file_only_memshard).unwrap();
        }

        // Test we're good on the ones without file entries.
        {
            let min_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&buffer), false, true).unwrap();
            let min_shard_async = MDBMinimalShard::from_reader_async(&mut &buffer[..], false, true).await.unwrap();

            assert_eq!(min_shard, min_shard_async);

            let mut cas_only_memshard = mem_shard.clone();
            cas_only_memshard.file_content.clear();

            verify_serialization(&min_shard, &cas_only_memshard).unwrap();
        }

        // Test custom callbacks
        {
            let mut file_info_views = vec![];
            let mut cas_info_views = vec![];

            let min_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&buffer), true, true).unwrap();
            let min_shard_async = MDBMinimalShard::from_reader_async_with_custom_callbacks(
                &mut &buffer[..],
                true,
                true,
                |f| {
                    file_info_views.push(f.clone());
                    Ok(())
                },
                |c| {
                    cas_info_views.push(c.clone());
                    Ok(())
                },
            )
            .await
            .unwrap();

            assert_eq!(min_shard, min_shard_async);
            assert_eq!(file_info_views, min_shard.file_info_views);
            assert_eq!(cas_info_views, min_shard.cas_info_views);

            let mut cas_only_memshard = mem_shard.clone();
            cas_only_memshard.file_content.clear();

            verify_serialization(&min_shard, mem_shard).unwrap();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_shards() -> Result<()> {
        let shard = gen_random_shard(0, &[], &[0], false, false)?;
        verify_minimal_shard(&shard).await?;

        // Tests to make sure the async and non-async match.
        let shard = gen_random_shard(0, &[1], &[1, 1], false, false)?;
        verify_minimal_shard(&shard).await?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, false)?;
        verify_minimal_shard(&shard).await?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, false)?;
        verify_minimal_shard(&shard).await?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, true)?;
        verify_minimal_shard(&shard).await?;

        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, true)?;
        verify_minimal_shard(&shard).await?;

        Ok(())
    }
}
