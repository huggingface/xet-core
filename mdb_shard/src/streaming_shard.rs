use std::io::{copy, Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;
use futures::AsyncRead;
use futures_util::io::AsyncReadExt;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfoView};
use crate::error::Result;
use crate::file_structs::{FileDataSequenceHeader, MDBFileInfoView};
use crate::shard_file::{MDB_FILE_INFO_ENTRY_SIZE, MDB_SHARD_FOOTER_VERSION};
use crate::{MDBShardFileFooter, MDBShardFileHeader};

/// Iterate through a shard in a streaming manner, calling a callback on each file object and each cas object.
pub fn process_shard_stream<R: Read, FileFunc, CasFunc>(
    reader: &mut R,
    file_callback: Option<FileFunc>,
    cas_callback: Option<CasFunc>,
) -> Result<()>
where
    FileFunc: FnMut(MDBFileInfoView) -> Result<()>,
    CasFunc: FnMut(MDBCASInfoView) -> Result<()>,
{
    // Check the header; not needed except for version verification.
    let _ = MDBShardFileHeader::deserialize(reader)?;

    if let Some(callback) = file_callback {
        process_shard_file_info_section(reader, callback)?;
    } else {
        process_shard_file_info_section(reader, |_| Ok(()))?;
    }

    // Now process through all the cas objects if needed.
    if let Some(callback) = cas_callback {
        process_shard_cas_info_section(reader, callback)?;
    }
    Ok(())
}

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

/// Iterate through a shard in a streaming manner, calling a callback on each file object and each cas object.
pub async fn process_shard_stream_async<R: AsyncRead + Unpin, FileFunc, CasFunc>(
    reader: &mut R,
    file_callback: Option<FileFunc>,
    cas_callback: Option<CasFunc>,
) -> Result<()>
where
    FileFunc: FnMut(MDBFileInfoView) -> Result<()>,
    CasFunc: FnMut(MDBCASInfoView) -> Result<()>,
{
    // Check the header; not needed except for version verification.
    let mut buf = [0u8; size_of::<MDBShardFileHeader>()];
    reader.read_exact(&mut buf[..]).await?;
    let _ = MDBShardFileHeader::deserialize(&mut Cursor::new(&buf))?;

    if let Some(callback) = file_callback {
        process_shard_file_info_section_async(reader, callback).await?;
    } else {
        process_shard_file_info_section_async(reader, |_| Ok(())).await?;
    }

    // Now process through all the cas objects if needed.
    if let Some(callback) = cas_callback {
        process_shard_cas_info_section_async(reader, callback).await?;
    }
    Ok(())
}

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

// TODO: Add callbacks to add verifications

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

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
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
            bytes += file_info.serialize(writer)?;
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
        MDBShardFileFooter {
            file_info_offset: fs_start,
            cas_info_offset: cs_start,
            file_lookup_offset: footer_start,
            file_lookup_num_entry: 0,
            cas_lookup_offset: footer_start,
            cas_lookup_num_entry: 0,
            chunk_lookup_offset: footer_start,
            chunk_lookup_num_entry: 0,
            shard_creation_timestamp: 0,
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
        // Now verify that the serialized version is the same too.
        let mut reloaded_shard = Vec::new();
        min_shard.serialize(&mut reloaded_shard).unwrap();

        let si = MDBShardInfo::load_from_reader(&mut Cursor::new(&reloaded_shard)).unwrap();

        let file_info: Vec<MDBFileInfo> = si.read_all_file_info_sections(&mut Cursor::new(&reloaded_shard)).unwrap();
        let mem_file_info: Vec<_> = mem_shard.file_content.clone().into_values().collect();
        assert_eq!(file_info, mem_file_info);

        let cas_info: Vec<MDBCASInfo> = si.read_all_cas_blocks_full(&mut Cursor::new(&reloaded_shard)).unwrap();
        let mem_cas_info: Vec<_> = mem_shard.cas_content.clone().into_values().collect();

        assert_eq!(cas_info.len(), mem_cas_info.len());

        for i in 0..cas_info.len() {
            assert_eq!(&cas_info[i], mem_cas_info[i].as_ref());
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
