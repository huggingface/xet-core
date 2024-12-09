use std::default;
use std::fmt::Write;
use std::io::{self, copy, Cursor, Read};
use std::sync::Arc;

use futures_io::AsyncRead;
use merklehash::HMACKey;
use tokio::io::AsyncRead;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfoView};
use crate::error::Result;
use crate::file_structs::{FileDataSequenceHeader, MDBFileInfoView};
use crate::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
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

        file_callback(MDBFileInfoView::from_data_and_header(header, Arc::from(file_data), 0)?)?;
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

        cas_callback(MDBCASInfoView::from_data_and_header(header, Arc::from(cas_data), 0)?)?;
    }
    Ok(())
}

// Async versions of the above

/// Iterate through a shard in a streaming manner, calling a callback on each file object and each cas object.
pub async fn process_shard_stream_async<R: ReadAsync, FileFunc, CasFunc>(
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
pub async fn process_shard_file_info_section_async<R: futures_io::AsyncRead + Unpin, FileFunc>(
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
        file_callback(MDBFileInfoView::from_data_and_header(header, Arc::from(file_data), 0)?)?;
    }

    Ok(())
}

pub async fn process_shard_cas_info_section_async<R: futures_io::AsyncRead + Unpin, CasFunc>(
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
        cas_callback(MDBCASInfoView::from_data_and_header(header, Arc::from(cas_data), 0)?)?;
    }

    Ok(())
}

// A minimal shard loaded in memory that could be useful by themselves.  In addition, this provides a testing surface for the above iteration routines.
pub struct MDBMinimalShard {
    data: Arc<[u8]>,
    file_offsets: Vec<u32>,
    cas_offsets: Vec<u32>,

    // Needed for serialization
    cas_info_start: u32,
}

impl MDBMinimalShard {
    pub fn from_reader<R: Read>(reader: &mut R, include_files: bool, include_cas: bool) -> Result<Self> {
        let mut data_vec = Vec::<u8>::new();

        // Check the header; not needed except for version verification.
        let _ = MDBShardFileHeader::deserialize(reader)?;

        let mut file_offsets = Vec::<u32>::new();
        process_shard_file_info_section(reader, |fiv: MDBFileInfoView| {
            // register the offset here to the file entries
            if include_files {
                file_offsets.push(data_vec.len() as u32);
                fiv.serialize(&mut data_vec)?;
            }
            Ok(())
        })?;

        // This always goes in.
        FileDataSequenceHeader::bookend().serialize(&mut data_vec)?;

        // CAS stuff if needed
        let cas_info_start = data_vec.len();
        let mut cas_offsets = Vec::<u32>::new();

        if include_cas {
            process_shard_cas_info_section(reader, |civ: MDBCASInfoView| {
                cas_offsets.push(data_vec.len() as u32);
                civ.serialize(&mut data_vec)?;
                Ok(())
            })?;
        }

        // Aiming for minimal data usage, so shrink things to fit
        data_vec.shrink_to_fit();
        file_offsets.shrink_to_fit();
        cas_offsets.shrink_to_fit();

        Ok(Self {
            data: Arc::from(data_vec),
            file_offsets,
            cas_offsets,
            cas_info_start,
        })
    }

    pub async fn from_reader_async<R: futures_io::AsyncRead + Unpin>(
        reader: &mut R,
        include_files: bool,
        include_cas: bool,
    ) -> Result<Self> {
        let mut data_vec = Vec::<u8>::new();

        // Check the header; not needed except for version verification.
        let _ = MDBShardFileHeader::deserialize(reader)?;

        let mut file_offsets = Vec::<u32>::new();
        process_shard_file_info_section_async(reader, |fiv: MDBFileInfoView| {
            // register the offset here to the file entries
            if include_files {
                file_offsets.push(data_vec.len() as u32);
                fiv.serialize(&mut data_vec)?;
            }
            Ok(())
        })
        .await?;

        // Add in a bookend struct so the shard can be serialized easily if needed.
        FileDataSequenceHeader::bookend().serialize(&mut data_vec)?;

        // CAS stuff
        let cas_info_start = data_vec.len();
        let mut cas_offsets = Vec::<u32>::new();

        if include_cas {
            process_shard_cas_info_section(reader, |civ: MDBCASInfoView| {
                cas_offsets.push(data_vec.len() as u32);
                civ.serialize(&mut data_vec)?;
                Ok(())
            })?;
        }

        // Add in a CAS bookend struct so the shard can be serialized easily if needed.
        let cas_info_start = data_vec.len();
        CASChunkSequenceHeader::bookend().serialize(&mut data_vec)?;

        // Aiming for minimal data usage, so shrink things to fit
        data_vec.shrink_to_fit();
        file_offsets.shrink_to_fit();
        cas_offsets.shrink_to_fit();

        Ok(Self {
            data: Arc::from(data_vec),
            file_offsets,
            cas_offsets,
            cas_info_start,
        })
    }
    pub fn num_files(&self) -> usize {
        self.file_offsets.len()
    }

    pub fn file(&self, index: usize) -> MDBFileInfoView {
        let offset = self.file_offsets[index] as usize;

        // do the equivalent of an unwrap here as we have already verified that
        // the sizes are correct on creation.
        MDBFileInfoView::new(self.data.clone(), offset).expect("Programming error: file info bookkeeping wrong.")
    }

    pub fn num_cas(&self) -> usize {
        self.cas_offsets.len()
    }

    pub fn cas(&self, index: usize) -> MDBCASInfoView {
        let offset = self.cas_offsets[index] as usize;

        // do the equivalent of an unwrap here as we have already verified that
        // the sizes are correct on creation.
        MDBCASInfoView::new(self.data.clone(), offset).expect("Programming error: cas info bookkeeping wrong.")
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let mut bytes = 0;

        bytes += MDBShardFileHeader::default().serialize(writer)?;
        let fs_start = bytes;

        copy(&mut Cursor::new(&self.data), writer)?;
        bytes += self.data.len();

        let footer_start = bytes;

        let mut stored_bytes_on_disk = 0;
        let mut stored_bytes = 0;
        let mut materialized_bytes = 0;

        // Now, to serialize this correctly, we need to go through and calculate all the stored information
        // as given in the file and cas section
        for i in 0..self.num_files() {
            let file_info = self.file(i);
            for j in 0..file_info.num_entries() {
                let segment_info = file_info.entry(j);

                materialized_bytes += segment_info.unpacked_segment_bytes as u64;
            }
        }
        for i in 0..self.num_cas() {
            let cas_info = self.cas(i);
            for j in 0..cas_info.num_entries() {
                let chunk_info = cas_info.chunk(j);

                stored_bytes_on_disk += chunk_info.num_bytes_on_disk as u64;
                stored_bytes += chunk_info.num_bytes_in_cas as u64;
            }
        }

        // Now fill out the footer and write it out.
        bytes += MDBShardFileFooter {
            file_info_offset: fs_start,
            cas_info_offset: self.cas_info_start + size_of::<MDBShardFileHeader>(),
            file_lookup_offset: footer_start,
            file_lookup_num_entry: 0,
            cas_lookup_offset: footer_start,
            cas_lookup_num_entry: 0,
            chunk_lookup_offset: footer_start,
            chunk_lookup_num_entry: 0,
            chunk_hash_hmac_key: HMACKey::default(),
            stored_bytes_on_disk,
            materialized_bytes,
            stored_bytes,
            footer_offset: footer_start,
            ..default::Default()
        }
        .serialize(writer)?;

        Ok(bytes)
    }
}
