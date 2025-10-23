use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write, copy};
use std::mem::size_of;

use bytes::Bytes;
use futures::AsyncRead;
use futures_util::io::AsyncReadExt;
use itertools::Itertools;
use merklehash::MerkleHash;
use more_asserts::debug_assert_lt;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfoView};
use crate::error::{MDBShardError, Result};
use crate::file_structs::{FileDataSequenceHeader, MDBFileInfoView};
use crate::shard_file::{MDB_FILE_INFO_ENTRY_SIZE, current_timestamp};
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

    /// Return a lookup of xorb hash to starting chunk indices for all the files present in the
    /// shard.  These are the chunks that are useful for global dedup.
    fn file_start_entries(&self) -> HashMap<MerkleHash, Vec<usize>> {
        let mut file_start_entries = HashMap::<MerkleHash, Vec<usize>>::new();

        for f_idx in 0..self.num_files() {
            let Some(fv) = self.file(f_idx) else {
                break;
            };

            if fv.num_entries() > 0 {
                let entry = fv.entry(0);
                let cas_hash = entry.cas_hash;
                let idx = entry.chunk_index_start;

                file_start_entries.entry(cas_hash).or_default().push(idx as usize);
            }
        }

        // Sort all the individual entries.
        for v in file_start_entries.values_mut() {
            v.sort_unstable();
            v.dedup();
        }

        file_start_entries
    }

    /// Implementation for the xorb serialization function.  Use one of the methods below
    /// to directly access this.
    fn serialize_impl<W: Write>(
        &self,
        writer: &mut W,
        with_file_section: bool,
        with_verification: bool,
        xorb_filter_fn: impl Fn(&MDBCASInfoView) -> bool,
    ) -> Result<usize> {
        let mut bytes = 0;

        bytes += MDBShardFileHeader::default().serialize(writer)?;

        // Now, to serialize this correctly, we need to go through and calculate all the stored information
        // as given in the file and cas section
        let mut stored_bytes_on_disk = 0;
        let mut stored_bytes = 0;
        let mut materialized_bytes = 0;

        // When adding in the global dedup flags based on the files present in the shard, we first need to get
        // a lookup of which chunks occur at the start of a file.  These are the ones for which we set the
        // global dedup eligibility flag.
        //
        // In addition, we propegate the global dedup eligibility flag if it is already present.
        //
        let file_start_chunks = self.file_start_entries();

        let fs_start = bytes as u64;

        if with_file_section {
            for file_info in &self.file_info_views {
                for j in 0..file_info.num_entries() {
                    let segment_info = file_info.entry(j);
                    materialized_bytes += segment_info.unpacked_segment_bytes as u64;
                }
                bytes += file_info.serialize(writer, with_verification)?;
            }
        }
        bytes += FileDataSequenceHeader::bookend().serialize(writer)?;

        let cs_start = bytes as u64;
        for cas_info in &self.cas_info_views {
            // Skip any filtered sections.
            if !xorb_filter_fn(cas_info) {
                continue;
            }

            stored_bytes_on_disk += cas_info.header().num_bytes_on_disk as u64;
            stored_bytes += cas_info.header().num_bytes_in_cas as u64;

            if let Some(gde_indices) = file_start_chunks.get(&cas_info.cas_hash()) {
                bytes += cas_info.header().serialize(writer)?;

                debug_assert!(!gde_indices.is_empty());
                debug_assert!(gde_indices.is_sorted());

                let mut next_idx = 0;
                let mut next_c_idx = gde_indices[next_idx];

                // Manually serialize out the cas info in order to set the global dedup flags correctly.
                for c_idx in 0..cas_info.num_entries() {
                    let mut chunk = cas_info.chunk(c_idx);
                    if c_idx == next_c_idx {
                        chunk = chunk.with_global_dedup_flag(true);
                        next_idx += 1;
                        next_c_idx = *gde_indices.get(next_idx).unwrap_or(&usize::MAX);
                    }

                    bytes += chunk.serialize(writer)?;
                }
            } else {
                bytes += cas_info.serialize(writer)?;
            }
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

    /// Serialize out a xorb without any of the file information and a subset of xorb data that is given
    /// by the xorb_filter_fn.  Global deduplication chunk information is preserved.
    pub fn serialize_xorb_subset_only<W: Write>(
        &self,
        writer: &mut W,
        xorb_filter_fn: impl Fn(&MDBCASInfoView) -> bool,
    ) -> Result<usize> {
        self.serialize_impl(writer, false, false, xorb_filter_fn)
    }

    /// Serialize out the given xorb, sanitizing and updating the global dedup chunk flags and optionally
    /// dropping the file verification section.
    pub fn serialize<W: Write>(&self, writer: &mut W, with_verification: bool) -> Result<usize> {
        self.serialize_impl(writer, true, with_verification, |_| true)
    }

    /// Returns a list of all the global dedup eligible chunks, as given either by the hash value, file starts, or
    /// the embedded global dedup flags.
    pub fn global_dedup_eligible_chunks(&self) -> Vec<MerkleHash> {
        // We need to get a list of all the chunk hashes that
        //   - References the first chunk of a file, or
        //   - hash_is_global_dedup_eligible(&hash) is true, or
        //   - has the global dedup flag set.

        let mut ret = HashSet::<MerkleHash>::new();

        // To do the file lookup part efficiently, first scan through the files and record
        // a lookup of xorb hash to offset.  Thus when scanning through the xorb definitions,
        // we can easily extract the hashes that match these indices.
        let file_start_entries = self.file_start_entries();

        for cas_idx in 0..self.num_cas() {
            let Some(cas_view) = self.cas(cas_idx) else {
                break;
            };

            let num_entries = cas_view.num_entries();

            if let Some(fse) = file_start_entries.get(&cas_view.cas_hash()) {
                for &c_idx in fse {
                    debug_assert_lt!(c_idx, num_entries);

                    // Check bounds to be safe here to ensure things don't crash in production; would be
                    // an error and fail verification elsewhere.
                    if c_idx < num_entries {
                        let chunk_hash = cas_view.chunk(c_idx).chunk_hash;
                        ret.insert(chunk_hash);
                    }
                }
            }

            for c_idx in 0..num_entries {
                let chunk = cas_view.chunk(c_idx);

                if chunk.is_global_dedup_eligible() {
                    ret.insert(chunk.chunk_hash);
                }
            }
        }

        Vec::from_iter(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::io::Cursor;

    use anyhow::Result;
    use merklehash::MerkleHash;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    use super::MDBMinimalShard;
    use crate::MDBShardInfo;
    use crate::cas_structs::MDBCASInfo;
    use crate::file_structs::MDBFileInfo;
    use crate::shard_file::test_routines::{convert_to_file, gen_random_shard, gen_random_shard_with_cas_references};
    use crate::shard_in_memory::MDBInMemoryShard;

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

            // Test for equality while ignoring the global dedup flag, as this gets modified on reserializing.
            for i in 0..cas_info.len() {
                let c1 = &cas_info[i];
                let c2 = mem_cas_info[i].as_ref();

                assert_eq!(c1.metadata, c2.metadata);

                for (ch1, ch2) in c1.chunks.iter().zip(c2.chunks.iter()) {
                    // Clear the global dedup one on the new serialized version, as it may have been set.
                    let ch1 = ch1.clone().with_global_dedup_flag(false);
                    assert_eq!(&ch1, ch2);
                }
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
    async fn test_shards() -> Result<()> {
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

    async fn verify_minimal_shard_dedup_processing(mem_shard: &MDBInMemoryShard) {
        verify_minimal_shard(mem_shard).await.unwrap();

        // Additionally, verify that the exporting functions work properly.
        let buffer = convert_to_file(mem_shard).unwrap();
        let min_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&buffer), true, true).unwrap();

        // Calculate the global_dedup chunks.
        let ref_global_dedup_chunks: HashSet<_> = min_shard.global_dedup_eligible_chunks().into_iter().collect();

        // Produce a new minimal shard without the file info.
        let mut xorb_only_shard_buffer = Vec::<u8>::new();
        min_shard
            .serialize_xorb_subset_only(&mut xorb_only_shard_buffer, |_| true)
            .unwrap();

        let xorb_only_shard =
            MDBMinimalShard::from_reader(&mut Cursor::new(&xorb_only_shard_buffer), true, true).unwrap();

        let global_dedup_chunks: HashSet<_> = xorb_only_shard.global_dedup_eligible_chunks().into_iter().collect();

        // Now make sure these are the same.
        assert_eq!(ref_global_dedup_chunks, global_dedup_chunks);

        // Now, exclude subsets of the xorbs for testing to make sure that the filtering works properly.
        //
        // We'll do the filtering by excluding the xorbs with index in the given shard list less
        // than a given value in a set.
        //
        // Annoyingly, our test setup allows some duplication between the chunks in the xorbs, so we end up
        // having to account for that in the tests by allowing a chunk to be in multiple xorbs.
        let mut chunk_hashes = HashMap::<MerkleHash, Vec<usize>>::new();
        let mut xorb_map = HashMap::<MerkleHash, usize>::new();

        let mut rng = SmallRng::seed_from_u64(0);

        for xi in 0..min_shard.num_cas() {
            let xorb = min_shard.cas(xi).unwrap();
            let group = rng.random_range(0..=3);

            xorb_map.insert(xorb.cas_hash(), group);
            for ci in 0..xorb.num_entries() {
                let chunk_hash = xorb.chunk(ci).chunk_hash;
                if ref_global_dedup_chunks.contains(&chunk_hash) {
                    chunk_hashes.entry(chunk_hash).or_default().push(group);
                }
            }
        }

        // Exclude xorbs with set index as given above.
        for grp_set_threshhold in 1..4 {
            let xorb_filter_fn = |xh| *xorb_map.get(&xh).unwrap() < grp_set_threshhold;

            // Get the reference set of xorbs.
            let ref_filtered_xorbs: HashSet<MerkleHash> =
                xorb_map.keys().filter(|&&xh| xorb_filter_fn(xh)).cloned().collect();

            let ref_filtered_global_dedup_chunks: HashSet<_> = chunk_hashes
                .iter()
                .filter(|(_, grp_set)| grp_set.iter().any(|&grp| grp < grp_set_threshhold))
                .map(|(&ch, _)| ch)
                .collect();

            let mut xo_subset_shard_buffer = Vec::<u8>::new();
            min_shard
                .serialize_xorb_subset_only(&mut xo_subset_shard_buffer, |xorb| xorb_filter_fn(xorb.cas_hash()))
                .unwrap();

            let xo_subset_shard =
                MDBMinimalShard::from_reader(&mut Cursor::new(&xo_subset_shard_buffer), true, true).unwrap();

            assert_eq!(xo_subset_shard.num_files(), 0);
            assert_eq!(xo_subset_shard.num_cas(), ref_filtered_xorbs.len());

            let xorbs_present: HashSet<_> = (0..xo_subset_shard.num_cas())
                .map(|i| xo_subset_shard.cas(i).unwrap().cas_hash())
                .collect();

            assert_eq!(xorbs_present, ref_filtered_xorbs);

            let xo_global_dedup_chunks: HashSet<_> =
                xo_subset_shard.global_dedup_eligible_chunks().into_iter().collect();

            assert_eq!(ref_filtered_global_dedup_chunks, xo_global_dedup_chunks);
        }
    }

    // Tests to verify that all the shard filtering options are supported.
    #[tokio::test]
    async fn test_shard_processing() {
        let shard = gen_random_shard_with_cas_references(1, &[1], &[1], false, false).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;

        // Tests to make sure the async and non-async match.
        let shard = gen_random_shard_with_cas_references(1, &[2], &[1, 1], false, false).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;

        let shard = gen_random_shard_with_cas_references(1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, false).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;

        let shard = gen_random_shard_with_cas_references(1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, false).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;

        let shard = gen_random_shard_with_cas_references(1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], false, true).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;

        let shard = gen_random_shard_with_cas_references(1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], true, true).unwrap();
        verify_minimal_shard_dedup_processing(&shard).await;
    }
}
