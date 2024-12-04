use std::{
    io::{self, copy, Read},
    sync::Arc,
};

use crate::{
    cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader},
    file_structs::{FileDataSequenceHeader, MDBFileInfoView},
    shard_file::MDB_FILE_INFO_ENTRY_SIZE,
    MDBShardFileHeader,
};

pub struct MDBShardView {
    data: Arc<[u8]>,
    file_offsets: Vec<u32>,
    cas_offsets: Vec<u32>,
}

impl MDBShardView {
    pub fn from_reader<R: Read>(reader: &mut R, include_files: bool, include_cas: bool) -> Result<Self> {
        let mut data_vec = Vec::<u8>::new();

        // Check the header; not needed except for version verification.
        let _ = MDBShardFileHeader::deserialize(reader)?;

        // Go through and load the shard into a data vector.
        let mut file_offsets = Vec::<u32>::new();

        // Load in the file entries.
        loop {
            let file_metadata = FileDataSequenceHeader::deserialize(reader)?;

            if file_metadata.is_bookend() {
                break;
            }

            if include_files {
                // register the offset here to the file entries
                file_offsets.push(data_vec.len().try_into()?);
                file_metadata.serialize(&mut data_vec);
            }

            let n = file_metadata.num_entries as usize;

            let mut n_entries = 1 + n;

            if file_metadata.contains_verification() {
                n_entries += n;
            }

            if file_metadata.contains_metadata_ext() {
                n_entries += 1;
            }

            let n_bytes = n_entries * MDB_FILE_INFO_ENTRY_SIZE as u64;

            if include_files {
                copy(&mut reader.take(n_bytes), &mut data_vec)?;
            } else {
                copy(&mut reader.take(n_bytes), &mut io::sink())?;
            }
        }

        if include_cas {
            // Load in the cas entries.
            let mut cas_offsets = Vec::<u32>::new();

            loop {
                let cas_metadata = CASChunkSequenceHeader::deserialize(reader)?;

                if cas_metadata.is_bookend() {
                    break;
                }
                cas_offsets.push(data_vec.len().try_into()?);
                cas_metadata.serialize(&mut data_vec)?;

                copy(&mut reader.take(cas_metadata.num_entries * size_of::<CASChunkSequenceEntry>()), &mut data_vec)?;
            }
        }

        // Aiming for minimal data usage, so shrink things to fit
        data_vec.shrink_to_fit();
        file_offsets.shrink_to_fit();
        cas_offsets.shrink_to_fit();

        Ok(Self {
            data: Arc::from_vector(data_vec),
            file_offsets,
            cas_offsets,
        })
    }

    pub fn file(&self, index: usize) -> MDBFileInfoView {
        let offset = self.file_offsets[index] as usize;

        // do the equivalent of an unwrap here as we have already verified that
        // the sizes are correct on creation.
        MDBFileInfoView::new(self.data.clone(), offset).expect("Programming error: bookkeeping wrong.")
    }

    pub fn num_files(&self) -> usize {
        self.file_offsets.len()
    }

    pub fn num_cas(&self) -> usize {
        self.cas_offsets.len()
    }
}
