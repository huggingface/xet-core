use std::io::{self, copy, Read};
use std::sync::Arc;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfoView};
use crate::error::Result;
use crate::file_structs::{FileDataSequenceHeader, MDBFileInfoView};
use crate::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
use crate::MDBShardFileHeader;

pub struct MDBMinimalShard {
    data: Arc<[u8]>,
    file_offsets: Vec<u32>,
    cas_offsets: Vec<u32>,
}

impl MDBMinimalShard {
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
                file_offsets.push(data_vec.len() as u32);
                file_metadata.serialize(&mut data_vec)?;
            }

            let n = file_metadata.num_entries as usize;

            let mut n_entries = n;

            if file_metadata.contains_verification() {
                n_entries += n;
            }

            if file_metadata.contains_metadata_ext() {
                n_entries += 1;
            }

            let n_bytes = (n_entries * MDB_FILE_INFO_ENTRY_SIZE) as u64;

            if include_files {
                copy(&mut reader.take(n_bytes), &mut data_vec)?;
            } else {
                copy(&mut reader.take(n_bytes), &mut io::sink())?;
            }
        }

        let mut cas_offsets = Vec::<u32>::new();

        if include_cas {
            // Load in the cas entries.

            loop {
                let cas_metadata = CASChunkSequenceHeader::deserialize(reader)?;

                if cas_metadata.is_bookend() {
                    break;
                }
                cas_offsets.push(data_vec.len() as u32);
                cas_metadata.serialize(&mut data_vec)?;

                copy(
                    &mut reader.take((cas_metadata.num_entries as u64) * (size_of::<CASChunkSequenceEntry>() as u64)),
                    &mut data_vec,
                )?;
            }
        }

        // Aiming for minimal data usage, so shrink things to fit
        data_vec.shrink_to_fit();
        file_offsets.shrink_to_fit();
        cas_offsets.shrink_to_fit();

        Ok(Self {
            data: Arc::from(data_vec),
            file_offsets,
            cas_offsets,
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
}
