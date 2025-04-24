use std::env::current_dir;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;

use merklehash::{HashedWrite, MerkleHash};
use utils::serialization_utils::*;
use uuid::Uuid;

use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
use crate::error::Result;
use crate::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry, SupersetResult,
};
use crate::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
use crate::shard_format::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo};
use crate::utils::truncate_hash;

#[derive(PartialEq, Debug, Copy, Clone)]
enum MDBSetOperation {
    Union,
    Difference,
}

enum NextAction {
    CopyToOut,
    SkipOver,
    Nothing,
    Merge,
}

#[inline]
fn get_next_actions(h1: Option<&MerkleHash>, h2: Option<&MerkleHash>, op: MDBSetOperation) -> Option<[NextAction; 2]> {
    match (h1, h2) {
        (None, None) => None,
        (Some(_), None) => {
            if op == MDBSetOperation::Union {
                Some([NextAction::CopyToOut, NextAction::Nothing])
            } else {
                Some([NextAction::SkipOver, NextAction::Nothing])
            }
        },
        (None, Some(_)) => Some([NextAction::Nothing, NextAction::CopyToOut]),
        (Some(ft0), Some(ft1)) => match ft0.cmp(ft1) {
            std::cmp::Ordering::Less => {
                if op == MDBSetOperation::Union {
                    Some([NextAction::CopyToOut, NextAction::Nothing])
                } else {
                    Some([NextAction::SkipOver, NextAction::Nothing])
                }
            },
            std::cmp::Ordering::Equal => {
                if op == MDBSetOperation::Union {
                    Some([NextAction::CopyToOut, NextAction::SkipOver])
                } else {
                    Some([NextAction::SkipOver, NextAction::SkipOver])
                }
            },
            std::cmp::Ordering::Greater => Some([NextAction::Nothing, NextAction::CopyToOut]),
        },
    }
}

#[inline]
fn get_next_actions_for_file_info(
    h1: Option<&FileDataSequenceHeader>,
    h2: Option<&FileDataSequenceHeader>,
    op: MDBSetOperation,
) -> Option<[NextAction; 2]> {
    // Special case for union operation on file info with same file hash.
    if let (Some(ft0), Some(ft1)) = (h1, h2) {
        if std::cmp::Ordering::Equal == ft0.file_hash.cmp(&ft1.file_hash) && op == MDBSetOperation::Union {
            // Now two parties have the same file hash, and union should produce only one copy.
            // Which one to use is a bit tricky as we have multiple optional pieces of information.
            // We can leverage whether one party's flags are a superset of the other to directly
            // copy over one of the file info. If neither party's flags are a superset, we will
            // need to merge both infos into a single, complete info with info from both parties.
            //
            // Note: we make an assumption that if info exists in both places, it is the same
            // since we can't make a distinction of what is valid and what isn't.
            let superset = FileDataSequenceHeader::compare_flag_superset(ft0, ft1);
            return match superset {
                SupersetResult::SuperA | SupersetResult::Equal => {
                    // use ft0 since it has more info
                    Some([NextAction::CopyToOut, NextAction::SkipOver])
                },
                SupersetResult::SuperB => {
                    // use ft1 since it has more info
                    Some([NextAction::SkipOver, NextAction::CopyToOut])
                },
                SupersetResult::Neither => {
                    // need to merge as both have some info the other doesn't
                    Some([NextAction::Merge, NextAction::Nothing]) // Note: merge advances both entries
                },
            };
        }
    }

    get_next_actions(h1.map(|f| &f.file_hash), h2.map(|f| &f.file_hash), op)
}

fn set_operation<R: Read + Seek, W: Write>(
    s: [&MDBShardInfo; 2],
    r: [&mut R; 2],
    out: &mut W,
    op: MDBSetOperation,
) -> Result<MDBShardInfo> {
    let mut out_offset = 0u64;

    let mut footer = MDBShardFileFooter::default();

    // Write out the header to the output.
    let header = MDBShardFileHeader::default();
    out_offset += header.serialize(out)? as u64;

    ///////////////////////////////////
    // File info section.
    // Set up the seek for the first section:
    r[0].seek(SeekFrom::Start(s[0].metadata.file_info_offset))?;
    r[1].seek(SeekFrom::Start(s[1].metadata.file_info_offset))?;

    footer.file_info_offset = out_offset;

    // This is written later, after this section.
    let mut file_lookup_data = Vec::<(u64, u32)>::new();
    {
        // Manually go through the whole file info section and

        let mut current_index = 0;

        let load_next = |_r: &mut R, _s: &MDBShardInfo| -> Result<_> {
            let fdsh = FileDataSequenceHeader::deserialize(_r)?;
            if fdsh.is_bookend() {
                Ok(None)
            } else {
                Ok(Some(fdsh))
            }
        };

        let mut file_data_header = [load_next(r[0], s[0])?, load_next(r[1], s[1])?];

        while let Some(action) =
            get_next_actions_for_file_info(file_data_header[0].as_ref(), file_data_header[1].as_ref(), op)
        {
            for i in [0, 1] {
                match action[i] {
                    NextAction::CopyToOut => {
                        let fh = file_data_header[i].as_ref().unwrap();

                        out_offset += fh.serialize(out)? as u64;

                        for _ in 0..fh.num_entries {
                            let entry = FileDataSequenceEntry::deserialize(r[i])?;
                            footer.materialized_bytes += entry.unpacked_segment_bytes as u64;
                            entry.serialize(out)?;
                        }

                        out_offset += (fh.num_entries as u64) * (size_of::<FileDataSequenceEntry>() as u64);

                        if fh.contains_verification() {
                            for _ in 0..fh.num_entries {
                                let entry = FileVerificationEntry::deserialize(r[i])?;
                                entry.serialize(out)?;
                            }

                            out_offset += (fh.num_entries as u64) * (size_of::<FileVerificationEntry>() as u64);
                        }

                        if fh.contains_metadata_ext() {
                            let entry = FileMetadataExt::deserialize(r[i])?;
                            out_offset += entry.serialize(out)? as u64;
                        }

                        file_lookup_data.push((truncate_hash(&fh.file_hash), current_index));

                        current_index += 1 + fh.num_info_entry_following();
                        file_data_header[i] = load_next(r[i], s[i])?;
                    },
                    NextAction::SkipOver => {
                        let fh = file_data_header[i].as_ref().unwrap();
                        r[i].seek(SeekFrom::Current(
                            (fh.num_info_entry_following() as i64) * (MDB_FILE_INFO_ENTRY_SIZE as i64),
                        ))?;
                        file_data_header[i] = load_next(r[i], s[i])?;
                    },
                    NextAction::Nothing => {},
                    NextAction::Merge => {
                        let fh0 = file_data_header[0].as_ref().unwrap();
                        let fh1 = file_data_header[1].as_ref().unwrap();
                        FileDataSequenceHeader::verify_same_file(fh0, fh1);
                        let has_verification = fh0.contains_verification() || fh1.contains_verification();
                        let has_metadata_ext = fh0.contains_metadata_ext() || fh1.contains_metadata_ext();

                        let header = FileDataSequenceHeader::new(
                            fh0.file_hash,
                            fh0.num_entries,
                            has_verification,
                            has_metadata_ext,
                        );
                        out_offset += header.serialize(out)? as u64;

                        // copy over the entries from fh0 and advance forward with fh1
                        for _ in 0..fh0.num_entries {
                            let entry = FileDataSequenceEntry::deserialize(r[0])?;
                            footer.materialized_bytes += entry.unpacked_segment_bytes as u64;
                            entry.serialize(out)?;
                        }

                        out_offset += (fh0.num_entries as u64) * (size_of::<FileDataSequenceEntry>() as u64);
                        r[1].seek(SeekFrom::Current((fh1.num_entries as i64) * (MDB_FILE_INFO_ENTRY_SIZE as i64)))?;

                        // if we have verification entries, copy them over from the appropriate shard and
                        // advance the other reader
                        if has_verification {
                            let (read_idx, advance_idx) = if fh0.contains_verification() { (0, 1) } else { (1, 0) };
                            let (read_header, advance_header) = if fh0.contains_verification() {
                                (fh0, fh1)
                            } else {
                                (fh1, fh0)
                            };
                            for _ in 0..read_header.num_entries {
                                let entry = FileVerificationEntry::deserialize(r[read_idx])?;
                                out_offset += entry.serialize(out)? as u64;
                            }
                            if advance_header.contains_verification() {
                                r[advance_idx].seek(SeekFrom::Current(
                                    (advance_header.num_entries as i64) * (MDB_FILE_INFO_ENTRY_SIZE as i64),
                                ))?;
                            }
                        }

                        // if we have metadata_ext, copy it over from the appropriate shard and advance the
                        // other reader
                        if has_metadata_ext {
                            let (read_idx, advance_idx) = if fh0.contains_metadata_ext() { (0, 1) } else { (1, 0) };
                            let (_read_header, advance_header) = if fh0.contains_metadata_ext() {
                                (fh0, fh1)
                            } else {
                                (fh1, fh0)
                            };
                            let entry = FileMetadataExt::deserialize(r[read_idx])?;
                            out_offset += entry.serialize(out)? as u64;
                            if advance_header.contains_metadata_ext() {
                                r[advance_idx].seek(SeekFrom::Current(MDB_FILE_INFO_ENTRY_SIZE as i64))?;
                            }
                        }

                        file_lookup_data.push((truncate_hash(&fh0.file_hash), current_index));
                        current_index += 1 + header.num_info_entry_following();

                        // load the next items
                        file_data_header[0] = load_next(r[0], s[0])?;
                        file_data_header[1] = load_next(r[1], s[1])?;
                    },
                };
            }
        }
        out_offset += FileDataSequenceHeader::bookend().serialize(out)? as u64;
    }

    // These are written later.
    let mut cas_lookup_data = Vec::<(u64, u32)>::new();
    let mut chunk_lookup_data = Vec::<(u64, (u32, u32))>::new();

    {
        ///////////////////////////////////
        // CAS info section.
        // Set up the seek for the first section:
        footer.cas_info_offset = out_offset;

        r[0].seek(SeekFrom::Start(s[0].metadata.cas_info_offset))?;
        r[1].seek(SeekFrom::Start(s[1].metadata.cas_info_offset))?;

        let mut current_index = 0;

        let load_next = |_r: &mut R, _s: &MDBShardInfo| -> Result<_> {
            let ccsh = CASChunkSequenceHeader::deserialize(_r)?;
            if ccsh.is_bookend() {
                Ok(None)
            } else {
                Ok(Some(ccsh))
            }
        };

        let mut cas_data_header = [load_next(r[0], s[0])?, load_next(r[1], s[1])?];

        while let Some(action) = get_next_actions(
            cas_data_header[0].as_ref().map(|h| &h.cas_hash),
            cas_data_header[1].as_ref().map(|h| &h.cas_hash),
            op,
        ) {
            for i in [0, 1] {
                match action[i] {
                    NextAction::CopyToOut => {
                        let fh = cas_data_header[i].as_ref().unwrap();
                        footer.stored_bytes_on_disk += fh.num_bytes_on_disk as u64;
                        footer.stored_bytes += fh.num_bytes_in_cas as u64;

                        out_offset += fh.serialize(out)? as u64;

                        for j in 0..fh.num_entries {
                            let chunk = CASChunkSequenceEntry::deserialize(r[i])?;

                            chunk_lookup_data.push((truncate_hash(&chunk.chunk_hash), (current_index, j)));
                            out_offset += chunk.serialize(out)? as u64;
                        }

                        cas_lookup_data.push((truncate_hash(&fh.cas_hash), current_index));

                        current_index += 1 + fh.num_entries;
                        cas_data_header[i] = load_next(r[i], s[i])?;
                    },
                    NextAction::SkipOver => {
                        let fh = cas_data_header[i].as_ref().unwrap();
                        r[i].seek(SeekFrom::Current(
                            (fh.num_entries as i64) * (size_of::<CASChunkSequenceEntry>() as i64),
                        ))?;
                        cas_data_header[i] = load_next(r[i], s[i])?;
                    },
                    NextAction::Nothing => {},
                    NextAction::Merge => {},
                };
            }
        }

        out_offset += CASChunkSequenceHeader::bookend().serialize(out)? as u64;
    }

    // The file lookup table
    {
        footer.file_lookup_offset = out_offset;
        footer.file_lookup_num_entry = file_lookup_data.len() as u64;
        out_offset += (file_lookup_data.len() * (size_of::<u64>() + size_of::<u32>())) as u64;
        for (h, idx) in file_lookup_data {
            write_u64(out, h)?;
            write_u32(out, idx)?;
        }
    }

    // The cas lookup table
    {
        // Write out the cas and chunk lookup sections.
        footer.cas_lookup_offset = out_offset;
        footer.cas_lookup_num_entry = cas_lookup_data.len() as u64;
        out_offset += (cas_lookup_data.len() * (size_of::<u64>() + size_of::<u32>())) as u64;

        for (h, idx) in cas_lookup_data {
            write_u64(out, h)?;
            write_u32(out, idx)?;
        }
    }

    // The chunk lookup table.
    {
        chunk_lookup_data.sort_unstable_by_key(|t| t.0);

        // Write out the cas and chunk lookup sections.
        footer.chunk_lookup_offset = out_offset;
        footer.chunk_lookup_num_entry = chunk_lookup_data.len() as u64;
        out_offset += (chunk_lookup_data.len() * (size_of::<u64>() + 2 * size_of::<u32>())) as u64;

        for (h, (i1, i2)) in chunk_lookup_data {
            write_u64(out, h)?;
            write_u32(out, i1)?;
            write_u32(out, i2)?;
        }
    }

    // Finally, rewrite the footer.
    {
        footer.footer_offset = out_offset;
        footer.serialize(out)?;
    }

    Ok(MDBShardInfo {
        header,
        metadata: footer,
    })
}

/// Given unions
pub fn shard_set_union<R: Read + Seek, W: Write>(
    s1: &MDBShardInfo,
    r1: &mut R,
    s2: &MDBShardInfo,
    r2: &mut R,
    out: &mut W,
) -> Result<MDBShardInfo> {
    set_operation([s1, s2], [r1, r2], out, MDBSetOperation::Union)
}

pub fn shard_set_difference<R: Read + Seek, W: Write>(
    s1: &MDBShardInfo,
    r1: &mut R,
    s2: &MDBShardInfo,
    r2: &mut R,
    out: &mut W,
) -> Result<MDBShardInfo> {
    set_operation([s1, s2], [r1, r2], out, MDBSetOperation::Difference)
}

fn open_shard_with_bufreader(path: &Path) -> Result<(MDBShardInfo, BufReader<File>)> {
    let mut reader = BufReader::new(File::open(path)?);

    let mdb = MDBShardInfo::load_from_reader(&mut reader)?;

    Ok((mdb, reader))
}

/// Merge two shard files, returning the Merkle hash of the resulting set operation
fn shard_file_op(f1: &Path, f2: &Path, out: &Path, op: MDBSetOperation) -> Result<(MerkleHash, MDBShardInfo)> {
    let cur_dir = current_dir()?;
    let dir = out.parent().unwrap_or(&cur_dir);

    let uuid = Uuid::new_v4();

    let temp_file_name = dir.join(format!(".{uuid}.mdb_temp"));

    let mut hashed_write; // Need to access after file is closed.
                          // Scoped so that file is closed and flushed before name is changed.

    let shard;
    {
        let temp_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_file_name)?;

        hashed_write = HashedWrite::new(temp_file);

        let mut buf_write = BufWriter::new(&mut hashed_write);

        // Do the shard op

        let (s1, mut r1) = open_shard_with_bufreader(f1)?;
        let (s2, mut r2) = open_shard_with_bufreader(f2)?;

        shard = set_operation([&s1, &s2], [&mut r1, &mut r2], &mut buf_write, op)?;
        buf_write.flush()?;
    }
    // Get the hash
    hashed_write.flush()?;
    let shard_hash = hashed_write.hash();

    std::fs::rename(&temp_file_name, out)?;

    Ok((shard_hash, shard))
}

/// Performs a set union operation on two shard files, writing the result to a third file and
/// returning the MerkleHash of the resulting shard file.
pub fn shard_file_union(f1: &Path, f2: &Path, out: &Path) -> Result<(MerkleHash, MDBShardInfo)> {
    shard_file_op(f1, f2, out, MDBSetOperation::Union)
}

/// Performs a set difference operation on two shard files, writing the result to a third file and
/// returning the MerkleHash of the resulting shard file.
pub fn shard_file_difference(f1: &Path, f2: &Path, out: &Path) -> Result<(MerkleHash, MDBShardInfo)> {
    shard_file_op(f1, f2, out, MDBSetOperation::Difference)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::panic::catch_unwind;

    use itertools::iproduct;
    use merklehash::compute_data_hash;
    use tempfile::TempDir;

    use super::*;
    use crate::error::Result;
    use crate::shard_format::test_routines::*;
    use crate::shard_in_memory::MDBInMemoryShard;

    fn test_operations(mem_shard_1: &MDBInMemoryShard, mem_shard_2: &MDBInMemoryShard) -> Result<()> {
        let disk_shard_1 = convert_to_file(mem_shard_1)?;
        let disk_shard_2 = convert_to_file(mem_shard_2)?;

        verify_mdb_shards_match(mem_shard_1, Cursor::new(&disk_shard_1))?;
        verify_mdb_shards_match(mem_shard_2, Cursor::new(&disk_shard_2))?;

        // Now write these out to disk to verify them
        let tmp_dir = TempDir::with_prefix("gitxet_shard_set_test")?;

        let shard_path_1 = tmp_dir.path().join("shard_1.mdb");
        let shard_path_2 = tmp_dir.path().join("shard_2.mdb");

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&shard_path_1)?
            .write_all(&disk_shard_1[..])?;

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&shard_path_2)?
            .write_all(&disk_shard_2[..])?;

        let mut r1 = Cursor::new(&disk_shard_1);
        let s1 = MDBShardInfo::load_from_reader(&mut r1)?;

        let mut r2 = Cursor::new(&disk_shard_2);
        let s2 = MDBShardInfo::load_from_reader(&mut r2)?;

        let mem_union = mem_shard_1.union(mem_shard_2)?;
        let mut shard_union = Vec::<u8>::new();
        shard_set_union(&s1, &mut r1, &s2, &mut r2, &mut shard_union)?;
        verify_mdb_shards_match(&mem_union, Cursor::new(&shard_union))?;

        let disk_union_path = tmp_dir.path().join("shard_union.mdb");
        let (disk_union_hash, _) = shard_file_union(&shard_path_1, &shard_path_2, &disk_union_path)?;

        let mut disk_union_reader = BufReader::new(File::open(&disk_union_path)?);
        verify_mdb_shards_match(&mem_union, &mut disk_union_reader)?;
        assert_eq!(disk_union_hash, compute_data_hash(&shard_union[..]));

        let mem_difference = mem_shard_1.difference(mem_shard_2)?;
        let mut shard_difference = Vec::<u8>::new();
        shard_set_difference(&s1, &mut r1, &s2, &mut r2, &mut shard_difference)?;
        verify_mdb_shards_match(&mem_difference, Cursor::new(&shard_difference))?;

        let disk_difference_path = tmp_dir.path().join("shard_difference.mdb");
        let (disk_difference_hash, _) = shard_file_difference(&shard_path_1, &shard_path_2, &disk_difference_path)?;

        let mut disk_difference_reader = BufReader::new(File::open(&disk_difference_path)?);
        verify_mdb_shards_match(&mem_difference, &mut disk_difference_reader)?;
        assert_eq!(disk_difference_hash, compute_data_hash(&shard_difference[..]));

        Ok(())
    }

    /// generate the 4 different combinations of shards that can have the following content:
    /// with/without verification
    /// with/without extra metadata (i.e. file SHA)
    ///
    /// The response is a Vec of the shards with a "name" associated with the shard for ease
    /// in debugging any errors.
    #[allow(clippy::type_complexity)]
    fn gen_specific_shard_cases(
        name: &str,
        cas_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
        verifications: &[&[u64]],
        metadata_exts: &[u64],
    ) -> Result<Vec<(String, MDBInMemoryShard)>> {
        Ok(vec![
            (format!("{name}_None_None"), gen_specific_shard(cas_nodes, file_nodes, None, None)?),
            (format!("{name}_Some_None"), gen_specific_shard(cas_nodes, file_nodes, Some(verifications), None)?),
            (format!("{name}_None_Some"), gen_specific_shard(cas_nodes, file_nodes, None, Some(metadata_exts))?),
            (
                format!("{name}_Some_Some"),
                gen_specific_shard(cas_nodes, file_nodes, Some(verifications), Some(metadata_exts))?,
            ),
        ])
    }

    #[test]
    fn test_simple() -> Result<()> {
        let s1_vec =
            gen_specific_shard_cases("s1", &[(10, &[(21, 5)])], &[(100, &[(200, (0, 5))])], &[&[485]], &[914])?;
        let s2_vec =
            gen_specific_shard_cases("s2", &[(11, &[(22, 5)])], &[(101, &[(201, (0, 5))])], &[&[624]], &[772])?;
        for (s1, mem_shard_1) in s1_vec.iter() {
            for (s2, mem_shard_2) in s2_vec.iter() {
                catch_unwind(|| {
                    test_operations(mem_shard_1, mem_shard_2).unwrap();
                })
                .unwrap_or_else(|_| panic!("Failed simple ops: {s1} U {s2}"));
            }
        }
        Ok(())
    }

    #[test]
    fn test_intersecting() -> Result<()> {
        let cases = gen_specific_shard_cases("s1", &[(10, &[(21, 5)])], &[(100, &[(200, (0, 5))])], &[&[95]], &[27])?;
        for (s1, mem_shard_1) in cases.iter() {
            for (s2, mem_shard_2) in cases.iter() {
                catch_unwind(|| {
                    test_operations(mem_shard_1, mem_shard_2).unwrap();
                })
                .unwrap_or_else(|_| panic!("Failed intersecting ops: {s1} U {s2}"));
            }
        }
        Ok(())
    }

    #[test]
    fn test_intersecting_2() -> Result<()> {
        let s1_cases =
            gen_specific_shard_cases("s1", &[(10, &[(21, 5)])], &[(100, &[(200, (0, 5))])], &[&[365]], &[9364])?;
        let s2_cases = gen_specific_shard_cases(
            "s2",
            &[(10, &[(21, 5)]), (11, &[(22, 5)])],
            &[(100, &[(200, (0, 5))]), (101, &[(201, (0, 5))])],
            &[&[365], &[48]],
            &[9364, 252],
        )?;

        for (s1, mem_shard_1) in s1_cases.iter() {
            for (s2, mem_shard_2) in s2_cases.iter() {
                catch_unwind(|| {
                    test_operations(mem_shard_1, mem_shard_2).unwrap();
                })
                .unwrap_or_else(|_| panic!("Failed intersecting ops, multi-file: {s1} U {s2}"));
            }
        }
        Ok(())
    }

    #[test]
    fn test_empty() -> Result<()> {
        let mem_shard_1 = gen_specific_shard(&[], &[], None, None)?;
        let mem_shard_2 = gen_specific_shard(&[], &[], None, None)?;

        test_operations(&mem_shard_1, &mem_shard_2)
    }
    #[test]
    fn test_empty_2() -> Result<()> {
        let mem_shard_1 = gen_random_shard(0, &[0], &[0], false, false)?;
        let mem_shard_2 = gen_random_shard(1, &[0], &[0], false, false)?;

        test_operations(&mem_shard_1, &mem_shard_2)
    }

    #[test]
    fn test_random() -> Result<()> {
        let bool_cases = vec![false, true];
        let cases = iproduct!(bool_cases.clone(), bool_cases.clone(), bool_cases);
        for (v1, v2, v3) in cases {
            let mem_shard_1 = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6], v1, v3)?;
            let mem_shard_2 = gen_random_shard(1, &[3, 5, 9, 8], &[8, 5, 5, 8, 5, 6], v2, v3)?;

            test_operations(&mem_shard_1, &mem_shard_2)?;

            let mem_shard_3 = mem_shard_1.union(&mem_shard_2)?;

            test_operations(&mem_shard_1, &mem_shard_3)?;
            test_operations(&mem_shard_2, &mem_shard_3)?;
        }

        Ok(())
    }
}
