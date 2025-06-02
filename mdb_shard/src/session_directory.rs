use std::collections::HashSet;
use std::io::Cursor;
use std::mem::swap;
use std::path::Path;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::error::Result;
use crate::set_operations::shard_set_union;
use crate::shard_file_handle::MDBShardFile;
use crate::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo};

/// Merge a collection of shards, deleting the old ones.
/// After calling this, the passed in shards may be invalid -- i.e. may refer to a shard that doesn't exist.
/// All shards are either kept as is or merged into shards in the session directory.
///
/// Ordering of staged shards is preserved.
pub fn consolidate_shards_in_directory(
    session_directory: impl AsRef<Path>,
    target_max_size: u64,
    skip_on_error: bool,
) -> Result<Vec<Arc<MDBShardFile>>> {
    let session_directory = session_directory.as_ref();
    // Get the new shards and the shards in the original list to remove.
    let shard_merge_result = merge_shards(session_directory, session_directory, target_max_size, skip_on_error)?;

    // Now, go through and remove all the shards in the delete list.
    for sfi in shard_merge_result.obsolete_shards {
        let res = std::fs::remove_file(&sfi.path);

        if !skip_on_error {
            res?;
        }
    }

    Ok(shard_merge_result.merged_shards)
}

#[derive(Default)]
pub struct ShardMergeResult {
    pub merged_shards: Vec<Arc<MDBShardFile>>,
    pub obsolete_shards: Vec<Arc<MDBShardFile>>,
    pub skipped_shards: Vec<Arc<MDBShardFile>>,
}

/// Merge a collection of shards, returning the new ones and the ones that can be deleted.
///
/// After calling this, the passed in shards may be invalid -- i.e. may refer to a shard that doesn't exist.
/// All shards are either merged into shards in the result directory or moved to that directory (if not there already).
///
/// Ordering of staged shards is preserved.
#[allow(clippy::needless_range_loop)] // The alternative is less readable IMO
pub fn merge_shards(
    source_directory: impl AsRef<Path>,
    target_directory: impl AsRef<Path>,
    target_max_size: u64,
    skip_on_error: bool,
) -> Result<ShardMergeResult> {
    let mut shards: Vec<_> = MDBShardFile::load_all_valid(source_directory.as_ref())?;

    shards.sort_unstable_by_key(|si| si.last_modified_time);

    // Make not mutable
    let shards = shards;
    if shards.is_empty() {
        return Ok(ShardMergeResult::default());
    }

    let mut cur_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut next_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(target_max_size as usize);

    let mut dest_shards = Vec::<Arc<MDBShardFile>>::with_capacity(shards.len());
    let mut ingested_shards: Vec<Arc<MDBShardFile>> = Vec::with_capacity(shards.len());
    let mut skipped_shards = Vec::new();

    let mut cur_si = MDBShardInfo::default();

    for sfi in shards {
        // Now, load the new shard data in.  To be resiliant to the possibility of shards
        // being deleted under us (as can happen in shard session resume with multiple
        // processes running), always load it all into memory at the start and write it out
        // at the end.
        if let Err(e) = sfi.read_into_buffer(&mut next_data) {
            if skip_on_error {
                info!("Error encountered reading shard {:?}: {e}; skipping.", &sfi.path);
                skipped_shards.push(sfi.clone());
                continue;
            } else {
                error!("Error encountered reading shard {:?}: {e}.", &sfi.path);
                return Err(e);
            }
        };

        ingested_shards.push(sfi.clone());

        if cur_data.is_empty() {
            // Starting from scratch
            swap(&mut cur_data, &mut next_data);
            cur_si = sfi.shard.clone();
        } else if cur_data.len() + next_data.len() - (size_of::<MDBShardFileHeader>() + size_of::<MDBShardFileFooter>())
            <= target_max_size as usize
        {
            // We have enough size capacity to merge this one in.
            out_data.clear();
            cur_si = shard_set_union(
                &cur_si,
                &mut Cursor::new(&cur_data),
                &sfi.shard,
                &mut Cursor::new(&next_data),
                &mut out_data,
            )?;

            // Now swap out the destination data with the current data.
            swap(&mut out_data, &mut cur_data);
        } else {
            // Flush everything out and replace the new.
            let out_sfi = MDBShardFile::write_out_from_reader(&target_directory, &mut Cursor::new(&cur_data))?;
            dest_shards.push(out_sfi);

            // Move the loaded data into the current buffer.
            swap(&mut cur_data, &mut next_data);
            cur_si = sfi.shard.clone();
        }
    }

    // If there is any left over at the end, flush that as well.
    if !cur_data.is_empty() {
        let out_sfi = MDBShardFile::write_out_from_reader(&target_directory, &mut Cursor::new(&cur_data))?;
        dest_shards.push(out_sfi);
    }

    // Now the obselete shards are all the source shards that do not refer to the same shard file
    // as one of the dest shards.
    if source_directory.as_ref() == target_directory.as_ref() {
        let dest_shard_hashes: HashSet<_> = dest_shards.iter().map(|s| s.shard_hash).collect();

        ingested_shards.retain(|sfi| !dest_shard_hashes.contains(&sfi.shard_hash));
    }

    Ok(ShardMergeResult {
        merged_shards: dest_shards,
        obsolete_shards: ingested_shards,
        skipped_shards,
    })
}

/// Same as above, but performs it in the background and on a io focused thread.
pub fn merge_shards_background(
    source_directory: impl AsRef<Path>,
    target_directory: impl AsRef<Path>,
    target_max_size: u64,
    skip_on_error: bool,
) -> JoinHandle<Result<ShardMergeResult>> {
    let source_directory = source_directory.as_ref().to_owned();
    let target_directory = target_directory.as_ref().to_owned();

    tokio::task::spawn_blocking(move || {
        merge_shards(source_directory, target_directory, target_max_size, skip_on_error)
    })
}
