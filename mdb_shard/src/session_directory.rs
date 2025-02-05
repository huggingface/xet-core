use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use merklehash::MerkleHash;
use tracing::debug;

use crate::error::Result;
use crate::set_operations::shard_set_union;
use crate::shard_file_handle::MDBShardFile;

// Merge a collection of shards.
// After calling this, the passed in shards may be invalid -- i.e. may refer to a shard that doesn't exist.
// All shards are either merged into shards in the result directory or moved to that directory (if not there already).
//
// Ordering of staged shards is preserved.

#[allow(clippy::needless_range_loop)] // The alternative is less readable IMO
pub fn consolidate_shards_in_directory(
    session_directory: &Path,
    target_max_size: u64,
) -> Result<Vec<Arc<MDBShardFile>>> {
    let mut shards = MDBShardFile::load_all_valid(session_directory)?;

    shards.sort_unstable_by_key(|si| si.last_modified_time);

    // Make not mutable
    let shards = shards;

    let mut finished_shards = Vec::<Arc<MDBShardFile>>::with_capacity(shards.len());
    let mut finished_shard_hashes = HashSet::<MerkleHash>::with_capacity(shards.len());

    let mut cur_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut alt_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(target_max_size as usize);

    let mut cur_idx = 0;

    {
        while cur_idx < shards.len() {
            let cur_sfi = &shards[cur_idx];

            // Now, see how many we can consolidate.
            let mut ub_idx = cur_idx + 1;
            let mut current_size = cur_sfi.shard.num_bytes();

            // Do we have to remove any shards along the way?
            let mut shards_to_remove = Vec::<(MerkleHash, PathBuf)>::new();

            for idx in (cur_idx + 1).. {
                if idx == shards.len() || shards[idx].shard.num_bytes() + current_size >= target_max_size {
                    ub_idx = idx;
                    break;
                }
                current_size += shards[idx].shard.num_bytes()
            }

            if ub_idx == cur_idx + 1 {
                // We can't consolidate any here.

                finished_shard_hashes.insert(cur_sfi.shard_hash);
                finished_shards.push(cur_sfi.clone());
            } else {
                // We have one or more shards to merge, so do this all in memory.

                // Get the current data in a buffer
                let mut cur_shard_info = cur_sfi.shard.clone();

                cur_data.clear();
                std::fs::File::open(&cur_sfi.path)?.read_to_end(&mut cur_data)?;

                // Now, merge in everything in memory
                for i in (cur_idx + 1)..ub_idx {
                    let sfi = &shards[i];

                    alt_data.clear();
                    std::fs::File::open(&sfi.path)?.read_to_end(&mut alt_data)?;

                    // Now merge the main shard
                    out_data.clear();

                    // Merge these in to the current shard.
                    cur_shard_info = shard_set_union(
                        &cur_shard_info,
                        &mut Cursor::new(&cur_data),
                        &sfi.shard,
                        &mut Cursor::new(&alt_data),
                        &mut out_data,
                    )?;

                    swap(&mut cur_data, &mut out_data);
                }

                // Write out the shard.
                let new_sfi = { MDBShardFile::write_out_from_reader(session_directory, &mut Cursor::new(&cur_data))? };

                debug!(
                    "Created merged shard {:?} from shards {:?}",
                    &new_sfi.path,
                    shards[cur_idx..ub_idx].iter().map(|sfi| &sfi.path)
                );

                finished_shard_hashes.insert(new_sfi.shard_hash);
                finished_shards.push(new_sfi);

                // Delete the old ones.
                for sfi in shards[cur_idx..ub_idx].iter() {
                    shards_to_remove.push((sfi.shard_hash, sfi.path.to_path_buf()));
                }
            }

            for (shard_hash, path) in shards_to_remove.iter() {
                if finished_shard_hashes.contains(shard_hash) {
                    // In rare cases, there could be empty shards or shards with
                    // duplicate entries and we don't want to delete any shards
                    // we've already finished
                    continue;
                }
                debug!(
                    "consolidate_shards: Removing {:?}; info merged to {:?}",
                    &path,
                    &finished_shards.last().unwrap().shard_hash
                );

                std::fs::remove_file(path)?;
            }

            cur_idx = ub_idx;
        }
    }

    Ok(finished_shards)
}
