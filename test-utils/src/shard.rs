use std::io::Write;

use mdb_shard::{
    cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo},
    file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
    shard_in_memory::MDBInMemoryShard,
    MDBShardInfo,
};
use merklehash::{HashedWrite, MerkleHash};
use rand::{
    rngs::{SmallRng, StdRng},
    Rng, SeedableRng,
};

fn rng_hash(seed: u64) -> MerkleHash {
    let mut rng = SmallRng::seed_from_u64(seed);
    MerkleHash::from([rng.gen(), rng.gen(), rng.gen(), rng.gen()])
}

pub fn gen_random_shard(
    cas_block_sizes: &[usize],
    file_chunk_range_sizes: &[usize],
) -> mdb_shard::error::Result<MDBInMemoryShard> {
    let seed: u64 = rand::random();
    gen_random_shard_with_seed(seed, cas_block_sizes, file_chunk_range_sizes)
}

pub fn gen_random_shard_with_seed(
    seed: u64,
    cas_block_sizes: &[usize],
    file_chunk_range_sizes: &[usize],
) -> mdb_shard::error::Result<MDBInMemoryShard> {
    // generate the cas content stuff.
    let mut shard = MDBInMemoryShard::default();
    let mut rng = StdRng::seed_from_u64(seed);

    for cas_block_size in cas_block_sizes {
        let mut cas_block = Vec::<_>::new();
        let mut pos = 0u32;

        for _ in 0..*cas_block_size {
            cas_block.push(CASChunkSequenceEntry::new(
                rng_hash(rng.gen()),
                rng.gen_range(10000..20000),
                pos,
            ));
            pos += rng.gen_range(10000..20000);
        }

        shard.add_cas_block(MDBCASInfo {
            metadata: CASChunkSequenceHeader::new(rng_hash(rng.gen()), *cas_block_size, pos),
            chunks: cas_block,
        })?;
    }

    for file_block_size in file_chunk_range_sizes {
        let file_hash = rng_hash(rng.gen());

        let file_contents: Vec<_> = (0..*file_block_size)
            .map(|_| {
                let lb = rng.gen_range(0..10000);
                let ub = lb + rng.gen_range(0..10000);
                FileDataSequenceEntry::new(rng_hash(rng.gen()), ub - lb, lb, ub)
            })
            .collect();

        shard.add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, *file_block_size),
            segments: file_contents,
        })?;
    }

    Ok(shard)
}

pub fn get_shard_hash(shard: &MDBInMemoryShard) -> mdb_shard::error::Result<MerkleHash> {
    let mut hashed_write; // Need to access after file is closed.

    hashed_write = HashedWrite::new(std::io::empty());
    MDBShardInfo::serialize_from(&mut hashed_write, shard)?;

    // Get the hash
    hashed_write.flush()?; // not actually necessary with std::io::Empty
    let shard_hash = hashed_write.hash();

    Ok(shard_hash)
}
