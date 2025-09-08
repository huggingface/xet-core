use std::fmt::Write;
use std::cell::RefCell;
use std::fs::File;
use std::io::Read;
use clap::Parser;
use cas_types::HexMerkleHash;
use merklehash::{compute_internal_node_hash, MerkleHash};
use serde::Deserialize;
use deduplication::Chunker;
use deduplication::constants::TARGET_CHUNK_SIZE;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(short, long)]
    original_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Chunk {
    pub hash: HexMerkleHash,
    pub length: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FileFormat {
    file_hash: HexMerkleHash,
    file_chunks: Vec<Chunk>,
}

fn main() {
    let args = Args::parse();
    let file = File::open(&args.path).unwrap();
    let FileFormat { file_hash, file_chunks } = serde_json::from_reader(file).unwrap();

    let chunks: Vec<(MerkleHash, usize)> = file_chunks.into_iter().map(|chunk| {
        (chunk.hash.into(), chunk.length as usize)
    }).collect();

    let computed_file_hash = file_hash_with_salt(&chunks);
    // let computed_file_hash = merklehash::file_hash(&chunks);

    // println!("json content:\t\t{file_hash}");
    println!("computed file hash:\t{computed_file_hash}");

    let Some(original_file) = args.original_file else {
        return;
    };

    let mut file = File::open(original_file).unwrap();
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);
    let mut computed_chunks = vec![];
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let num_read = file.read(&mut buf).unwrap();
        if num_read == 0 {
            break;
        }
        let new_chunks = chunker.next_block(&buf[..num_read], false);
        computed_chunks.extend(new_chunks.into_iter().map(|chunk| (chunk.hash, chunk.data.len())));
    }
    if let Some(chunk) = chunker.finish() {
        computed_chunks.push((chunk.hash, chunk.data.len()));
    }

    let computed_computed_file_hash = merklehash::file_hash(&computed_chunks);
    println!("file content computed hash:\t{computed_computed_file_hash}");

    if chunks.len() != computed_chunks.len() {
        println!("chunks len don't match comp {} != {} rec", computed_chunks.len(), chunks.len());
        println!("first chunks of each\nchunked\t: {} {}\nrec\t: {} {}", computed_chunks[0].0, computed_chunks[0].1, chunks[0].0, chunks[0].1);
        return;
    }

    println!("lens match");
    for (i, ((comp_hash, comp_len), (rec_hash, rec_len))) in computed_chunks.iter().zip(chunks.iter()).enumerate() {
        if rec_hash == comp_hash && rec_len == comp_len {
            continue;
        }
        println!("first mismatch at index: {i}");
        println!("chunker: {comp_hash} {comp_len}");
        println!("from json: {rec_hash} {rec_len}");
        return;
    }

    println!("all chunks match");

}


pub const AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR: u64 = 4;

/// Find the next cut point in a sequence of hashes at which to break.
///
///
/// We basically loop through the set of nodes tracking a window between
/// cur_children_start_idx and idx (current index).
/// [. . . . . . . . . . . ]
///          ^   ^
///          |   |
///  start_idx   |
///              |
///             idx
///
/// When the current node at idx satisfies the cut condition:
///  - the hash % MEAN_TREE_BRANCHING_FACTOR == 0: assuming a random hash distribution, this implies on average, the
///    number of children is AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR,
///  - OR this is the last node in the list.
///  - subject to each parent must have at least 2 children, and at most AGGREGATED_MEAN_TREE_BRANCHING_FACTOR * 2
///    children: This ensures that the graph always has at most 1/2 the number of parents as children. and we don't have
///    too wide branches.
#[inline]
fn next_merge_cut(hashes: &[(MerkleHash, usize)]) -> usize {
    if hashes.len() <= 2 {
        return hashes.len();
    }

    let end = (2 * AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR as usize + 1).min(hashes.len());

    for i in 2..end {
        let h = unsafe { hashes.get_unchecked(i).0 };

        if h % AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR == 0 {
            return i + 1;
        }
    }

    end
}

/// Merge the hashes together, including the size information and returning the new (hash, size) pair.
#[inline]
fn merged_hash_of_sequence(hash: &[(MerkleHash, usize)]) -> (MerkleHash, usize) {
    // Use a threadlocal buffer to avoid the overhead of reallocations.
    thread_local! {
        static BUFFER: RefCell<String> =
        RefCell::new(String::with_capacity(1024));
    }

    BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        let mut total_len = 0;

        for (h, s) in hash.iter() {
            writeln!(buf, "{h:x} : {s}").unwrap();
            total_len += *s;
        }
        (compute_internal_node_hash(buf.as_bytes()), total_len)
    })
}

/// The base calculation for the aggregated node hash.
///
/// Iteratively collapse the list of hashes using the criteria in next_merge_cut
/// until only one hash remains; this is the aggregated hash.
#[inline]
fn aggregated_node_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    if chunks.is_empty() {
        return MerkleHash::default();
    }

    let mut hv = chunks.to_vec();

    let mut round = 0;
    while hv.len() > 1 {
        round += 1;
        let mut write_idx = 0;
        let mut read_idx = 0;

        while read_idx != hv.len() {
            // Find the next cut point of hashes at which to merge.
            let next_cut = read_idx + next_merge_cut(&hv[read_idx..]);

            // Get the merged hash of this block.
            hv[write_idx] = merged_hash_of_sequence(&hv[read_idx..next_cut]);
            println!("round: {round} cut: {next_cut} merged hash {} {}", hv[write_idx].0, hv[write_idx].1);
            write_idx += 1;

            read_idx = next_cut;
        }

        hv.resize(write_idx, Default::default());
    }

    hv[0].0
}

/// The file hash when a salt is needed.
#[inline]
pub fn file_hash_with_salt(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
    let salt = &[0; 32];
    if chunks.is_empty() {
        return MerkleHash::default();
    }


    let agg = aggregated_node_hash(chunks);
    println!("pre salt: {agg}");
    agg.hmac(salt.into())
}
