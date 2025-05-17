// Copyright (c) 2020 Nathan Fiedler
//
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use merkledb::{chunk_target, low_variance_chunk_target};
use merklehash::*;

const DEFAULT_SIZE: usize = 131072; // 128KiB

/// Example of using fastcdc crate.
/// Splits a (large) file and computes checksums
#[derive(Debug, Parser)]
struct Args {
    /// The desired average size of the chunks.
    #[clap(long, short, default_value_t = DEFAULT_SIZE)]
    size: usize,

    /// If the low variance chunker is used
    #[clap(long, short)]
    lowvariance: bool,

    /// Sets the input file to use
    #[clap()]
    input: PathBuf,
}

fn main() {
    let args = Args::parse();
    let mut file = File::open(args.input).expect("cannot open file!");
    let chunks = if args.lowvariance {
        eprintln!("Using the low variance chunker");
        low_variance_chunk_target(&mut file, args.size, 8)
    } else {
        eprintln!("Using the regular chunker");
        chunk_target(&mut file, args.size)
    };
    let mut h: HashSet<MerkleHash> = HashSet::new();
    let mut dist: BTreeMap<usize, usize> = BTreeMap::new();
    let mut len: usize = 0;
    let mut ulen: usize = 0;
    let total_chunks = chunks.len();
    for entry in chunks {
        let digest = entry.hash;
        if !h.contains(&digest) {
            len += entry.length;
            h.insert(digest);
            if let Entry::Vacant(e) = dist.entry(entry.length) {
                e.insert(1);
            } else {
                *dist.get_mut(&entry.length).unwrap() += 1;
            }
        }
        ulen += entry.length;
    }
    println!("{} / {} = {}", len, ulen, len as f64 / ulen as f64);
    println!("{} unique chunks", h.len());
    println!("{total_chunks} total chunks");
    for (k, v) in dist.iter() {
        println!("{k}, {v}");
    }
}
