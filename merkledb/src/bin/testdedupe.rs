// Copyright (c) 2020 Nathan Fiedler
//
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::str::FromStr;

use clap::{App, Arg};
use merkledb::{chunk_target, low_variance_chunk_target};
use merklehash::*;

fn main() {
    fn is_integer(v: &str) -> Result<(), String> {
        if u64::from_str(v).is_ok() {
            return Ok(());
        }
        Err(String::from("The size must be a valid unsigned 64-bit integer."))
    }
    let matches = App::new("Example of using fastcdc crate.")
        .about("Splits a (large) file and computes checksums.")
        .arg(
            Arg::new("size")
                .short('s')
                .long("size")
                .value_name("SIZE")
                .help("The desired average size of the chunks.")
                .takes_value(true)
                .validator(is_integer),
        )
        .arg(
            Arg::new("lowvariance")
                .short('l')
                .long("lowvariance")
                .help("If the low variance chunker is used"),
        )
        .arg(Arg::new("INPUT").help("Sets the input file to use").required(true).index(1))
        .get_matches();
    let size = matches.value_of("size").unwrap_or("131072");
    let lv: bool = if matches.occurrences_of("lowvariance") >= 1 {
        eprintln!("Using the low variance chunker");
        true
    } else {
        eprintln!("Using the regular chunker");
        false
    };
    let avg_size = u64::from_str(size).unwrap() as usize;
    let filename = matches.value_of("INPUT").unwrap();
    let mut file = File::open(filename).expect("cannot open file!");
    let chunks = if lv {
        low_variance_chunk_target(&mut file, avg_size, 8)
    } else {
        chunk_target(&mut file, avg_size)
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
