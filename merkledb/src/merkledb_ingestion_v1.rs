use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Instant, SystemTime};

use rayon::prelude::*;
use tracing::{debug, info};
use walkdir::*;

use crate::chunk_iterator::*;
use crate::constants::*;
use crate::error::Result;
use crate::merkledb_highlevel_v1::*;
use crate::merklenode::*;

pub trait MerkleDBIngestionMethodsV1: MerkleDBHighLevelMethodsV1 {
    /**
     * Ingests a single file into the MerkleTree returning a MerkleNode
     * on success.
     */
    fn ingest_file(&mut self, input: PathBuf) -> Result<MerkleNode> {
        debug!("Ingesting file {:?}", input);
        let input_file = File::open(&input)?;
        let mut buf_reader = BufReader::new(input_file);
        let chunks = low_variance_chunk_target(&mut buf_reader, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
        let mut staging = self.start_insertion_staging();
        self.add_file(&mut staging, &chunks);
        let ret = self.finalize(staging);
        self.flush()?;
        Ok(ret)
    }

    /**
     * Ingests an entire directory into the MerkleTree returning a MerkleNode
     * on success.
     */
    fn ingest_dir(
        &mut self,
        input: PathBuf,
        after_file_date: Option<SystemTime>,
        metadata_output_file: &mut BufWriter<File>,
    ) -> Option<MerkleNode> {
        let now = Instant::now();
        debug!("Ingesting dir {:?}", input);
        let mut staging = self.start_insertion_staging();
        // Walkdir usage copied from https://docs.rs/walkdir/2.3.2/walkdir/
        // TODO: magic constant here. Probably change to something like k * nCPUs
        let (tx, rx) = sync_channel::<(Vec<ChunkInfo>, PathBuf)>(64);
        thread::spawn(move || {
            WalkDir::new(&input)
                .follow_links(false) // do not follow symlinks
                .into_iter()
                .filter_entry(|e| !e.path_is_symlink()) // do not read symlinks
                .filter_map(|e| e.ok())
                .par_bridge() // we use par_bridge() instead of
                // collecting then par_iter since when
                // there are millions of files, collecting
                // will be terrifyingly memory intensive.
                .filter(|p| p.file_type().is_file())
                .filter(|p| {
                    if let Some(after) = after_file_date {
                        if let Ok(Ok(modified)) = p.metadata().map(|m| m.modified()) {
                            modified >= after
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                })
                .map(|entry| {
                    let path = entry.into_path();

                    let input_file = match File::open(&path) {
                        Err(why) => panic!("Cannot open {input:?}: {why}"),
                        Ok(file) => file,
                    };
                    let mut buf_reader = BufReader::new(input_file);
                    let chunks =
                        low_variance_chunk_target(&mut buf_reader, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS);
                    (chunks, path)
                })
                .for_each(|x| tx.send(x).unwrap());
        });
        while let Ok((chunks, path)) = rx.recv() {
            if !chunks.is_empty() {
                let hash = self.add_file(&mut staging, &chunks);
                writeln!(metadata_output_file, "{hash:x} {path:?}").unwrap();
            } else {
                writeln!(metadata_output_file, "{:x} {:?}", MerkleHash::default(), path).unwrap();
            }
        }
        let dirroot = self.finalize(staging);
        self.flush().unwrap();

        let chunk_time = now.elapsed().as_secs_f64();
        info!("Completed chunking in {:.2?}", now.elapsed());

        let total_length = dirroot.len() as f64;
        info!("Chunking speed: {} MB/s", total_length / 1024.0 / 1024.0 / chunk_time);
        Some(dirroot)
    }
}
