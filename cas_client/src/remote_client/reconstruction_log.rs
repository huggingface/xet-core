use std::env::current_dir;
use std::fs::OpenOptions;
use std::time::Duration;

use cas_types::{ChunkRange, HexMerkleHash};
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use crate::error::Result;
use crate::CasClientError;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GetVariant {
    Cache,
    Download,
}

pub struct TermLog {
    pub hash: MerkleHash,
    pub term_range: ChunkRange,
    pub source: GetVariant,
    pub term_idx: usize,
    pub time: Duration,
    pub final_size: usize,

    // only relevant to download
    pub download_range: ChunkRange,
    pub download_len: usize,
}

#[derive(Debug, Serialize)]
struct TermLogSerializable {
    pub hash: HexMerkleHash,
    pub term_range_start: u32,
    pub term_range_end: u32,
    pub source: GetVariant,
    pub time_micros: u64,
    pub term_idx: usize,
    pub final_size: usize,

    // only relevant to download
    pub download_range_start: u32,
    pub download_range_end: u32,
    pub download_len: usize,
}

impl From<TermLog> for TermLogSerializable {
    fn from(value: TermLog) -> Self {
        let TermLog {
            hash,
            term_range,
            source,
            download_range,
            time,
            term_idx,
            final_size,
            download_len,
        } = value;
        Self {
            hash: hash.into(),
            term_range_start: term_range.start,
            term_range_end: term_range.end,
            source,
            download_range_start: download_range.start,
            download_range_end: download_range.end,
            time_micros: time.as_micros() as u64,
            term_idx,
            final_size,
            download_len,
        }
    }
}

pub async fn record_reconstruction_terms(
    file_id: MerkleHash,
    mut recv: Receiver<TermLog>,
    num_terms: usize,
) -> Result<()> {
    let mut file_path = current_dir()?;
    file_path.push(format!("{file_id}_reconstruction_log.csv"));
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    for _i in 0..num_terms {
        if let Some(term_log) = recv.recv().await {
            let term_log_ser: TermLogSerializable = term_log.into();
            writer
                .serialize(term_log_ser)
                .log_error("error writing value to csv")
                .map_err(|e| CasClientError::Other(format!("can't serialize term log {e}")))
                .unwrap();
        }
    }
    writer.flush()?;

    Ok(())
}
