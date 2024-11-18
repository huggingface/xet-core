use std::{env::current_dir, fs::OpenOptions, time::Duration};

use cas_types::{ChunkRange, HexMerkleHash};
use merklehash::MerkleHash;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use crate::{error::Result, CasClientError};

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
    // only relevant to download
    pub xorb_range: ChunkRange,
    pub time: Duration,
}

#[derive(Debug, Serialize)]
struct TermLogSerializable {
    pub hash: HexMerkleHash,
    pub term_range_start: u32,
    pub term_range_end: u32,
    pub source: GetVariant,
    // only relevant to download
    pub xorb_range_start: u32,
    pub xorb_range_end: u32,
    pub time_micros: u64,
}

impl From<TermLog> for TermLogSerializable {
    fn from(value: TermLog) -> Self {
        let TermLog {
            hash,
            term_range,
            source,
            xorb_range,
            time,
        } = value;
        Self {
            hash: hash.into(),
            term_range_start: term_range.start,
            term_range_end: term_range.end,
            source,
            xorb_range_start: xorb_range.start,
            xorb_range_end: xorb_range.end,
            time_micros: time.as_micros() as u64,
        }
    }
}

pub async fn record_reconstruction_terms(
    file: MerkleHash,
    mut recv: Receiver<TermLog>,
    num_terms: usize,
) -> Result<()> {
    let mut file_path = current_dir()?;
    file_path.push(format!("{file}_reconstruction_log.csv"));
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    for _ in 0..num_terms {
        if let Some(term_log) = recv.recv().await {
            let term_log_ser: TermLogSerializable = term_log.into();
            writer
                .serialize(term_log_ser)
                .map_err(|e| CasClientError::Other(format!("can't serialize term log {e}")))?;
        }
    }
    writer.flush()?;

    Ok(())
}
