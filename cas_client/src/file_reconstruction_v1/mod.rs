//! File reconstruction module v1 for the CAS client.
//!
//! This module contains all functionality for reconstructing files from CAS storage,
//! including:
//! - Download utilities for fetching terms and managing concurrent downloads
//! - Output providers for writing reconstructed data
//! - The `FileReconstructorV1` struct that orchestrates file reconstruction
//!
//! The reconstruction process involves:
//! 1. Querying the CAS server for file reconstruction info (terms and fetch info)
//! 2. Downloading the required chunk ranges from blob storage
//! 3. Decompressing and reassembling the file data
//! 4. Writing the output to the provided destination

pub(crate) mod download_utils;
mod output_provider;
pub(crate) mod reconstructor;

pub use download_utils::{DownloadRangeResult, RangeDownloadSingleFlight, TermDownloadOutput, get_file_term_data_impl};
#[cfg(test)]
pub use output_provider::buffer_provider;
pub use output_provider::{
    FileProvider, SeekingOutputProvider, SequentialOutput, sequential_output_from_filepath,
    sequential_output_from_writer,
};
pub use reconstructor::FileReconstructorV1;
