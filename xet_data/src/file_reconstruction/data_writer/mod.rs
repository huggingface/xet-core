#[allow(clippy::module_inception)]
mod data_writer;
pub mod download_stream;
#[cfg(not(target_family = "wasm"))]
mod parallel_writer;
mod sequential_writer;
pub mod unordered_download_stream;
mod unordered_writer;

pub use data_writer::{DataFuture, DataWriter};
pub use download_stream::DownloadStream;
pub use sequential_writer::SequentialWriter;
pub use unordered_download_stream::UnorderedDownloadStream;
pub use unordered_writer::UnorderedWriter;
