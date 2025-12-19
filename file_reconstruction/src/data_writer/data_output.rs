use std::io::Write;
use std::path::PathBuf;

pub enum DataOutput {
    SequentialWriter(Box<dyn Write + Send>),
    File(PathBuf),
}
