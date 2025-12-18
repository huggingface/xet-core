use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::data_writer::DataWriter;
use crate::data_writer::reordering_writer::ReorderingWriter;

pub enum DataOutput {
    SequentialWriter(Box<dyn Write + Send>),
    File(PathBuf),
}

impl DataOutput {
    pub fn get_writer(self, _config: &xet_config::ReconstructionConfig) -> Arc<dyn DataWriter> {
        match self {
            DataOutput::SequentialWriter(writer) => Arc::new(ReorderingWriter::new(writer)),
            DataOutput::File(path) => {
                let file = std::fs::File::create(&path).expect("Failed to create file");
                Arc::new(ReorderingWriter::new(Box::new(file)))
            },
        }
    }
}
