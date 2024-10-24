use std::io::Write;
use std::sync::Arc;

use crate::DataProgressReporter;

pub struct ReportedWriter<W: Write> {
    writer: W,
    progress_reporter: Option<Arc<DataProgressReporter>>,
}

impl<W: Write> ReportedWriter<W> {
    pub fn new(writer: W, progress_reporter: &Option<Arc<DataProgressReporter>>) -> Self {
        Self {
            writer,
            progress_reporter: progress_reporter.as_ref().cloned(),
        }
    }

    fn report(&self, s: usize) {
        if let Some(pr) = self.progress_reporter.as_ref() {
            pr.register_progress(None, Some(s));
        }
    }
}

impl<W: Write> Write for ReportedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.writer.write(buf)?;
        self.report(res);
        Ok(res)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(buf)?;
        self.report(buf.len());
        Ok(())
    }
}
