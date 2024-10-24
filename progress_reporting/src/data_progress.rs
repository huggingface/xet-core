use std::io::{stderr, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossterm::{cursor, QueueableCommand};
use utils::output_bytes;

const MAX_PRINT_INTERVAL_MS: u64 = 250;

#[derive(Debug, Default)]
struct DPRPrintInfo {
    last_write_length: usize,
    buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct DataProgressReporter {
    is_active: AtomicBool,
    disable: bool,
    total_count: AtomicUsize,
    total_bytes: AtomicUsize,
    current_count: AtomicUsize,
    current_bytes: AtomicUsize,
    message: String,
    time_at_start: Instant,
    last_print_time: AtomicU64, // In miliseconds since time_at_start

    // Also used as a lock around the printing.
    print_info: Mutex<DPRPrintInfo>,
}

impl DataProgressReporter {
    pub fn new(message: &str, total_unit_count: Option<usize>, total_byte_count: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            is_active: AtomicBool::new(true),
            disable: atty::isnt(atty::Stream::Stderr),
            total_count: AtomicUsize::new(total_unit_count.unwrap_or(0)),
            total_bytes: AtomicUsize::new(total_byte_count.unwrap_or(0)),
            current_count: AtomicUsize::new(0),
            current_bytes: AtomicUsize::new(0),
            message: message.to_owned(),
            time_at_start: Instant::now(),
            last_print_time: AtomicU64::new(0),
            print_info: Mutex::new(<_>::default()),
        })
    }

    pub fn new_inactive(message: &str, total_unit_count: Option<usize>, total_byte_count: Option<usize>) -> Arc<Self> {
        let s = Self::new(message, total_unit_count, total_byte_count);
        s.is_active.store(false, Ordering::Relaxed);
        s
    }

    pub fn set_active(&self, active_flag: bool) {
        self.is_active.store(active_flag, Ordering::Relaxed);
    }

    /// Adds progress into the register, printing the result.
    ///
    /// There are two quantities here, unit_amount and bytes.  If total_unit_count was given
    /// initially, then unit_amount should be specified to indicate how many new units were completed.
    /// Note that if no total count was specified, then this should be None.  
    ///
    /// bytes indicates the number of bytes completed along with these units.
    ///
    ///
    /// Example 1:
    ///  
    /// let mut pb = DataProgressReporter::new("Testing progress bar", Some(10));
    /// pb.register_progress(Some(1), 25 * 1024);
    /// pb.register_progress(Some(8), 50 * 1024);
    /// pb.finalize();
    ///
    ///
    /// This will update the progress display line to, in order,
    ///
    /// Testing progress bar: (1/10), 25 KiB | 25 KiB/s.
    /// Testing progress bar: (9/10), 75 KiB | 75 KiB/s.
    /// Testing progress bar: (10/10), 75 KiB | 75 KiB/s, done.
    ///
    ///
    /// Example 2:
    ///
    ///
    /// let mut pb = DataProgressReporter::new("Testing progress bar, bytes only", None);
    /// pb.register_progress(None, 25 * 1024);
    /// pb.register_progress(None, 50 * 1024);
    /// pb.finalize();
    ///
    /// This will update the progress display line to, in order,
    ///
    /// Testing progress bar, bytes only: 25 KiB | 25 KiB/s.
    /// Testing progress bar, bytes only: 75 KiB | 75 KiB/s.
    /// Testing progress bar, bytes only: 75 KiB | 75 KiB/s, done.
    pub fn register_progress(&self, unit_amount: Option<usize>, bytes: Option<usize>) {
        if let Some(c) = unit_amount {
            self.current_count.fetch_add(c, Ordering::Relaxed);
        }

        if let Some(b) = bytes {
            self.current_bytes.fetch_add(b, Ordering::Relaxed);
        }

        let _ = self.print(false);
    }

    /// Sometimes, when we're scanning things, this can be a moving target
    pub fn update_target(&self, unit_delta_amount: Option<usize>, byte_delta: Option<usize>) {
        if let Some(c) = unit_delta_amount {
            self.total_count.fetch_add(c, Ordering::Relaxed);
        }

        if let Some(b) = byte_delta {
            self.total_bytes.fetch_add(b, Ordering::Relaxed);
        }
    }

    pub fn set_progress(&self, unit_amount: Option<usize>, bytes: Option<usize>) {
        if let Some(c) = unit_amount {
            self.current_count.store(c, Ordering::Relaxed);
        }

        if let Some(b) = bytes {
            self.current_bytes.store(b, Ordering::Relaxed);
        }

        let _ = self.print(false);
    }

    /// Call when done with all progress.
    pub fn finalize(&self) {
        let _ = self.print(true);
    }

    /// Does the actual printing
    fn print(&self, is_final: bool) -> std::result::Result<(), std::io::Error> {
        // Put a minimum width for the line we print.  This is based on git's messages,
        // which can be longer than ours, causing weird inteleaving when the user does
        // stuff to the input (hits a new line, etc.)  So, print a minimum buffer of spaces.
        const PRINT_LINE_MIN_WIDTH: usize = 74;

        if self.disable || !self.is_active.load(Ordering::Relaxed) {
            return Ok(());
        }

        let elapsed_time = Instant::now().duration_since(self.time_at_start);

        let elapsed_millis = elapsed_time.as_millis().min(u64::MAX as u128) as u64;

        let last_print_time = self.last_print_time.load(Ordering::Relaxed);

        if !is_final && last_print_time != 0 && elapsed_millis < last_print_time + MAX_PRINT_INTERVAL_MS {
            return Ok(());
        }

        // Acquire the print lock
        let Ok(mut lg_print_info) = self.print_info.lock() else {
            return Ok(());
        };

        // get the last print time
        let last_print_time = self.last_print_time.load(Ordering::Relaxed);

        // Is this condition still valid?  Could have been updated while waiting for the lock.
        if !is_final && last_print_time != 0 && elapsed_millis < last_print_time + MAX_PRINT_INTERVAL_MS {
            return Ok(());
        }

        const WHITESPACE: &str = "                                                    ";
        let current_bytes = self.current_bytes.load(Ordering::Relaxed);
        let current_count = self.current_count.load(Ordering::Relaxed);

        // Now, get the info.
        let byte_rate = (1000 * current_bytes) / (usize::max(1000, elapsed_millis as usize));

        let mut write_str = match (self.total_count.load(Ordering::Relaxed), self.total_bytes.load(Ordering::Relaxed)) {
            (0 | 1, 0) => {
                match (current_count, current_bytes) {
                    (0, 0) => {
                        // Things are still pending, so just print ... to indicate this.

                        // No information yet.
                        // EX:  Uploading: ...
                        format!("{}: ...", self.message)
                    },
                    (0, b) => {
                        // Just the number of bytes transferred so far.
                        // EX:  Uploading: 2.5MB | 1MB/s.
                        format!(
                            "{}: {} | {}/s{}",
                            self.message,
                            &output_bytes(b),
                            &output_bytes(byte_rate),
                            if is_final { ", done." } else { "." }
                        )
                    },
                    (c, 0) => {
                        // Just the number of units completed, no info on total.
                        // EX:  Scanning Directories: 23 completed.
                        format!("{}: {} completed{}", self.message, c, if is_final { ", done." } else { "." })
                    },
                    (c, b) => {
                        // Number of units completed and number of bytes, no info on totals:
                        // EX: Downloading: 45/??, 210 MB | 32MB/s.
                        format!(
                            "{}: {} / {}, {} | {}/s{}",
                            self.message,
                            c,
                            if is_final { format!("{c}") } else { "??".to_owned() },
                            &output_bytes(b),
                            &output_bytes(byte_rate),
                            if is_final { ", done." } else { "." }
                        )
                    },
                }
            },
            (0 | 1, total_bytes) => {
                // Number of bytes completed and total bytes.
                // EX: Downloading: (210 MB / 1.2 GB) | 32MB/s.

                format!(
                    "{}: ({} / {}) | {}/s{}",
                    self.message,
                    &output_bytes(if is_final { total_bytes } else { current_bytes }),
                    &output_bytes(total_bytes),
                    &output_bytes(byte_rate),
                    if is_final { ", done." } else { "." }
                )
            },
            (total_count, 0) => {
                if current_bytes != 0 {
                    // Number of units completed and bytes completed, no total byte information.
                    // EX: Downloading: (750 / 1001), 453MB | 23MB/s.
                    format!(
                        "{}: ({} / {}), {} | {}/s{}",
                        self.message,
                        if is_final { total_count } else { current_count },
                        total_count,
                        &output_bytes(current_bytes),
                        &output_bytes(byte_rate),
                        if is_final { ", done." } else { "." }
                    )
                } else {
                    // Number of units completed and total count, no byte information.
                    // EX: Scanning: (750 / 1001).
                    format!(
                        "{}: ({} / {}){}",
                        self.message,
                        if is_final { total_count } else { current_count },
                        total_count,
                        if is_final { ", done." } else { "." }
                    )
                }
            },
            (total_count, total_bytes) => {
                // Number of units completed and bytes completed, total byte information (but total not used here..
                // EX: Downloading: (750 / 1001), 453MB | 23MB/s.

                format!(
                    "{}: ({} / {}), {} | {}/s{}",
                    self.message,
                    if is_final { total_count } else { current_count },
                    total_count,
                    &output_bytes(if is_final { total_bytes } else { current_bytes }),
                    &output_bytes(byte_rate),
                    if is_final { ", done." } else { "." }
                )
            },
        };

        let write_str_pad_len = lg_print_info.last_write_length.max(PRINT_LINE_MIN_WIDTH);

        if write_str.len() < write_str_pad_len {
            let mut len_to_write = write_str_pad_len - write_str.len();

            loop {
                write_str.push_str(&WHITESPACE[..usize::min(len_to_write, WHITESPACE.len())]);
                if len_to_write > WHITESPACE.len() {
                    len_to_write -= WHITESPACE.len();
                    continue;
                } else {
                    break;
                }
            }
        }

        // Annoyingly, on windows prior to Windows 11, cursor movement is done as soon as the buffer

        lg_print_info.last_write_length = write_str.len();

        let use_buffer = {
            #[cfg(windows)]
            {
                crossterm::ansi_support::supports_ansi()
            }
            #[cfg(not(windows))]
            {
                true
            }
        };

        if use_buffer {
            lg_print_info.buffer.clear();
        }

        let mut stderr = stderr();

        // We haven't printed anything yet; it's all in the write_str
        if last_print_time == 0 {
            // Move the cursor to the next line.  We can't seem to use cursor::MoveToNextLine
            // as it doesn't seem to add anything to the terminal buffer.  Doing a newline like
            // this seems to work fine.
            if use_buffer {
                lg_print_info.buffer.write_all("\n".as_bytes())?;
            } else {
                stderr.write_all("\n".as_bytes())?;
            }
        }
        if use_buffer {
            lg_print_info.buffer.queue(cursor::SavePosition)?;
            lg_print_info.buffer.queue(cursor::MoveToPreviousLine(1))?;

            lg_print_info.buffer.queue(cursor::MoveToColumn(0)).unwrap();

            lg_print_info.buffer.write_all(write_str.as_bytes())?;

            lg_print_info.buffer.queue(cursor::RestorePosition)?;

            // Finally, flush it out.
            stderr.write_all(&lg_print_info.buffer)?;
        } else {
            stderr.queue(cursor::SavePosition)?;
            stderr.queue(cursor::MoveToPreviousLine(1))?;

            stderr.queue(cursor::MoveToColumn(0)).unwrap();

            stderr.write_all(write_str.as_bytes())?;

            stderr.queue(cursor::RestorePosition)?;
        }

        self.last_print_time.store(elapsed_millis + 1, Ordering::Relaxed);

        Ok(())
    }
}
