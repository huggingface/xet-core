use std::collections::HashMap;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use io_uring::types::Fd;
use io_uring::{IoUring, Probe, opcode};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, error, warn};
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphorePermit;

use super::super::error::{FileReconstructionError, Result};
use super::super::run_state::RunState;
use super::unordered_writer::CompletedTerm;

const SQ_BACKOFF_SLEEP: Duration = Duration::from_millis(10);

/// Returns `true` if the running kernel supports io_uring with the `Write`
/// opcode. The result is cached after the first call.
pub(crate) fn io_uring_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        let Ok(ring) = IoUring::new(4) else {
            return false;
        };
        let mut probe = Probe::new();
        if ring.submitter().register_probe(&mut probe).is_err() {
            return false;
        }
        probe.is_supported(opcode::Write::CODE)
    })
}

struct InFlightOp {
    _data: Bytes,
    expected_len: usize,
    _permit: Option<AdjustableSemaphorePermit>,
}

/// Background writer thread that uses io_uring to perform positioned writes.
/// Consumed by [`tokio::task::spawn_blocking`].
struct IoUringWriterThread {
    ring: IoUring,
    fd: RawFd,
    rx: UnboundedReceiver<Result<CompletedTerm>>,
    in_flight: HashMap<u64, InFlightOp>,
    next_user_data: u64,
    bytes_written: Arc<AtomicU64>,
    run_state: Arc<RunState>,
}

impl IoUringWriterThread {
    fn new(
        ring_size: u32,
        file: &impl AsRawFd,
        rx: UnboundedReceiver<Result<CompletedTerm>>,
        bytes_written: Arc<AtomicU64>,
        run_state: Arc<RunState>,
    ) -> Result<Self> {
        let ring = IoUring::new(ring_size)
            .map_err(|e| std::io::Error::other(format!("Failed to create io_uring (ring_size={ring_size}): {e}")))?;

        Ok(Self {
            ring,
            fd: file.as_raw_fd(),
            rx,
            in_flight: HashMap::new(),
            next_user_data: 0,
            bytes_written,
            run_state,
        })
    }

    fn submit_write(&mut self, term: CompletedTerm) -> Result<()> {
        let offset = term.byte_range.start;
        let data = term.data;
        let len = data.len();

        let write_op = opcode::Write::new(Fd(self.fd), data.as_ptr(), len as u32)
            .offset(offset)
            .build()
            .user_data(self.next_user_data);

        let op = InFlightOp {
            _data: data,
            expected_len: len,
            _permit: term.permit,
        };
        self.in_flight.insert(self.next_user_data, op);
        self.next_user_data += 1;

        // Loop until the SQE is accepted. The only failure mode from push()
        // is PushError (submission queue full) -- a transient condition resolved
        // by flushing in-flight ops. Any real I/O error surfaces in
        // reap_completions, not here.
        //
        // The SubmissionQueue borrow must be dropped before calling
        // submit_and_reap, so we scope each push attempt separately.
        loop {
            let sq_full = unsafe { self.ring.submission().push(&write_op).is_err() };
            if !sq_full {
                break;
            }
            warn!("io_uring SQ full, flushing and backing off");
            self.submit_and_reap()?;
            std::thread::sleep(SQ_BACKOFF_SLEEP);
        }

        Ok(())
    }

    /// Submit all pending SQEs and wait for at least one completion to free
    /// ring slots.
    fn submit_and_reap(&mut self) -> Result<()> {
        self.ring.submit_and_wait(1)?;
        self.reap_completions()
    }

    fn reap_completions(&mut self) -> Result<()> {
        self.ring.submit()?;

        for cqe in self.ring.completion() {
            let user_data = cqe.user_data();
            let result = cqe.result();

            let Some(op) = self.in_flight.remove(&user_data) else {
                return Err(FileReconstructionError::InternalWriterError(format!(
                    "Unknown io_uring completion user_data={user_data}"
                )));
            };

            if result < 0 {
                return Err(std::io::Error::from_raw_os_error(-result).into());
            }

            let written = result as usize;
            if written != op.expected_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!("io_uring short write: expected {} bytes, wrote {written}", op.expected_len),
                )
                .into());
            }

            self.bytes_written.fetch_add(written as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    fn run(mut self) -> Result<u64> {
        loop {
            match self.rx.blocking_recv() {
                Some(Ok(term)) => {
                    self.submit_write(term)?;
                },
                Some(Err(e)) => {
                    self.run_state.set_error(e.clone());
                    return Err(e);
                },
                None => break,
            }

            // Batch: drain any additional ready messages.
            loop {
                match self.rx.try_recv() {
                    Ok(Ok(term)) => {
                        self.submit_write(term)?;
                    },
                    Ok(Err(e)) => {
                        self.run_state.set_error(e.clone());
                        return Err(e);
                    },
                    Err(_) => break,
                }
            }

            self.reap_completions()?;
        }

        // Channel closed -- all tasks have sent their data.
        // Drain remaining in-flight io_uring operations.
        while !self.in_flight.is_empty() {
            self.submit_and_reap()?;
        }

        let total = self.bytes_written.load(Ordering::Relaxed);
        debug!(total_bytes = total, "io_uring writer completed");
        Ok(total)
    }
}

/// Starts the io_uring background writer thread. Returns a `JoinHandle` that
/// resolves to the total bytes written on success.
pub(crate) fn spawn_io_uring_writer(
    ring_size: u32,
    file: std::fs::File,
    rx: UnboundedReceiver<Result<CompletedTerm>>,
    run_state: Arc<RunState>,
) -> Result<tokio::task::JoinHandle<Result<u64>>> {
    let bytes_written = Arc::new(AtomicU64::new(0));
    let writer = IoUringWriterThread::new(ring_size, &file, rx, bytes_written, run_state.clone())?;

    // Keep the File alive for the duration of the background thread so the
    // fd stays valid.
    let handle = xet_runtime::core::XetRuntime::current().spawn_blocking(move || {
        let _file_guard = file;
        let result = writer.run();
        if let Err(ref e) = result {
            error!(error = %e, "io_uring writer thread failed");
            run_state.set_error(e.clone());
        }
        result
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::time::Duration;

    use bytes::Bytes;
    use xet_client::cas_types::FileRange;
    use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphore;

    use super::super::super::data_writer::{DataFuture, DataWriter};
    use super::super::super::run_state::RunState;
    use super::super::unordered_writer::UnorderedWriter;
    use super::*;

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    fn delayed_future(data: Bytes, delay: Duration) -> DataFuture {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(data)
        })
    }

    fn read_file(path: &std::path::Path) -> Vec<u8> {
        let mut buf = Vec::new();
        std::fs::File::open(path).unwrap().read_to_end(&mut buf).unwrap();
        buf
    }

    #[tokio::test]
    async fn test_io_uring_available_probe() {
        let available = io_uring_available();
        assert!(available, "io_uring should be available on this Linux kernel");
    }

    #[tokio::test]
    async fn test_basic_io_uring_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("basic.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        let bytes = writer.finish().await.unwrap();
        assert_eq!(bytes, 11);
        assert_eq!(read_file(&path), b"Hello World");
    }

    #[tokio::test]
    async fn test_out_of_order_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ooo.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        writer
            .set_next_term_data_source(
                FileRange::new(6, 11),
                None,
                delayed_future(Bytes::from("World"), Duration::from_millis(10)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        let bytes = writer.finish().await.unwrap();
        assert_eq!(bytes, 11);
        assert_eq!(read_file(&path), b"Hello World");
    }

    #[tokio::test]
    async fn test_delayed_futures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delayed.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        let f0: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Bytes::from("Hello"))
        });
        let f1: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Bytes::from(" "))
        });
        let f2: DataFuture = Box::pin(async { Ok(Bytes::from("World")) });

        writer.set_next_term_data_source(FileRange::new(0, 5), None, f0).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(5, 6), None, f1).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(6, 11), None, f2).await.unwrap();

        let bytes = writer.finish().await.unwrap();
        assert_eq!(bytes, 11);
        assert_eq!(read_file(&path), b"Hello World");
    }

    #[tokio::test]
    async fn test_size_mismatch_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mismatch.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        writer
            .set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_future_error_propagates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("err.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        let failing: DataFuture =
            Box::pin(async { Err(FileReconstructionError::InternalError("test error".to_string())) });

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, failing)
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_semaphore_permits_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("permits.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();
        let semaphore = AdjustableSemaphore::new(2, (0, 2));

        let p1 = semaphore.acquire().await.unwrap();
        let p2 = semaphore.acquire().await.unwrap();
        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(p1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), Some(p2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        writer.finish().await.unwrap();
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_many_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        let num_terms: usize = 200;
        let mut expected = vec![0u8; 0];
        let mut offset = 0u64;

        for i in 0..num_terms {
            let size = 100 + (i % 50) * 10;
            let data: Vec<u8> = (0..size).map(|j| ((i * 7 + j * 13) % 256) as u8).collect();
            expected.extend_from_slice(&data);

            let bytes = Bytes::from(data);
            let delay = Duration::from_micros((i % 10) as u64 * 100);
            writer
                .set_next_term_data_source(
                    FileRange::new(offset, offset + size as u64),
                    None,
                    delayed_future(bytes, delay),
                )
                .await
                .unwrap();
            offset += size as u64;
        }

        let bytes_written = writer.finish().await.unwrap();
        assert_eq!(bytes_written, offset);
        assert_eq!(read_file(&path), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_exceeds_ring_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow.bin");
        let file = std::fs::File::create(&path).unwrap();

        let run_state = RunState::new_for_test();
        let writer = UnorderedWriter::new_io_uring(64, file, run_state).unwrap();

        // Submit more writes than RING_SIZE (64) to exercise SQ backpressure.
        let num_terms: usize = 200;
        let chunk_size = 1024usize;
        let mut expected = Vec::with_capacity(num_terms * chunk_size);

        for i in 0..num_terms {
            let data: Vec<u8> = (0..chunk_size).map(|j| ((i + j) % 256) as u8).collect();
            expected.extend_from_slice(&data);
            let start = (i * chunk_size) as u64;
            writer
                .set_next_term_data_source(
                    FileRange::new(start, start + chunk_size as u64),
                    None,
                    immediate_future(Bytes::from(data)),
                )
                .await
                .unwrap();
        }

        let bytes_written = writer.finish().await.unwrap();
        assert_eq!(bytes_written as usize, num_terms * chunk_size);
        assert_eq!(read_file(&path), expected);
    }

    // ── Tuning benchmark ───────────────────────────────────────────────

    use std::time::Instant;

    use rand::Rng;

    use super::super::sequential_writer::SequentialWriter;

    const KB: usize = 1024;
    const MB: usize = 1024 * KB;

    struct BenchConfig {
        label: &'static str,
        writer: WriterKind,
        chunk_size: usize,
        max_delay_ms: u64,
    }

    #[derive(Clone, Copy)]
    enum WriterKind {
        Sequential,
        Vectored,
        IoUring(u32),
    }

    struct BenchResult {
        label: String,
        avg_ms: f64,
        throughput_mbs: f64,
    }

    fn make_data_future(data: Bytes, max_delay_ms: u64) -> DataFuture {
        if max_delay_ms == 0 {
            Box::pin(async move { Ok(data) })
        } else {
            Box::pin(async move {
                let delay_ms = rand::rng().random_range(0..=max_delay_ms);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                Ok(data)
            })
        }
    }

    async fn run_trial(
        file_size: usize,
        writer_kind: WriterKind,
        chunk_size: usize,
        max_delay_ms: u64,
    ) -> f64 {
        let num_chunks = file_size / chunk_size;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench.bin");
        let file = std::fs::File::create(&path).unwrap();
        let run_state = RunState::new_for_test();

        let writer: Box<dyn DataWriter> = match writer_kind {
            WriterKind::Sequential => SequentialWriter::new(file, false, run_state),
            WriterKind::Vectored => SequentialWriter::new(file, true, run_state),
            WriterKind::IoUring(ring_size) => {
                UnorderedWriter::new_io_uring(ring_size, file, run_state).unwrap()
            },
        };

        let start = Instant::now();
        let mut offset = 0u64;
        for i in 0..num_chunks {
            let data = Bytes::from(vec![(i & 0xff) as u8; chunk_size]);
            let end = offset + chunk_size as u64;
            writer
                .set_next_term_data_source(
                    FileRange::new(offset, end),
                    None,
                    make_data_future(data, max_delay_ms),
                )
                .await
                .unwrap();
            offset = end;
        }
        writer.finish().await.unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    }

    fn bench_trials(times: &mut Vec<f64>, iterations: usize) -> f64 {
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let trimmed = if iterations > 4 {
            &times[1..iterations - 1]
        } else {
            &times[..]
        };
        trimmed.iter().sum::<f64>() / trimmed.len() as f64
    }

    async fn run_bench_suite(configs: &[BenchConfig], file_size: usize, iterations: usize) -> Vec<BenchResult> {
        let mut results = Vec::new();

        for cfg in configs {
            let mut times = Vec::new();
            for _ in 0..iterations {
                times.push(run_trial(file_size, cfg.writer, cfg.chunk_size, cfg.max_delay_ms).await);
            }
            let avg = bench_trials(&mut times, iterations);
            let mbs = (file_size as f64 / MB as f64) / (avg / 1000.0);

            let delay_str = if cfg.max_delay_ms > 0 {
                format!(" d={}ms", cfg.max_delay_ms)
            } else {
                String::new()
            };
            let label = format!("{}{}", cfg.label, delay_str);
            println!(
                "  {:<35} {:>10.1} {:>10.1}",
                label, avg, mbs
            );

            results.push(BenchResult {
                label,
                avg_ms: avg,
                throughput_mbs: mbs,
            });
        }
        results
    }

    /// Comprehensive io_uring benchmark including large writes and out-of-order delivery.
    ///
    /// Run with:
    /// ```sh
    /// cargo test -p xet-data --release --lib io_uring_writer::tests::tuning_benchmark -- --ignored --nocapture
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn tuning_benchmark() {
        let iterations = 7;

        for &file_size in &[512 * MB, 1024 * MB] {
            println!("\n{}", "=".repeat(72));
            println!(
                "  io_uring benchmark: {} MB, {} iterations",
                file_size / MB,
                iterations
            );
            println!("{}", "=".repeat(72));

            // ── Phase 1: Immediate delivery (no delay) ──────────────────
            println!("\n  ── Immediate delivery (no random delay) ──");
            println!("  {:<35} {:>10} {:>10}", "Config", "Avg(ms)", "MB/s");

            let immediate_configs = vec![
                BenchConfig { label: "seq 64K", writer: WriterKind::Sequential, chunk_size: 64 * KB, max_delay_ms: 0 },
                BenchConfig { label: "vec 64K", writer: WriterKind::Vectored, chunk_size: 64 * KB, max_delay_ms: 0 },
                BenchConfig { label: "uring 64K r=128", writer: WriterKind::IoUring(128), chunk_size: 64 * KB, max_delay_ms: 0 },
                BenchConfig { label: "seq 1M", writer: WriterKind::Sequential, chunk_size: 1 * MB, max_delay_ms: 0 },
                BenchConfig { label: "vec 1M", writer: WriterKind::Vectored, chunk_size: 1 * MB, max_delay_ms: 0 },
                BenchConfig { label: "uring 1M r=128", writer: WriterKind::IoUring(128), chunk_size: 1 * MB, max_delay_ms: 0 },
                BenchConfig { label: "seq 4M", writer: WriterKind::Sequential, chunk_size: 4 * MB, max_delay_ms: 0 },
                BenchConfig { label: "vec 4M", writer: WriterKind::Vectored, chunk_size: 4 * MB, max_delay_ms: 0 },
                BenchConfig { label: "uring 4M r=128", writer: WriterKind::IoUring(128), chunk_size: 4 * MB, max_delay_ms: 0 },
            ];
            run_bench_suite(&immediate_configs, file_size, iterations).await;

            // ── Phase 2: Random delay (simulates out-of-order network arrival) ──
            for &max_delay in &[5u64, 20, 50] {
                println!(
                    "\n  ── Random delay 0..{}ms (out-of-order arrival) ──",
                    max_delay
                );
                println!("  {:<35} {:>10} {:>10}", "Config", "Avg(ms)", "MB/s");

                let delay_configs = vec![
                    BenchConfig { label: "seq 64K", writer: WriterKind::Sequential, chunk_size: 64 * KB, max_delay_ms: max_delay },
                    BenchConfig { label: "vec 64K", writer: WriterKind::Vectored, chunk_size: 64 * KB, max_delay_ms: max_delay },
                    BenchConfig { label: "uring 64K r=128", writer: WriterKind::IoUring(128), chunk_size: 64 * KB, max_delay_ms: max_delay },
                    BenchConfig { label: "seq 1M", writer: WriterKind::Sequential, chunk_size: 1 * MB, max_delay_ms: max_delay },
                    BenchConfig { label: "vec 1M", writer: WriterKind::Vectored, chunk_size: 1 * MB, max_delay_ms: max_delay },
                    BenchConfig { label: "uring 1M r=128", writer: WriterKind::IoUring(128), chunk_size: 1 * MB, max_delay_ms: max_delay },
                    BenchConfig { label: "seq 4M", writer: WriterKind::Sequential, chunk_size: 4 * MB, max_delay_ms: max_delay },
                    BenchConfig { label: "vec 4M", writer: WriterKind::Vectored, chunk_size: 4 * MB, max_delay_ms: max_delay },
                    BenchConfig { label: "uring 4M r=128", writer: WriterKind::IoUring(128), chunk_size: 4 * MB, max_delay_ms: max_delay },
                ];
                run_bench_suite(&delay_configs, file_size, iterations).await;
            }
        }
    }

    fn format_size(bytes: usize) -> String {
        if bytes >= MB {
            format!("{}M", bytes / MB)
        } else if bytes >= KB {
            format!("{}K", bytes / KB)
        } else {
            format!("{}B", bytes)
        }
    }
}
