use std::fs;
use std::path::PathBuf;

use chrono::Local;
use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};

/// A simple pprof-rs integration assuming only one function type is active at once.
/// We store a single global session (with a single call_name). All callers
/// share that session if it exists, or create a new one if not. Once the
/// last handle is dropped, the profiler stops and results are saved.

/// A global reference to the **current** session (if any).  
/// Since we only allow one active session name, this is just an `Option`.
const SAMPLING_FREQUENCY: i32 = 100; // 100 Hz

lazy_static::lazy_static! {
    static ref CURRENT_SESSION: ProfilingSession<'static> = ProfilingSession::new();
}

/// Holds the actual ProfilerGuard plus the associated call_name.
/// When this struct is dropped, we build the report and dump it to disk.
struct ProfilingSession<'a> {
    guard: Option<ProfilerGuard<'a>>,
}

/// In `Drop`, we stop the profiler by building a report, then save
/// the flamegraph & a protobuf (optional) to `profiles/<call_name>/<timestamp>/`.
impl ProfilingSession<'_> {
    fn save_report(&self) {
        let Some(guard) = &self.guard else {
            return;
        };

        // Build the report from the guard
        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error building profiler report: {e:?}");
                return;
            },
        };

        let date_str = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        let output_dir = PathBuf::from("profiles").join(date_str);

        let Ok(_) = fs::create_dir_all(&output_dir)
            .inspect_err(|e| eprintln!("Error creating profile output directory: {e:?}"))
        else {
            return;
        };

        // Write flamegraph.svg
        let flame_path = output_dir.join("flamegraph.svg");
        if let Ok(mut fg_file) =
            fs::File::create(&flame_path).inspect_err(|e| eprintln!("Error writing flamegraph file: {e:?}"))
        {
            let _ = report.flamegraph(&mut fg_file).inspect_err(|e| {
                eprintln!("Failed writing flamegraph: {e:?}");
            });
        }

        let pb_path = output_dir.join("pprof.pb");
        if let Ok(mut pb_file) =
            fs::File::create(pb_path).inspect_err(|e| eprintln!("Failed opening pperf out file: {e:?}"))
        {
            if let Ok(profile) = report
                .pprof()
                .inspect_err(|e| eprintln!("Error creating pprof profile report: {e:?}"))
            {
                let _ = profile.write_to_writer(&mut pb_file).inspect_err(|e| {
                    eprintln!("Failed writing protobuf perf out file: {e:?}");
                });
            }
        }

        eprintln!("Saved run profile to {output_dir:?}");
    }

    /// Create a new SessionState by starting a `ProfilerGuard`.
    fn new() -> Self {
        let guard = ProfilerGuardBuilder::default()
            .frequency(SAMPLING_FREQUENCY)
            .build()
            .inspect_err(|e| eprintln!("Warning: Failed to start profiler: {e:?}"))
            .ok();

        ProfilingSession { guard }
    }
}

pub fn start_profiler() {
    if CURRENT_SESSION.guard.is_some() {
        eprintln!("Profiler running.");
    }
}

pub fn save_profiler_report() {
    CURRENT_SESSION.save_report()
}
