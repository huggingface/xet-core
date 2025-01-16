// Enables an easy way to profile xet-core operations.  Internally, the code uses pprof-rs, which links in the
// GPerfTools suite.
//
// To use, compile with --features=profiling; with maturin, install with `maturin develop --release
// --features=profiling`
//
// When running, it saves the profile to profilers/<timestamp>, writing both a gperf protobuf file and a flamegraph svg
// file. The protobuf file can be read using the pprof tool:
//
// ```bash
// go install github.com/google/pprof@latest  # Installs to ~/go/bin/
// # Replace this with the correct path to the hf_xet library file.
// ~/go/bin/pprof  ./venv/lib/python3.10/site-packages/hf_xet/hf_xet.abi3.so  profiles/<timestamp>/pprof.pb
// ```
//
// On OSX, I like using qcachegrind as the interactive visualizer of the results, which requires the profile data to be
// exported to the callgrind format:
//
// ```bash
// brew install qcachegrind     # If needed
// ~/go/bin/pprof  -callgrind ./venv/lib/python3.10/site-packages/hf_xet/hf_xet.abi3.so  profiles/<timestamp>/pprof.pb
// qcachegrind <reported callgrind output file>
// ```

use std::fs;
use std::path::PathBuf;

use chrono::Local;
use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};

const SAMPLING_FREQUENCY: i32 = 100; // 100 Hz

// A global reference to the current profiling session.  The python at_exit function dumps this out.
lazy_static::lazy_static! {
    static ref CURRENT_SESSION: ProfilingSession<'static> = ProfilingSession::new();
}

struct ProfilingSession<'a> {
    guard: Option<ProfilerGuard<'a>>,
}

impl ProfilingSession<'_> {
    /// Create a new SessionState by starting a `ProfilerGuard`.
    fn new() -> Self {
        let guard = ProfilerGuardBuilder::default()
            .frequency(SAMPLING_FREQUENCY)
            .build()
            .inspect_err(|e| eprintln!("Profiler: Warning: Failed to start profiler: {e:?}"))
            .ok();

        ProfilingSession { guard }
    }

    fn save_report(&self) {
        let Some(guard) = &self.guard else {
            return;
        };

        // Build the report from the guard
        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Profiler: Error building profiler report: {e:?}");
                return;
            },
        };

        let date_str = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        let output_dir = PathBuf::from("profiles").join(date_str);

        let Ok(_) = fs::create_dir_all(&output_dir)
            .inspect_err(|e| eprintln!("Profiler: Error creating profile output directory: {e:?}"))
        else {
            return;
        };

        // Write flamegraph.svg
        let flame_path = output_dir.join("flamegraph.svg");
        if let Ok(mut fg_file) =
            fs::File::create(&flame_path).inspect_err(|e| eprintln!("Profiler: Error writing flamegraph file: {e:?}"))
        {
            let _ = report.flamegraph(&mut fg_file).inspect_err(|e| {
                eprintln!("Profiler: Failed writing flamegraph: {e:?}");
            });
        }

        let pb_path = output_dir.join("pprof.pb");
        if let Ok(mut pb_file) =
            fs::File::create(pb_path).inspect_err(|e| eprintln!("Profiler: Failed opening pperf out file: {e:?}"))
        {
            if let Ok(profile) = report
                .pprof()
                .inspect_err(|e| eprintln!("Profiler: Error creating pprof profile report: {e:?}"))
            {
                let _ = profile.write_to_writer(&mut pb_file).inspect_err(|e| {
                    eprintln!("Profiler: Failed writing protobuf gperf out file: {e:?}");
                });
            }
        }

        eprintln!("Profiler: Saved run profile to {output_dir:?}");
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
