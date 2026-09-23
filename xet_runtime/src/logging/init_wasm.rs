//! Browser-console logging backend, the wasm counterpart to [`super::init`].
//!
//! Only [`LoggingMode::Console`] is meaningful here: wasm32-unknown-unknown has no
//! filesystem, so the rolling-file and directory-cleanup machinery in `init.rs` has
//! nothing to write to and is not compiled for this target.

use std::io;

use tracing::{Level, Metadata, info};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use wasm_bindgen::JsValue;

use super::config::LoggingConfig;
use super::constants::DEFAULT_LOG_LEVEL_WASM_CONSOLE;

/// Collects one formatted event, then hands it to the browser console.
///
/// The `fmt` layer writes an event and drops the writer without calling `flush`, so
/// the console call happens in `Drop`.
struct ConsoleWriter {
    level: Level,
    buf: Vec<u8>,
}

impl io::Write for ConsoleWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        let text = String::from_utf8_lossy(&self.buf);
        let message = JsValue::from_str(text.trim_end());

        // Map onto the console's own levels so devtools filtering and the
        // warning/error counters work on xet-core output.
        match self.level {
            Level::ERROR => web_sys::console::error_1(&message),
            Level::WARN => web_sys::console::warn_1(&message),
            Level::INFO => web_sys::console::info_1(&message),
            Level::DEBUG => web_sys::console::log_1(&message),
            Level::TRACE => web_sys::console::debug_1(&message),
        }

        self.buf.clear();
        Ok(())
    }
}

impl Drop for ConsoleWriter {
    fn drop(&mut self) {
        let _ = io::Write::flush(self);
    }
}

struct MakeConsoleWriter;

impl<'a> MakeWriter<'a> for MakeConsoleWriter {
    type Writer = ConsoleWriter;

    fn make_writer(&'a self) -> Self::Writer {
        ConsoleWriter {
            level: Level::INFO,
            buf: Vec::new(),
        }
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        ConsoleWriter {
            level: *meta.level(),
            buf: Vec::new(),
        }
    }
}

/// The main entry point to set up logging.  Should only be called once; later calls
/// leave the installed subscriber alone.
pub fn init(cfg: LoggingConfig) {
    // `RUST_LOG` is always absent on wasm32-unknown-unknown, but reading it keeps the
    // behaviour identical on wasi and under `wasm-bindgen-test`, which do have an env.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL_WASM_CONSOLE))
        .unwrap_or_default();

    // No timestamp: `SystemTime::now` panics on wasm32-unknown-unknown, and the console
    // stamps every line itself. No ANSI either - devtools prints the escapes literally.
    let fmt_layer = fmt::layer()
        .without_time()
        .with_ansi(false)
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .with_writer(MakeConsoleWriter);

    let installed = if cfg.use_json {
        tracing_subscriber::registry()
            .with(fmt_layer.json().with_filter(filter))
            .try_init()
    } else {
        tracing_subscriber::registry().with(fmt_layer.with_filter(filter)).try_init()
    };

    if installed.is_err() {
        return;
    }

    info!("{}, xet-core revision {}", &cfg.version, git_version::git_version!(fallback = "unknown"));

    // See the note in `init.rs`: the values are computed while building the config,
    // before any subscriber exists, so they have to be emitted here.
    crate::utils::system_memory::log_derived_defaults();
}
