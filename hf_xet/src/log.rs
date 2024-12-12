use std::env;
use std::sync::Arc;

use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use utils::ThreadPool;

use crate::log_buffer::{get_telemetry_task, LogBufferLayer, TELEMETRY_PRE_ALLOC_BYTES};

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "warn";

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "info";

pub fn initialize_logging(runtime: &tokio::runtime::Handle) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    if env::var("HF_HUB_DISABLE_TELEMETRY").as_deref() == Ok("1") {
        tracing_subscriber::registry().with(fmt_layer).with(filter_layer).init();
    } else {
        let telemetry_buffer_layer = LogBufferLayer::new(TELEMETRY_PRE_ALLOC_BYTES);
        let telemetry_task =
            get_telemetry_task(telemetry_buffer_layer.buffer.clone(), telemetry_buffer_layer.stats.clone());

        let telemetry_filter_layer =
            telemetry_buffer_layer.with_filter(FilterFn::new(|meta| meta.target() == "client_telemetry"));

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(filter_layer)
            .with(telemetry_filter_layer)
            .init();

        let _telemetry_task = runtime.spawn(telemetry_task);
    }
}
