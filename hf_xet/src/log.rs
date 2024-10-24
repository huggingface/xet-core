use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
/// TODO: probably change default to warn or error before shipping.
const DEFAULT_LOG_LEVEL: &str = "info";

pub fn initialize_logging() {
    // TODO: maybe have an env variable for writing to a log file instead of stderr
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();
    tracing_subscriber::registry().with(fmt_layer).with(filter_layer).init();
}
