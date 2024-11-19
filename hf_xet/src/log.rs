use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::Config;
use opentelemetry_sdk::Resource;
use tracing_opentelemetry::OpenTelemetryLayer;
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
        .with_test_writer()
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(Config::default().with_resource(Resource::new(vec![KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            "xet-core",
        )])))
        .install_batch(opentelemetry_sdk::runtime::TokioCurrentThread)
        // .install_simple()
        .unwrap()
        .tracer("xet-core");

    let otel_layer = OpenTelemetryLayer::new(tracer);

    if tracing_subscriber::registry()
        .with(fmt_layer)
        .with(filter_layer)
        .with(otel_layer)
        .try_init()
        .is_err()
    {
        eprintln!("Unable to initialise tracing");
    }
}
