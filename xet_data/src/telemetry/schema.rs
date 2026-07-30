//! Generates the committed telemetry schema from the payload structs.
//!
//! The Rust types in [`payload`](super::payload) are the single source of truth;
//! `telemetry/metrics.schema.json` is a generated export of them. `schemars` is a dev-dependency
//! and the `JsonSchema` derives are `cfg(test)`, so none of this exists in a release build.
//!
//! # Regenerating
//!
//! ```text
//! UPDATE_TELEMETRY_SCHEMA=1 cargo test -p xet-data --lib telemetry::schema
//! ```
//!
//! Without that variable the same test *asserts* the committed file matches, so changing a payload
//! struct without committing the regenerated schema fails the build rather than silently shipping
//! a stale contract.
//!
//! # Scope
//!
//! This publishes *what the client emits* - the metric names, their JSON types, and what each one
//! means. How the receiving service stores, indexes, or aggregates those documents is its own
//! concern and is deliberately not described here: this repo is public, and it has no way to keep
//! a description of someone else's storage layer correct.
//!
//! A consumer that needs to type its own storage derives that from each property's `type`. The
//! schema states the compatibility rules that make doing so safe.
//!
//! # Why every field must be documented
//!
//! The generated schema is the only definition of this vocabulary that anyone outside this repo
//! has. A field whose doc comment is missing arrives there with no `description`, and is then
//! undocumented everywhere - so [`test_every_property_is_documented`] fails the build instead.
//!
//! [`test_every_property_is_documented`]: tests::test_every_property_is_documented

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde_json::{Value, json};

use super::payload::{DownloadMetrics, UploadMetrics};

/// Committed artifact: the JSON Schema for both directions.
const SCHEMA_PATH: &str = "telemetry/metrics.schema.json";

/// Set this to rewrite the schema instead of asserting on it.
const UPDATE_ENV: &str = "UPDATE_TELEMETRY_SCHEMA";

/// Repo root, derived from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xet_data has a parent")
        .to_path_buf()
}

fn schema_of<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("schemars emits valid JSON")
}

/// Strips the per-schema preamble so the two definitions nest cleanly under `$defs`.
fn as_definition(mut schema: Value) -> Value {
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
    }
    schema
}

/// The combined JSON Schema document.
fn metrics_json_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://raw.githubusercontent.com/huggingface/xet-core/main/telemetry/metrics.schema.json",
        "title": "Xet client transfer telemetry metrics",
        "description":
            "The `metrics` object of a POST /v1/telemetry document, one shape per transfer \
             direction. This file is the source of truth for the metric vocabulary: every property \
             carries a description of what it measures and its unit.\n\n\
             Generated from xet_data/src/telemetry/payload.rs - edit the Rust structs, not this \
             file, and regenerate with `UPDATE_TELEMETRY_SCHEMA=1 cargo test -p xet-data --lib \
             telemetry::schema`.\n\n\
             Compatibility: adding a property is backward compatible. Removing one, or changing \
             its type, is NOT - consumers typically assign a column or field type on first sight \
             and cannot change it in place afterwards. If a metric's meaning or unit changes, add \
             a new property rather than repurposing the existing one (`duration_us` alongside \
             `duration_ms`, not `duration_ms` becoming a float). CI enforces this.\n\n\
             Every value is a scalar - never null, never nested, never an array. Integers fit in \
             an unsigned 64-bit integer; numbers are finite doubles. No file names, paths, hashes, \
             repository ids, or user ids appear anywhere in this vocabulary.",
        "oneOf": [
            { "$ref": "#/$defs/UploadMetrics" },
            { "$ref": "#/$defs/DownloadMetrics" },
        ],
        "$defs": {
            "UploadMetrics": as_definition(schema_of::<UploadMetrics>()),
            "DownloadMetrics": as_definition(schema_of::<DownloadMetrics>()),
        },
    })
}

/// Every property across both directions, mapped to its JSON Schema type.
///
/// `BTreeMap` for a stable, diffable ordering. A property present in both directions must agree on
/// its type: a consumer storing both in one place could not represent a disagreement.
fn all_metric_types() -> BTreeMap<String, String> {
    let mut merged: BTreeMap<String, String> = BTreeMap::new();

    for schema in [schema_of::<UploadMetrics>(), schema_of::<DownloadMetrics>()] {
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| panic!("metrics schema has no properties; did serde(flatten) stop inlining?"));

        for (name, property) in properties {
            let ty = property
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("metric {name:?} has no JSON Schema type"))
                .to_owned();

            if let Some(existing) = merged.insert(name.clone(), ty.clone()) {
                assert_eq!(
                    existing, ty,
                    "metric {name:?} is {existing} in one direction and {ty} in the other; a \
                     consumer storing both in one place could not represent that"
                );
            }
        }
    }

    merged
}

/// Serializes with a trailing newline, so the file is well-formed for git and editors.
fn to_pretty(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("the generated schema serializes");
    text.push('\n');
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walks every property of both definitions.
    fn each_property(schema: &Value, mut visit: impl FnMut(&str, &str, &Value)) {
        for definition in ["UploadMetrics", "DownloadMetrics"] {
            let properties = schema["$defs"][definition]["properties"]
                .as_object()
                .unwrap_or_else(|| panic!("{definition} has no properties"));
            for (name, spec) in properties {
                visit(definition, name, spec);
            }
        }
    }

    #[test]
    fn test_committed_schema_is_current() {
        let path = repo_root().join(SCHEMA_PATH);
        let generated = to_pretty(&metrics_json_schema());

        if std::env::var_os(UPDATE_ENV).is_some() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create telemetry/ directory");
            }
            std::fs::write(&path, &generated).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            return;
        }

        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "cannot read {}: {e}\n\nGenerate it with:\n    {UPDATE_ENV}=1 cargo test -p xet-data --lib telemetry::schema",
                path.display()
            )
        });

        assert_eq!(
            committed, generated,
            "\n{SCHEMA_PATH} is out of date with the payload structs.\n\nRegenerate with:\n    \
             {UPDATE_ENV}=1 cargo test -p xet-data --lib telemetry::schema\n\nThen review the diff: \
             adding a property is backward compatible, but removing one or changing its type is a \
             breaking change for consumers.\n"
        );
    }

    /// The schema is the only definition of this vocabulary outside this repo, so a property
    /// without a description is a metric nobody can interpret.
    #[test]
    fn test_every_property_is_documented() {
        let schema = metrics_json_schema();
        let mut undocumented = Vec::new();

        each_property(&schema, |definition, name, spec| {
            let described = spec
                .get("description")
                .and_then(Value::as_str)
                .is_some_and(|d| !d.trim().is_empty());
            if !described {
                undocumented.push(format!("{definition}.{name}"));
            }
        });

        assert!(
            undocumented.is_empty(),
            "these metrics have no description, so they arrive undocumented for every consumer: \
             {undocumented:?}\n\nAdd a `///` doc comment to the field in payload.rs and regenerate."
        );
    }

    /// Only scalars. A nested or nullable value would break consumers that flatten the object into
    /// columns, and `serde_json` renders NaN and infinity as null.
    #[test]
    fn test_every_property_is_a_scalar() {
        let schema = metrics_json_schema();

        each_property(&schema, |definition, name, spec| {
            let ty = spec.get("type").and_then(Value::as_str);
            assert!(
                matches!(ty, Some("integer" | "number" | "boolean" | "string")),
                "{definition}.{name} has type {ty:?}; the vocabulary is scalars only"
            );
        });
    }

    #[test]
    fn test_schema_documents_both_directions() {
        let schema = metrics_json_schema();
        assert!(schema["$defs"]["UploadMetrics"]["properties"]["dedup_ratio"].is_object());
        assert!(schema["$defs"]["DownloadMetrics"]["properties"]["expansion_ratio"].is_object());
        // Nested `$schema` keys would make the combined document ambiguous.
        assert!(schema["$defs"]["UploadMetrics"].get("$schema").is_none());
    }

    /// Shared properties must agree across directions; `all_metric_types` asserts that, and this
    /// proves the merge actually runs over both.
    #[test]
    fn test_shared_properties_have_one_type_across_directions() {
        let merged = all_metric_types();
        assert_eq!(merged.get("duration_ms").map(String::as_str), Some("integer"));
        assert_eq!(merged.get("throughput_bps").map(String::as_str), Some("number"));

        // Direction-specific properties are present alongside the shared ones.
        assert_eq!(merged.get("dedup_ratio").map(String::as_str), Some("number"));
        assert_eq!(merged.get("expansion_ratio").map(String::as_str), Some("number"));

        // The union is strictly larger than either direction alone.
        let upload_count = schema_of::<UploadMetrics>()["properties"].as_object().unwrap().len();
        assert!(merged.len() > upload_count, "download-only properties were not merged in");
    }

    /// The numeric metrics that regression alerting depends on must be typed as numbers, not
    /// strings - a consumer typing its storage off this schema needs them aggregatable.
    #[test]
    fn test_alerting_metrics_are_numeric() {
        let types = all_metric_types();

        for name in [
            "duration_ms",
            "total_bytes",
            "transfer_bytes",
            "peak_concurrency",
            "n_files",
        ] {
            assert_eq!(types.get(name).map(String::as_str), Some("integer"), "{name} must be numeric");
        }
        for name in [
            "throughput_bps",
            "logical_throughput_bps",
            "ewma_throughput_bps",
            "dedup_ratio",
        ] {
            assert_eq!(types.get(name).map(String::as_str), Some("number"), "{name} must be numeric");
        }
        for name in ["direction", "outcome", "error_class", "transfer_id"] {
            assert_eq!(types.get(name).map(String::as_str), Some("string"), "{name} must be groupable");
        }
        assert_eq!(types.get("terminal").map(String::as_str), Some("boolean"));
    }
}
