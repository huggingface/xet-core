//! Manifest invariants for the pyo3 dependency (see issue #891).
//!
//! `hf_xet` ships as a cdylib extension module, which is always loaded by an
//! already-running interpreter and therefore never needs to start one. Enabling pyo3's
//! `auto-initialize` feature on the regular dependency makes pyo3's build script
//! hard-fail against a Python built with `--disable-shared` -- the standard
//! configuration for manylinux-compliant build environments.
//!
//! The feature is still needed by the unit tests, which call `Python::attach` from a
//! plain Rust test binary where no interpreter is running yet. Declaring it under
//! `[dev-dependencies]` satisfies both: Cargo's v2/v3 feature resolver only unifies
//! dev-dependency features when test targets are being built, so the cdylib build
//! never sees it.
//!
//! These are asserted here because release CI builds wheels against interpreters that
//! do expose a shared libpython, so a regression would not surface in CI -- only later,
//! for downstream users building against a `--disable-shared` Python.

const MANIFEST: &str = include_str!("../Cargo.toml");

/// Collects the body of a top-level TOML table, with `#` comments stripped.
///
/// Deliberately minimal rather than pulling in a TOML parser as a dev-dependency.
/// Comment stripping matters here: the manifest documents this very invariant in
/// comments that mention `auto-initialize` by name.
fn table_body(manifest: &str, header: &str) -> String {
    let mut body = String::new();
    let mut inside = false;

    for raw_line in manifest.lines() {
        let line = match raw_line.find('#') {
            Some(idx) => &raw_line[..idx],
            None => raw_line,
        };
        let trimmed = line.trim();

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            inside = trimmed == header;
            continue;
        }
        if inside {
            body.push_str(line);
            body.push('\n');
        }
    }

    body
}

#[test]
fn pyo3_auto_initialize_is_not_a_regular_dependency() {
    let dependencies = table_body(MANIFEST, "[dependencies]");

    // Guard against the table_body helper silently matching nothing if the manifest
    // is restructured, which would make the assertion below vacuously pass.
    assert!(
        dependencies.contains("pyo3"),
        "no pyo3 entry found in [dependencies] -- the manifest layout changed and this \
         test needs updating"
    );

    assert!(
        !dependencies.contains("auto-initialize"),
        "pyo3's `auto-initialize` feature must not be enabled in [dependencies]: it makes \
         the pyo3 build script fail against a Python built with `--disable-shared`, which \
         is what manylinux build environments use. It belongs in [dev-dependencies]. \
         See issue #891."
    );
}

#[test]
fn pyo3_auto_initialize_is_enabled_for_tests() {
    let dev_dependencies = table_body(MANIFEST, "[dev-dependencies]");

    assert!(
        dev_dependencies.contains("auto-initialize"),
        "the unit tests call `Python::attach` with no interpreter running, which requires \
         pyo3's `auto-initialize`; it must stay in [dev-dependencies]. See issue #891."
    );
}
