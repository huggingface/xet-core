use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out = crate_dir.join("include").join("hf_xet.h");
    std::fs::create_dir_all(out.parent().unwrap()).unwrap();

    // Do not fail the build if generation hiccups on partial code; the
    // dedicated up-to-date test (Task 11) is the source of truth.
    if let Ok(bindings) = cbindgen::generate(&crate_dir) {
        bindings.write_to_file(&out);
    }
    // Expose the target triple to tests: `cc` needs TARGET/HOST but those are
    // only set in the build-script environment, not at test runtime.
    if let Ok(target) = std::env::var("TARGET") {
        println!("cargo:rustc-env=CAPI_TARGET={target}");
    }

    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
