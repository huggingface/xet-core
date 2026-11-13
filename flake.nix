{
  description = "xet-core development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        fenixPkgs = fenix.packages.${system};

        # CI pins rustc/cargo/clippy to 1.95.0 (.github/workflows/ci.yml) and
        # formats with nightly rustfmt. Combine individual components instead
        # of whole toolchains so nightly's rustfmt binary wins without
        # colliding with stable's: `cargo fmt` then runs on nightly rustfmt
        # while build/test/clippy stay on 1.95.0.
        stable = fenixPkgs.toolchainOf {
          channel = "1.95.0";
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        nightly = fenixPkgs.toolchainOf {
          channel = "nightly";
          date = "2025-10-01";
          sha256 = "sha256-GCGEXGZeJySLND0KU5TdtTrqFV76TF3UdvAHSUegSsk=";
        };

        rustToolchain = fenixPkgs.combine [
          stable.rustc
          stable.cargo
          stable.rust-std
          stable.rust-src
          stable.clippy
          stable.rust-analyzer
          nightly.rustfmt
        ];

        devPython = pkgs.python3.withPackages (ps: [ ps.pytest ]);
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            rustToolchain
            pkgs.cargo-audit
            pkgs.maturin
            pkgs.uv
            # Dev headers (required by PyO3 for `cargo test` linking against
            # libpython) and pytest, from one interpreter.
            devPython
          ];

          shellHook = ''
            if [ ! -d ".venv" ]; then
              ${devPython}/bin/python3 -m venv .venv --system-site-packages
            fi
            source .venv/bin/activate

            # AGENTS.md's format and lint commands, for both the root workspace
            # and hf_xet. No `+nightly` needed: rustfmt in this toolchain
            # already is nightly's build.
            alias fmt='cargo fmt --manifest-path Cargo.toml --all && cargo fmt --manifest-path hf_xet/Cargo.toml --all'
            alias clippy='cargo clippy -r --verbose -- -D warnings && cargo clippy -r --verbose --manifest-path hf_xet/Cargo.toml -- -D warnings'
            alias build-python='(cd hf_xet; maturin develop)'
            alias test-python='python -m pytest hf_xet/tests -v'
          '';
        };
      });
}
