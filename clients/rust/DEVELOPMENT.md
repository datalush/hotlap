# Native Rust client development in Hotlap

This directory contains Hotlap's imported Apache Fluss Rust client. Keep the
original Apache-2.0 source headers, LICENSE/NOTICE and attribution. The client
owns the wire protocol, metadata, codecs, scanners, routing and writer queues;
DataFusion integration lives in `../../crates/fluss-datafusion`.

## Prerequisites and protocol regeneration

Use the pinned Rust toolchain and Cargo locks. Regular builds use the checked-in
`crates/fluss/src/proto/fluss.rs`; they do not need Java, Python or protoc.
To regenerate deliberately after updating `crates/fluss/proto/FlussApi.proto`,
install protoc and run `crates/fluss/regen.sh`. The Rust `gen` crate uses that
vendored schema directly, without a fallback to an external Java checkout.
Do not hand-edit the generated Rust protocol file.

## Workspace and verification

Members: `crates/fluss`, `crates/fluss/gen`, `crates/fluss-test-cluster`,
`crates/examples`. Language bindings, Bazel/CMake tooling, the imported website
and upstream release scripts are outside the current Hotlap tree.

From the Hotlap root:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo clippy --manifest-path clients/rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo fmt --manifest-path clients/rust/Cargo.toml --all -- --check
```

Docker/lab integration tests are opt-in and must clean only their own fixtures.
Performance profiles use RELEASE/eight jobs; functional checks use DEBUG/eight
jobs. The independent client lock and root integration lock can pin different
Arrow59 minor releases; a root dependency follows the root lock.
