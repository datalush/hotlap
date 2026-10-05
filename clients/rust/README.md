<!-- SPDX-License-Identifier: Apache-2.0 -->
# Fluss Rust client in Hotlap

This is Hotlap's source copy of Apache Fluss's `fluss-rs` client, imported from
`dc427e1290847b4a569b6745fcb87b256292bf6a`, with the native fixes tracked in this
repository. It is a client SDK, not the Fluss server. Its package name stays
`fluss-rs`; upstream copyright, source headers, LICENSE and NOTICE are retained.

The client owns protocol/authentication, admin/metadata, routing by partition and
bucket, Arrow/KV encoding, scanners, writer buffers/queues, ACKs/retries and metrics.
Hotlap's DataFusion adapter reuses these mechanisms instead of duplicating them.

```text
crates/fluss/                 Native Rust SDK and vendored protocol schema
crates/fluss/gen/             Explicit protobuf regeneration utility
crates/fluss-test-cluster/    Owned Docker integration fixtures
crates/examples/              Runnable native client examples
```

Java/reference sources, non-Rust bindings and their website/release tooling are
removed from the current tree. Their imported provenance is preserved in Git.
No CMake, Bazel, maturin or language-binding build is needed for the native client.

See [development](DEVELOPMENT.md), the [Hotlap README](../../README.md) and
[native contracts](../../docs/rust-contract.md) for build/run/API boundaries.
The integration uses the client by `path`; local changes are not automatically
published to crates.io or transferred upstream.

## Native examples

From this directory, inspect available client examples with:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo run -p fluss-examples --example example-table -- --help
```

A Fluss server is an external runtime dependency; Docker tests supply their own
server image. Normal Rust builds use the checked-in generated protocol and do
not compile Java. [Protocol regeneration](DEVELOPMENT.md#prerequisites-and-protocol-regeneration)
uses the schema vendored alongside the client.
