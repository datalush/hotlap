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

## Selective recoverable log-write backport

The prepared recoverable log-batch API and ProduceLog v2 `recovery_mode` wire
field are selectively backported from Fluss develop at
`5f87385b4426e79dfed2cc4484dc30df5a458e38`. This is not a full SDK sync: the
Hotlap SDK base and protocol schema remain authoritative for all unrelated APIs.
In particular, Hotlap-owned accumulator admission/flush behavior in
`crates/fluss/src/client/write/accumulator.rs`, sender retry/resource-lifetime
changes in `sender.rs`, and native writer lifecycle/memory-accounting paths in
`writer_client.rs` are retained rather than replaced; feature additions there
are additive. The embedded SDK's resolved Arrow version remains 59.3.x.

`PreparedLogBatch` lets callers persist and replay an immutable batch envelope;
it does not wire that state into Hotlap runtime checkpoints, provide 2PC, or
guarantee exactly-once behavior across arbitrary sinks. Callers must follow the
API's NewSend/Retry contract and persist the prepared bytes and acknowledgment
state themselves.

The opt-in server tests exercise restart-style restore/retry, unknown-writer
failure, and a real dropped ProduceLog ACK followed by retry. Run them against a
current-source Fluss image that supports ProduceLog v2 (not the default 0.9.x
compatibility image):

```sh
FLUSS_IMAGE=<image> FLUSS_VERSION=<tag> CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=8 \
  cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs \
  --test prepared_log_batch --features recoverable_integration_tests -- --test-threads=1
```

## Native examples

From this directory, inspect available client examples with:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo run -p fluss-examples --example example-table -- --help
```

A Fluss server is an external runtime dependency; Docker tests supply their own
server image. Normal Rust builds use the checked-in generated protocol and do
not compile Java. [Protocol regeneration](DEVELOPMENT.md#prerequisites-and-protocol-regeneration)
uses the schema vendored alongside the client.
