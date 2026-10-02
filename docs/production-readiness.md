# Production-readiness status

The native DataFusion providers are read-only SQL sources for Fluss logs and
current KV state. Their consistency guarantees are described in
[reading-semantics.md](reading-semantics.md). A KV scan has one snapshot **per
bucket**, not an atomic cross-bucket snapshot.

## Verified in the isolated native-sni laboratory

- Mixed-layout partitions: existing two-bucket partitions remain readable
  after changing the table default to three buckets. The live test writes to
  bucket 2 of the new partition and reads it through both SQL providers.
- A pre-existing Rust writer writes to both old and new partitions, and point
  lookups find the written KV keys using each partition's routing count.
- Awaiting a write reports its ACK. `flush()` reports a failed ACK even if it
  arrived before the flush began; cancelling a flush releases flush mode.
- KV pagination, updates, deletes, schema evolution and cancellation. Losing
  the tabletserver leader invalidates an open KV snapshot; a *new* scanner
  succeeds after recovery, rather than continuing on a different snapshot.
- DataFusion's memory pool accounts for the most recently decoded source
  batch. With a restrictive pool, scans fail instead of returning partial
  results. A stalled consumer does not pull another KV page; cancelling it
  releases the reservation.
- Logs capture earliest retained and latest offsets. If retention passes a
  captured start before subscription, the read fails. A unit test injects an
  out-of-range response *after* another bucket produced data and checks that
  the Arrow batch reader fails instead of swallowing the error.
- An execution-time budget (default 16,384 selected partition/bucket pairs)
  rejects larger scans instead of silently omitting buckets. Use
  `with_max_assigned_buckets` to choose a deliberate larger budget.

## Verified in a separate Docker server profile

The ignored `remote_retention` test uses the Fluss 1.0 server image, a local
filesystem shared with its tabletserver, 120-byte log segments and one-second
tiering/retention checks. It verifies the **actual remote-download byte
counter** while DataFusion returns exactly the projected and filtered rows.
With a stalled remote consumer it checks the four-file prefetch bound, stable
DataFusion reservation and cleanup of temporary files after cancellation.
With `table.log.ttl` changed from disabled to two seconds mid-read, the
original execution fails instead of returning an incomplete result; a new
query returns exactly the rows still retained. A one-segment pending-request
budget also fails a large remote scan rather than silently truncating it.

## Runtime limits to configure

The default DataFusion memory pool is unbounded. Supply a bounded
`RuntimeEnv` memory pool, set `target_partitions` (and optionally
`with_max_partitions`), and choose a positive scan timeout. A reservation
covers **one decoded source batch per active stream**; it does not include
Fluss's compressed fetch buffer, remote prefetch or memory held by downstream
operators. Set the Rust client's `scanner_log_fetch_max_bytes`,
`scanner_log_fetch_max_bytes_for_bucket`,
`scanner_remote_log_prefetch_num` (downloaded file slots) and
`scanner_remote_log_max_pending_segments` (outstanding request cap, default
8192) for the deployment's budget. A single oversized server record can
exceed a fetch size hint before the pool rejects its decoded Arrow batch.

The source uses the Rust client copied in this repository. The Rust integration
pins DataFusion 55.1 and Arrow 59 in `Cargo.lock`. The currently published
DataFusion Python wheel uses major version 54; its TableProvider FFI cannot
register this major-55 Rust provider. Do not substitute a bounded preview for
a complete SQL table scan.

## Verification commands

```bash
cargo fmt --all --check
cargo fmt --manifest-path clients/rust/Cargo.toml --all --check
cargo clippy -p fluss-datafusion --all-targets -- -D warnings
cargo clippy --manifest-path clients/rust/Cargo.toml -p fluss-rs --all-targets -- -D warnings
cargo test -p fluss-datafusion
cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::table::
cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::write::
```

With the isolated native-sni lab running and ignored credentials in
`../lab/.env`:

```bash
CARGO_BUILD_JOBS=2 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test live_log_sql -- --ignored
KUBECONFIG=/tmp/opencode/native-sni.kubeconfig CARGO_BUILD_JOBS=2 uv run --no-sync --env-file ../lab/.env cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::table::kv_scanner::tests::leader_restart_invalidates_snapshot_without_restarting_reader -- --ignored
```

The second command restarts **only** a tabletserver pod in `k3d-native-sni`.

Run the independent Docker storage profile separately from the native-sni
tests (both may bind port 9123):

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 cargo test -p fluss-datafusion --test remote_retention -- --ignored
```

## Remaining verification before a general production claim

The Docker test uses a local filesystem for remote segments; it does not yet
verify the same transition against the intended S3 backend. Long-duration
load tests should measure compressed fetch-buffer memory (which the DataFusion
reservation does not cover), temporary disk usage, simultaneous queries and
coordinator failover. Reproduce them against the exact server, client and
storage profile intended for deployment.
