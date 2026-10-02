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

## Runtime limits to configure

The default DataFusion memory pool is unbounded. Supply a bounded
`RuntimeEnv` memory pool, set `target_partitions` (and optionally
`with_max_partitions`), and choose a positive scan timeout. A reservation
covers **one decoded source batch per active stream**; it does not include
Fluss's compressed fetch buffer, remote prefetch, or memory held by downstream
operators. Set the Rust client's `scanner_log_fetch_max_bytes`,
`scanner_log_fetch_max_bytes_for_bucket` and remote-log prefetch settings for
the deployment's budget. A single oversized server record can exceed a fetch
size hint before the pool rejects its decoded Arrow batch.

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

## Remaining verification before a general production claim

The laboratory tests do not yet force a log segment to move from local to
remote storage while a query crosses the boundary, nor do they exercise the
server's TTL eviction of a log segment during a query. The bounded reader's
offset and out-of-range safeguards are tested, but those storage transitions
need an isolated server profile with short segments and retention intervals.
Long-duration load tests should measure fetch-buffer/prefetch memory (which
the DataFusion reservation does not cover), simultaneous queries and
coordinator failover. Reproduce them against the exact server, client and
storage profile intended for deployment.
