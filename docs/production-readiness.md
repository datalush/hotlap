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

The ignored `remote_retention` tests use the Fluss 1.0 server image, either a
local filesystem shared with its tabletserver or the existing RustFS S3
endpoint, 120-byte log segments and one-second tiering/retention checks. The
S3 test isolates objects under a unique prefix in `fluss-lab` and removes
that prefix afterward. Both tests verify the **actual remote-download byte
counter** while DataFusion returns exactly the projected and filtered rows.
With a stalled remote consumer it checks the four-file prefetch bound, stable
DataFusion reservation and cleanup of temporary files after cancellation.
With `table.log.ttl` changed from disabled to two seconds mid-read, a scanner
limited to one prefetched remote segment and one row per pull keeps unread
segments out of its cache. After retention advances, that in-progress scan
fails with an out-of-range or missing-segment error instead of returning an
incomplete result; a new query returns exactly the rows still retained. A
one-segment pending-request budget and a one-byte remote
prefetch budget both fail a large remote scan rather than silently truncating
it. Remote request slots are released on cancellation, and temporary S3
credentials received as either `security_token` or `session_token` are passed
to OpenDAL as its S3 `session_token` property. If both names are present with
different values, the client rejects them without logging either token.
The remote downloader reports a missing segment as an incomplete scan (the
object may have expired or been removed); it preserves the storage error as
the cause without assuming that TTL was the reason. Permanent storage errors
are not retried.

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
8192), and `scanner_remote_log_max_prefetch_bytes` (downloaded remote bytes
per scanner, default 64 MiB). An oversized remote segment fails the scan
rather than bypassing the limit. A single oversized server record can
exceed a fetch size hint before the pool rejects its decoded Arrow batch.
`scanner_remote_log_max_retries` controls retries *after* the first attempt
(default 10; `0` means one attempt). `scanner_remote_log_retry_backoff_base_ms`
and `scanner_remote_log_retry_backoff_max_ms` configure exponential backoff
with jitter (defaults 100ms and 5000ms); base must be positive and max at
least base, up to 3600000ms. These settings apply only to retriable remote
download failures.
Cancelling a scan interrupts a queued retry instead of waiting for its
backoff. The DataFusion scan timeout still bounds the complete source read.

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
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 cargo test -p fluss-datafusion --test remote_retention datafusion_reads_remote_and_rejects_lost_retention -- --ignored
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_reads_and_expires_rustfs_s3 -- --ignored
```

## Remaining verification before a general production claim

Long-duration load tests should measure compressed fetch-buffer memory (which
the DataFusion reservation does not cover), temporary disk usage, simultaneous
queries and coordinator failover. Reproduce them against the exact server,
client and storage profile intended for deployment.
