# Production-readiness status

The native DataFusion providers scan Fluss logs and current KV state, and
implement SQL `INSERT INTO` through the existing Rust Fluss writers. Their
consistency guarantees are described in
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
- Batch logs capture earliest retained and latest offsets. If retention passes a
  captured start before subscription, the read fails. A unit test injects an
  out-of-range response *after* another bucket produced data and checks that
  the Arrow batch reader fails instead of swallowing the error.
- The streaming source declares itself unbounded and waits through idle polls;
  an isolated log created in native-sni delivered rows appended after the
  query started, without EOF between writes. Source-side delivery events
  carried the correct next offsets and execution ID. Pausing its consumer left
  the shared 8 MiB DataFusion pool reservation stable, and dropping the stream
  released it. An explicit batch scan resumed from offset 1 without replaying
  row 0, while an invalid offset failed; a fresh partition added during an
  open partitioned streaming scan caused an explicit topology error instead
  of disappearing silently. The Python 55 FFI stream independently received
  two late appends on another isolated log through an SQL filter, observed
  progress and cancelled. The filtered stream used DataFusion session
  `batch_size=1`; its default `FilterExec` coalescing can delay small
  nonterminating results until more rows arrive (see reading semantics).
  This is not an engine checkpoint, continuous KV changelog, or a sustained
  streaming resource profile.
- Evolved log schemas read historical rows without pushing newly added fields
  into the server projection. A fresh DataFusion provider returns those
  fields as null for older rows; an old plan keeps its original schema or
  fails. After dropping and recreating a table with the same name, old log
  and KV plans reject the new table identity. Concurrent executions of one
  physical log source with distinct TaskContexts return complete results.
- An ignored failover test in the isolated native-sni lab restarts only the
  active coordinator while a bounded log stream is open. The original stream
  completes with every captured bucket row or fails; once cluster health is
  Green, a fresh scan returns all 512 rows. A separate test rejects an
  untrusted TLS CA without displaying the SASL password.
- An execution-time budget (default 16,384 selected partition/bucket pairs)
  rejects larger scans instead of silently omitting buckets. Use
  `with_max_assigned_buckets` to choose a deliberate larger budget.
- SQL `INSERT INTO` appended log rows, performed an `INSERT ... SELECT`
  between Fluss tables and upserted KV keys; counts reflected acknowledged
  inputs, while subsequent SQL scans checked actual values. Two concurrent
  inserts used separate writers. A physical INSERT plan refused to write to
  a table dropped and recreated under the same path. Mixed-partition Arrow
  batches routed across old two-bucket and new three-bucket layouts for both
  log and KV. A 4 MiB SQL input completed with only a 2 MiB Fluss writer
  buffer (routing rows on Tokio's blocking pool instead of stalling its
  sender). One `INSERT ... SELECT` from an unbounded Fluss log delivered
  and acknowledged each batch before source EOF; cancellation stopped the
  query and later source writes did not reach the destination. Python 55 FFI
  separately checked finite log/KV INSERTs and a continuous INSERT visible
  to another query before cancellation. Rust-release wheels for the write
  change (`fluss-datafusion-native` and `fluss-connectors` 0.1.1.dev2) were
  installed with the pinned DataFusion Python 55 release wheel in a fresh
  Python 3.12 virtualenv: all six Python tests and `uv pip check` passed.
  The same wheels in `lab/python-lab` passed its two targeted DataFusion
  integration tests. Partial writes cannot be rolled back or reported as
  a fully successful operation.
- Subsequent Rust working-tree hardening charges retained sink batches to the
  query pool and shares a deadline between enqueue/ACK. The 4 MiB input still
  completed with a 2 MiB writer buffer, released pool reservations, and a
  one-byte DataFusion pool rejected an INSERT before its row was written.
  Unit tests reject fire-and-forget ACK policies and nulls in required columns.
  SQL DELETE KV passed exact filtering, no matches, all rows and partitioned
  deletion after rescale; configured/implicit ignore policies are rejected.
  These additions are not in the already-installed dev2 wheel. Cancelling
  under a blocked ACK or saturated buffer still requires targeted verification;
  Python DELETE/MERGE FFI and sustained acceptance are pending.
- Rust working-tree MERGE passed a finite VALUES source combining UPDATE,
  DELETE and INSERT, a false-predicate/no-op clause, first-clause precedence,
  NOT MATCHED BY SOURCE deletion, and explicit rejection of duplicate
  modifying keys with the tested current batch left unapplied. It composes
  DataFusion join/filter/CASE operators; no new SQL evaluator was added.
  Primary-key changes, incomplete INSERT column lists and unbounded MERGE
  sources are explicitly unsupported. Full MERGE validation, concurrency,
  failure injection, and matching Python FFI/release artifacts remain open.

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
continuous log scan paused on its first remote row also fails explicitly
after that retention advance, instead of silently skipping lost records.
This was verified in the RustFS profile with the published `.6` image and
the prefix-scoped read-only STS policy. A one-segment pending-request budget
and a one-byte remote prefetch budget both fail a large remote scan rather
than silently truncating it. Remote request slots are released on
cancellation, and temporary S3
credentials received as either `security_token` or `session_token` are passed
to OpenDAL as its S3 `session_token` property. If both names are present with
different values, the client rejects them without logging either token.
The credential manager publishes Fluss's token expiration with each update.
If refresh fails and the previous token expires, new remote downloads wait
up to `scanner_remote_log_operation_timeout_ms` for a valid replacement;
the manager also bounds its own token-fetch RPC with this setting, and
shutdown interrupts a blocked fetch. Credentials' `Debug` output redacts
access keys and session tokens. Readers never send the stale token to
OpenDAL. The scanner's overall DataFusion
timeout still bounds the full read, including credential waits and retries.
Unit tests inject temporary OpenDAL failures, confirm recovery within the
configured retry budget and a subsequent successful read, then separately
exercise the exhausted budget and a blocked credential refresh.
An ignored fault profile routes S3 HTTP through a short-lived test proxy to
the **existing** RustFS. It injects two 503 responses followed by recovery,
persistent 503 until the retry budget is exhausted, and a held response for
timeout/cancellation; a new DataFusion query succeeds afterward. STS calls
still go directly to RustFS, and only STS-signed reads under the test prefix
receive injected failures. The same profile also made an **unbounded** log
scan fail explicitly after exhausting three real HTTP 503 attempts, without
returning a partial batch. This test ran in about 36 seconds with 32 rows
against the published `.6` image.

The separate ignored real-expiry profile pauses an active log scan after its
first STS-signed remote row. Fluss's default AssumeRole request lasts one hour;
for this profile a test-only STS endpoint requests genuine **900-second**
sessions from the same RustFS, preserving the inline read-only policy. The
initial server-issued token listed the test prefix before expiry and was
rejected by live RustFS afterward (`InvalidRequest`); Fluss fetched a second
session, and the *same paused scan* returned all 32 rows. The S3 proxy observed
different session-token fingerprints before and after expiry. This completed
against the published `.6` image in approximately 938 seconds.

The RustFS lab also has a bucket-scoped `fluss-read` IAM **user** and policy:
its direct and assumed credentials read but cannot write (403). This user is
not the Fluss server's uploader. RustFS accepts `RoleArn` for compatibility,
but sessions signed with server root keys still inherit root permissions
unless the `AssumeRole` call also carries an inline `Policy`. The published
`ghcr.io/midnattsol/fluss:1.0.0-midnattsol.6` image contains the optional
`s3.assumed.role.policy` fix (revision `ff40eadf0`). With a policy restricted
to the isolated test prefix, it uploaded log segments and issued tokens that
read a real RustFS object but were denied PutObject and DeleteObject; the
DataFusion S3/TTL scan passed. The same published `.6` image also passed the
S3/TTL scan with no policy, preserving the default behavior. The `.5` image
still issues unrestricted root-derived sessions; `.6` requires explicit
policy configuration before claiming least privilege in production.
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
rather than bypassing the limit: the scanner initially reserves its advertised
size, then reserves any extra bytes **before writing each downloaded chunk**.
Failure or cancellation removes partial files and releases the reservation.
This limits the scanner's remote temporary files, not other users of the same
disk or temporary memory used by OpenDAL's read chunks. A single oversized
server record can exceed a fetch size hint before the pool rejects its decoded
Arrow batch.
`scanner_remote_log_read_chunk_bytes` (default 8 MiB, maximum 64 MiB) sets
the per-reader chunk size; combine it with `scanner_remote_log_read_concurrency`
and `remote_file_download_thread_num` (defaults 4 and 3) when budgeting memory
outside the DataFusion pool. At the defaults their product is 96 MiB of
potential in-flight chunk data **per scanner**. This is a sizing input, not
a strict process-memory bound: decompression, fetch responses and OpenDAL's
internal allocations are additional. Set
`scanner_remote_log_operation_timeout_ms` (default 30000ms) for individual
remote reader/open/read operations and credential fetch/wait; this is not a
query-wide timeout.
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
CARGO_BUILD_JOBS=2 KUBECONFIG=/tmp/opencode/native-sni.kubeconfig uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test live_log_sql -- --ignored --test-threads=1
KUBECONFIG=/tmp/opencode/native-sni.kubeconfig CARGO_BUILD_JOBS=2 uv run --no-sync --env-file ../lab/.env cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::table::kv_scanner::tests::leader_restart_invalidates_snapshot_without_restarting_reader -- --ignored
```

The first command restarts **only** the active coordinator pod for its failover
test; the second restarts **only** a tabletserver pod. Both require the isolated
`k3d-native-sni` context and wait for recovery.

Run the independent Docker storage profile separately from the native-sni
tests (both may bind port 9123):

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 cargo test -p fluss-datafusion --test remote_retention datafusion_reads_remote_and_rejects_lost_retention -- --ignored
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_reads_and_expires_rustfs_s3 -- --ignored
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_STS_READONLY_POLICY=1 CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_reads_and_expires_rustfs_s3 -- --ignored
```

The third command enables a policy limited to the test's unique prefix and
verifies **the token returned by Fluss**, including a successful GetObject and
denied PutObject/DeleteObject. The published `.6` also passed this test without
`FLUSS_STS_READONLY_POLICY`, exercising the backward-compatible default.

Run the short real-HTTP fault profile separately, using a host IP reachable
from Docker for `FLUSS_FAULT_PROXY_HOST` (the reference host used
`192.168.68.55`):

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_FAULT_PROXY_HOST=<host-IP> CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_handles_real_rustfs_http_failures -- --ignored
```

Run the separate, approximately 16-minute real STS-expiry profile only when
that long verification is needed. Add `FLUSS_STS_PREFLIGHT=1` for a short setup
check that stops before expiry:

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_FAULT_PROXY_HOST=<host-IP> CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_renews_real_rustfs_sts_after_expiry -- --ignored --nocapture
```

## Reference resource-pressure profile

The ignored `datafusion_resource_pressure_rustfs` test creates its **own**
Docker Fluss cluster and a unique, removable prefix in the existing RustFS
bucket. It runs against the Rust DataFusion provider, not the Python wheel.
Its default profile writes 4,800 log rows of 128 KiB (600 MiB decoded, larger
than the 512 MiB pool) and 64 KV rows (8 MiB). Four queries run concurrently
(two logs, two KV), with two physical partitions per query. It uses a shared
512 MiB DataFusion pool, 1 MiB remote chunks, two read operations and two
downloads per scanner, and two prefetched segments/64 MiB per scanner. After
five minutes of warmup it measures for 30 minutes; a scan wave completes
before another starts. Every scan checks row IDs, values and duplicates;
each wave exercises early cancellation and checks that Arrow reservations and
remote temporary files are released. The test also forces an explicit memory
pool rejection using a separate tiny pool.

On the reference machine with the test process pinned to four permitted CPUs
(`taskset -c 0-3`), two full runs passed. The latest run measured 112 scans in
1,854 seconds, 832,049,024 remote bytes fetched, sampled RSS peak 170 MiB,
kernel-reported RSS high-water mark about 169 MiB, sampled pool peak 19 MiB,
and sampled remote temporary-file peak 636,819 bytes. RSS after warmup was
120 MiB and after measurement 122 MiB. The test rejects RSS above 1.5 GiB,
remote temporary-file bytes above 2 GiB, nonzero reservations or temporary
bytes after each wave, or an RSS increase above 256 MiB after warmup. The 2 GiB memory
budget was **checked by RSS/high-water mark**, not imposed as a cgroup limit;
the temporary-file limit covers the client scanner, not all process disk use.
The two RSS measurements are separate kernel observations and are rounded to
MiB; do not treat a one-MiB difference as an exact ordering of peaks.

Run the long profile alone, with four CPU IDs allowed by the host affinity:

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.5 CARGO_BUILD_JOBS=1 uv run --no-sync --env-file ../lab/.env taskset -c 0-3 cargo test -p fluss-datafusion --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --nocapture
```

For a functional smoke run before committing to 35 minutes, set
`FLUSS_PRESSURE_ROWS=64 FLUSS_PRESSURE_WARMUP_SECS=2 FLUSS_PRESSURE_MEASURE_SECS=5`.
Its output is marked `full=false` and **does not** meet the resource-profile
acceptance criterion.

## Remaining verification before a general production claim

The measured profile is evidence for this **Rust, Docker Fluss, RustFS**
configuration, not a strict bound on every transient allocation or another
deployment. Compressed fetch buffers and OpenDAL remain outside the DataFusion
pool; an Arrow batch is decoded before the pool can reserve it. The native-sni
coordinator failover test covers a live two-coordinator lab, not the Docker
profile's single coordinator. The short HTTP profile verifies real transport
503/timeout/cancellation. The separate 900-second STS profile verifies actual
expiry and successful renewal on one paused scan; failed credential renewal
and the deadline after expiry are covered by deterministic client tests.
A `.6` deployment must explicitly configure
`s3.assumed.role.policy` to restrict root-signed STS sessions. A local Python
3.12 installation of DataFusion Python 55.0.0 (upstream revision
`5ef2856f5b02cddcd3d7d3559669d95ef181ccf4`, Rust engine 55.1), the
separate Fluss FFI wheel, and this package executed real `COUNT(*)`, filtered
and limited SQL queries on populated native-sni log and KV tables (9 and 3
rows at the time of the check), plus eight limited queries across four Python
workers. It used a 256 MiB DataFusion pool and two target partitions.
Python 55 is not published to PyPI yet; installation from that pinned source
and both built wheels into a fresh Python 3.12 environment passed all six
Python tests and `uv pip check`. Installation is documented in the README.
This live functional check is
not a Python sustained-resource profile: only the Rust/Docker/RustFS workload
has the 35-minute pressure evidence above. Other deployment profiles still
require their own acceptance evidence.
