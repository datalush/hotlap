# Arrow write routing and ownership — 3etr

Scope: Rust log INSERT optimization, finite and continuous, and a separate
evaluation of KV upsert/delete. Functional verification uses DEBUG/8 jobs.
This is materialization/correctness evidence, not the sustained throughput or
RSS acceptance owned by `pc5n`.

## Route and materialization boundaries

```text
DataFusion input → schema/null validation → input backing lease
  → blocking worker + pool-admitted routing scratch
  → client schema normalization + authoritative destination grouping
  → contiguous slice / interleaved Arrow take
  → client byte-target slices → PrebuiltRecordBatchBuilder
  → IPC/statistics/compression → sender/retries → flush/ACK
```

The log sink no longer calls row append or rebuilds Arrow columns through
`RowAppendRecordBatchBuilder`. A reusable ColumnarRow in the client reads routing
keys; it does not convert every payload field into a new Arrow column.

| Boundary | Allocation/copy behavior | Evidence |
| --- | --- | --- |
| Compatible schema normalization | Clones column references; no payload copy | Existing client normalization/type/null tests |
| Lossless encoding normalization | Arrow casts may materialize; caller retainer admits resulting buffers | Same existing normalization, now called before grouping; callback also covers casts |
| Homogeneous or contiguous destination | Slice shares backing buffers | New client pointer/null/nested test |
| Interleaved destination | One Arrow take per destination, preserving relative input order | New client semantic test; 96-row live native-row comparison covers all five old2/new3 groups |
| Oversized input group | Binary-search logical slice size and submit byte-target slices; original backings remain retained | 20 × 200,000-byte values split into four slices with identical value-buffer pointers; live 4 MiB input / 2 MiB client buffer |
| Wire batch | IPC/statistics/compression and encoded bytes still materialize | Existing builder/size/statistics/type tests; no end-to-end zero-copy claim |

Gathers are justified for interleaved rows: using separate prebuilt singleton
batches would multiply IPC framing/requests. Groups keep indices in input order;
no global order across destinations is promised. Groups are processed in first
destination appearance order. Row indices use checked Fluss i32 record-count
admission before conversion to u32, without an additional gather-index copy.

Grouping calls the existing `WriterClient::assign_bucket`, which resolves
effective per-partition counts. `send_assigned` reuses that assigner and the
same immutable Cluster snapshot for enqueue. It does not re-hash a representative
key against a newer layout. Native sticky/round-robin/hash behavior, accumulator
admission, retries and ACK/error propagation remain in the client. Unknown
post-rescale partition metadata fails instead of guessing a table default.

## Pool and client accounting

The connector reuses `resources::reserve_batch` buffer owners for log inputs and
newly materialized cast/gather buffers. Reservations survive worker/query drop
while the client still holds any corresponding buffer. Slices keep the original
whole-batch lease; gathers have independent leases. Other providers and VALUES
receive the same sink admission, not an assumption that source leases exist.

The native client exposes a conservative routing scratch hint: row indices,
possible destination metadata and selected routing-key backing bytes. The sink
admits that hint against the supplied DataFusion pool for the worker lifetime.
Before rescale, unresolved partitions can make the group allowance conservative;
after rescale, effective cached partition counts bound possible destinations.
This hint is not a separate pool or an Arrow allocator.

`append_arrow_batch_with_retainer` is a resource ownership hook around
materialized Arrow batches. Its callback must preserve schema, values, count and
order. Plain native `append_arrow_batch` uses the same route with identity
retention. There is no legacy first-partition/bucket grouping route or alternate
writer. Admission of casts/gathers occurs **after** Arrow allocation, so it
does not prevent allocation peaks. Errors can occur after earlier groups were
submitted; they are not statement rollback.

Client batch permits now include a logical Arrow batch size hint and existing
framing, rather than treating prebuilt batches as zero-byte records. Slice
estimation uses Arrow's `ArrayData::get_slice_memory_size`, falling back
conservatively to backing bytes for unsupported layouts. Nested child storage
may also make estimates conservative. Request/buffer admission remains estimated,
not a process-RSS bound; encoder scratch and actual encoded buffers retain the
`yeqf`/`pc5n` audit obligation. A singleton may exceed the batch target but must
pass buffer admission and the uncompressed request-size estimate check.

## KV decision

KV is not an Arrow-log batch format. `UpsertWriter::upsert` encodes PK/bucket keys
and required row bytes, and `delete` emits keys without value bytes. ColumnarRow
does not supply already encoded KV bytes, so row encoding is necessary. Existing
encoders/accumulator reuse their own buffers. Replacing these calls with the log
append API would violate protocol/PK/delete semantics.

Keep the single reusable typed row view for KV/MERGE actions and existing worker
reservation. Preserve per-key input order, null/type validation, full-row upsert,
delete policy and old/new layouts. Further KV batch API work requires a measured
benefit beyond required wire encoding; no duplicate KV writer or false zero-copy
claim is introduced here.

## Verification

- Client library: **831 passed, 2 ignored**, including new contiguous/gather and
  byte-slice tests; existing encoder/builder/null/nested/statistics/limiter tests.
- Core library: **23 passed**, including a new sink lease test retaining a slice
  and an independent gather after worker drop, then freeing both reservations.
- Four native-sni `write_sql` tests: INSERT log/KV, SQL source/concurrent/repeated
  execution, DELETE/MERGE regressions, 4 MiB/2 MiB pressure and tiny-pool rejection,
  mixed/rescaled partitions, continuous singleton confirmation before EOF and
  cancellation. The rescale test compares exact bucket membership, per-bucket
  order and null values against 96 native row writes, and verifies the pool
  returns to its pre-INSERT baseline after ACK. Earlier retained KV SELECT
  results keep their legitimate source leases; they must not be freed by INSERT.
- Core and client clippy with `-D warnings`; package formatting checks.

Commands (from repository root):

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p fluss-datafusion --all-targets --all-features --locked -- -D warnings
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo clippy --manifest-path clients/rust/Cargo.toml -p fluss-rs --all-targets --locked -- -D warnings
```

Sparse KV streaming, idle beyond ACK timeout, ACK-loss/saturated cancellation,
structured confirmations and explicit restart acceptance remain `yeqf`, `bqrq`
and `bsjm`; the existing streaming regression is not their complete acceptance.
