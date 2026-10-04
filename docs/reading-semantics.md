# Reading semantics before engine adapters

The Rust client in `clients/rust/crates/fluss` already returns Arrow
`RecordBatch` objects. Its Python binding is in
`clients/rust/bindings/python`. Neither should be reimplemented here.

`TableScan::limit(n).create_bucket_batch_scanner(bucket)` yields at most `n`
rows **per bucket**. It is useful for explicit previews, but does not prove a
complete current view of a primary-key table. A log scanner yields changes,
not the current state of a primary-key table. Do not expose either as an
unqualified `read_table()` or an unrestricted SQL table.

The **primary-key** provider uses the server's `ScanKv` RPC, not a limited
preview or a changelog reconstruction. Each bucket's server-side RocksDB
snapshot supplies every live row; upserts and deletes are reflected in that
state. Continuations reuse the same snapshot session. No external snapshot
files or snapshot/log merge are needed for this online scan. A session error
never silently starts a new snapshot; cancellation closes known sessions
best-effort, with server TTL for an interrupted initial open. A query timeout
fails rather than claiming partial results. Each bucket opens its snapshot
when it is first read; there is **no transactionally consistent cross-bucket
snapshot** during concurrent writes, nor a snapshot retained across queries.
Both partitioned and non-partitioned KV tables are supported. No row filter or global SQL limit is
pushed into `ScanKv`; DataFusion evaluates them exactly. Non-empty projections
are applied by the Arrow decoder after reading value records, and `COUNT(*)`
still decodes full rows. Schema changes and non-partitioned topology changes
require replanning; partitioned layouts are rediscovered on each execution.

The batch append-only log source relies on the client's bounded offset reader;
the continuous source subscribes to the client's unbounded batch scanner. Both
share the same projection/pruning path. Predicate pushdown only prunes
batches, so filters must still be evaluated exactly
in the engine. Only representable `Int32`/`Int64` comparisons are translated
for tables still on their initial schema. Conjuncts may be pushed independently
because DataFusion retains the whole
expression. `OR` and lossy numeric conversions are not pushed. A log table
needs `table.statistics.columns` set before writing for those batches to
carry useful pruning statistics. Keep projection, limits, and partitions
honest about these semantics.

The log provider supports append-only logs, including partitioned tables. Both
providers set their physical stream count using the table's bucket count at
planning time, DataFusion's target parallelism and an optional positive
connector cap. At execution time, every selected partition's actual buckets
are assigned across those streams, even when its count differs from the
table default. Log streams share one offset capture per execution.
By default each bucket starts at its earliest **retained** offset captured
alongside its latest offset; an explicit start can use captured latest offsets
or a complete mapping of table/partition/bucket IDs to inclusive offsets.
Incomplete/stale mappings and out-of-range starts fail explicitly. Reusing
the physical source plan with a new TaskContext
captures new offsets. Before subscribing, the source checks that retention has
not advanced past that start. If it has, the query fails rather than silently
starting from newer data. A server out-of-range response during an ongoing
scan also fails the query. The bounded read
finishes or fails with an explicit error on timeout. Offsets are collected
per bucket, not as a transactional cross-bucket snapshot. Non-empty SQL
projections are requested from the Fluss scanner on initial-schema tables.
After a schema change, the source reads full rows and projects Arrow columns
locally: older log batches may not contain fields added later, and the server
can reject a projection or predicate referring to those fields. A zero-column
`COUNT(*)` also fetches full rows before stripping columns locally. Exact
filtering and global SQL limits remain DataFusion operations.
When every trailing batch is pruned, the scanner's consumed offset advances
even though it yields no Arrow batches. The bounded reader checks that
progress and completes once each captured stopping offset is reached;
DataFusion polls in short intervals while preserving the overall timeout.
Partitions that start after other partitions finish retain the shared offsets.
Cancelled offset initialization can be retried by a different partition before
any rows are delivered. Missing/invalid offsets or a late partition failure
produce an error, not a complete result. Concurrent queries require distinct
`TaskContext` instances; using one context for overlapping executions of the
same physical plan cannot distinguish the two queries.
`EXPLAIN ANALYZE` reports Arrow decoded/output bytes and peak decoded batch
size, not network bytes or process memory. Time metrics measure offset capture
and waiting for read batches; they are not an end-to-end latency budget.
`fluss_active_partition_streams` tracks source stream lifetimes, including
failed and cancelled reads, and is zero after each source query completes.
The peak-batch gauge does not include buffers held by DataFusion operators
upstream of the scan.
For partitioned tables, the execution discovers partition names/IDs once and
shares that list between physical streams. Log queries also capture the
selected partitions' bucket stopping offsets once per execution; KV sessions
still open lazily per bucket. New partitions created later belong to the next
execution. If a partition is dropped in flight, an already-open session may
complete, or its pending scan can fail; never assume atomic DDL/read isolation.
For `Utf8` partition keys, simple equality to a string literal (including
conjuncts under `AND`) can prune partitions; `OR`, casts, and other conditions
are evaluated exactly by DataFusion without partition pruning. Each selected
partition supplies its own bucket count to range validation, routing, and
the physical-stream assignment. Changing the table default does not rewrite
old partitions; the existing physical plan can rediscover the new layout on
its next execution. Missing or invalid partition bucket counts fail the scan
instead of silently omitting buckets. Each **batch** query has a finite timeout.

`FlussLogTable::open` remains batch for existing callers, while
`open_with_options(LogReadOptions::default())` uses streaming. A streaming scan
has no query-completion deadline: when idle, it waits for records and does not
report EOF. Scanner operation/network deadlines remain configurable. DataFusion
sees `Boundedness::Unbounded`, incremental emission and no global order; final
global sorts/aggregates cannot be assumed to finish. During polling the source
checks table ID/schema and partition IDs/counts periodically; changes fail
explicitly rather than incorporating unknown buckets without start positions.
New partitions require a new execution. `subscribe_deliveries()` observes
source-side batches with table/partition/bucket, first and exclusive next
offset, and execution ID. Server-side pruning may leave gaps and downstream
SQL may filter entire batches. These are neither prefetched offsets nor
processed/committed sink checkpoints. Treat a lagged observer as an error.

`EXPLAIN ANALYZE` identifies log versus KV scans, table and projected
columns, optional log batch predicate and partition pruning. Partition
counts describe discovered and selected partitions (recorded once, not once
per physical stream, regardless of which stream starts first).
`fluss_buckets_assigned` counts actual selected partition/bucket pairs assigned
to executed streams, including empty ranges. KV counts server-confirmed
sessions and successful `ScanKv` RPC responses (including empty pages), not
Arrow output batches. First-page latency includes request and decoding.
Projection is identified as `server` for initial-schema logs,
`client_evolved_schema` for evolved logs, `decoder` for KV, or
`full_rows_for_count` for a zero-column scan. These are not network-byte or
server-only snapshot-opening measurements.

Each source holds its most recently decoded Arrow batch against the shared
DataFusion `MemoryPool` until the next pull or stream drop. When the configured
pool cannot reserve that batch, the query fails rather than returning a partial
result. The batch is decoded before its size is known and reserved; the pool
does not prevent that transient allocation. An evolved log scan charges the
full decoded batch before locally projecting the requested columns. This
reservation does **not** account for Fluss fetch buffers, remote
prefetch or batches retained by downstream operators: size the client fetch
settings and DataFusion target parallelism accordingly. With the default
unbounded DataFusion pool, it is accounting rather than a memory limit.

The catalog discovers names once; reload it after creating tables.
It selects the log or KV provider from the table's primary-key metadata.

`INSERT INTO` is a sink operation on either provider. The target schema and
table ID/schema ID are checked before writing and on each input batch.
DataFusion passes one asynchronous stream of Arrow batches to a writer
isolated for that statement. The existing Fluss client does the per-row
routing; this is necessary when a batch mixes partitions with different
historical bucket counts. On append-only logs, INSERT appends. On KV tables,
INSERT performs full-row upsert; duplicate keys in one input count as two
submitted rows, and unordered parallel inputs have no guaranteed winner.
The sink waits for ACK after each batch, even when the input never ends.
The final `count` is available only after the source reaches EOF. Cancellation
or a later failure leaves already committed rows intact; no cross-batch
rollback, source/sink checkpoint, or exactly-once execution is promised.
Writer buffer budgets and ACK policy belong to Fluss Config; separate sink
options cap ACK waiting and retry attempts.

The sink reserves its retained Arrow batch in the DataFusion pool before
enqueueing it. That reservation travels with the blocking worker and is
released when the worker drops its batch, even after async cancellation.
Reservations from input operators may overlap conservatively with it; the
Fluss writer's encoded buffer is a separate per-writer budget. A single
deadline covers enqueueing and flushing one batch. Cancellation marks the
row loop stopped and aborts the dedicated writer synchronously, waking any
producer waiting for buffer space; requests already sent may still commit.
Counts require `writer_acks=all`, `-1`, or `1`; fire-and-forget ACK mode is
rejected. Required destination columns are checked for nulls before enqueue.

Rust `DELETE FROM kv WHERE ...` composes the existing finite KV scan with
DataFusion's exact predicate evaluation and submits selected keys to the
Fluss delete writer. No filter selects all snapshot rows; no matches returns
zero. The count is acknowledged delete operations on selected snapshot rows,
not proof of how many rows existed at the instant the deletes reached the
server. Concurrent changes can be overwritten by a delete selected earlier:
there is no conditional write or statement-wide isolation. Table policy must
allow deletes; `ignore`/`disable`, including implicit `ignore` for a configured
merge engine, is rejected before sending. DataFusion FFI 55.1 does **not**
carry `delete_from`, so this SQL DELETE path is not available through the
current Python provider; coordinated FFI support and MERGE remain pending.

The DuckDB/Polars/pandas Python adapters take **already bounded** PyArrow
results from the existing binding; they are not live Fluss table providers.
The separate `fluss_datafusion_native` FFI wheel exposes Rust log and KV
providers to DataFusion Python 55. Its new log API defaults to streaming:
request `mode="batch"` before a finite `COUNT(*)` or `.collect()`. Consume
continuous sources with `execute_stream()` / `execute_stream_partitioned()`;
`LIMIT` can end a compatible streaming plan. DataFusion's `FilterExec`
coalesces small filtered batches up to the configured session `batch_size`:
with the default a sparse, nonterminating stream may wait for thousands of
matching rows before yielding a result. Choose a smaller DataFusion batch
size (e.g. 1 for single-event latency) explicitly if needed, trading batching
efficiency for latency; the source must not add a second SQL filter.
