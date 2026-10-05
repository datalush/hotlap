# Reading semantics before engine adapters

This document describes the current implementation. The canonical Rust-first
target contract and explicitly pending requirements are in
[rust-contract.md](rust-contract.md); its decisions are not all implemented yet.

The Rust client in `clients/rust/crates/fluss` already returns Arrow
`RecordBatch` objects. Native Rust client/codec behavior is reused rather than
reimplemented. FFI/Python integration is outside the active project scope.

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
are materialized by a selective compacted decoder and Arrow builder after reading
value records. `COUNT(*)` requests zero columns and preserves row counts without
building Arrow value columns; raw pages and schema/record framing still arrive.
Unrequested logical values are not converted/validated. Schema changes and non-partitioned topology changes
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

`subscribe_progress()` adds initial per-partition assignment, including empty
buckets, under the shared execution ID; streaming ranges have no stop. Offered
events retain the delivery contract. Excluded ranges advance only up to the first
batch still queued for that bucket (and no farther than a finite stop), so fetch
completion cannot skip unoffered rows. Completed bounded ranges remain observable
after the client unsubscribes; idle all-pruned streams can advance without output.
Each executing partition reports Completed, Failed or Cancelled; a missing
initialization/terminal or a broadcast Lagged error makes evidence incomplete.
For finite complete assignment, collect every partition initialization. LIMIT can
stop before all partitions run: never fill unknown positions by guessing.
Resume uses a complete validated offset map with the same selection semantics;
offered/excluded progress still does not mean processed or committed work.

`EXPLAIN ANALYZE` identifies log versus KV scans, table and projected
columns, optional log batch predicate and partition pruning. Partition
counts describe discovered and selected partitions (recorded once, not once
per physical stream, regardless of which stream starts first).
`fluss_buckets_assigned` counts actual selected partition/bucket pairs assigned
to executed streams, including empty ranges. KV counts server-confirmed
sessions and successful `ScanKv` RPC responses (including empty pages), not
Arrow output batches. First-page latency includes request and decoding.
Projection is identified as `server` for initial-schema logs,
`client_evolved_schema` for evolved logs, `decoder` for projected KV,
`row_count_only` for KV COUNT, or `full_rows_for_count` for zero-column log scans. These are not network-byte or
server-only snapshot-opening measurements.

Each source reserves decoded Arrow backing buffers in the shared DataFusion pool.
The reservation is attached through Arrow's custom-allocation ownership API,
without copying values/offsets/validity buffers. Clones, slices and projections
keep the conservative whole-batch lease until the last retained source buffer is
dropped, even after the source stream ends or is cancelled. Downstream operators
that materialize new arrays account their own output/state; these leases cover
retained source buffers, not every possible SQL result allocation.

Decoded log batches waiting in the bounded/streaming reader are charged as a
separate queue reservation after polling, before offering output. On admission
failure, the source errors and releases its owned queue. Raw responses, batches
inside an unfinished client poll, decompression and remote files have separate
client limits: this is not a pre-decode or total RSS cap. The client poll's 64 MiB
decoded/raw cap is soft. An evolved scan reserves the full decoded backing
storage before projection, so retaining one projected buffer may conservatively
keep the entire batch charge. Array header wrappers allocate metadata; custom
buffer capacity reflects the visible view while the lease charges the original
backing capacities. Decoded/output byte metrics need not equal pool reservations.
Log and KV providers accept `with_max_retained_batch_bytes(bytes)`, default 64 MiB,
as an independent per-batch retention ceiling. Exceeding it produces native
ResourcesExhausted even with an unbounded pool. This check occurs after decode.
`fluss_retained_source_buffer_bytes` records live backing-storage leases rather
than just the latest pull; its value persists while consumers retain buffers.

Native source pulls now decode at most one batch through the client's limited
poll API. The connector's streaming decoded queue is removed; the bounded reader
also requests one batch rather than a bulk poll. The client bulk API remains
available with its existing soft byte cap. Batch polling does not await another
fetch after consuming output. See [read pressure verification](read-pressure-verification.md)
for bounds, cancellation and repeated remote/pressure evidence.

Batch log and KV partitions share a source-execution deadline initialized by the
first executing partition. A late partition receives the same deadline, not a
fresh timeout. Sequential reexecution/new contexts create fresh deadline scopes.
The deadline does not bound arbitrary downstream SQL work. Source expiry is an
inspectable `FlussScanTimeout` inside DataFusion's External error; client network/
storage errors preserve their original causes. Streaming idle remains unbounded.
Streaming initialization and topology operations use the connection remote-log
operation timeout; poll adds its normal idle interval to that finite allowance.
`FlussOperationTimeout` distinguishes that failure from a completion deadline.
Local identity/schema/topology/retention changes expose `FlussReadInvalidated`
reasons through DataFusion External errors without replacing protocol causes.

The catalog discovers names once; reload it after creating tables.
It selects the log or KV provider from the table's primary-key metadata.

`INSERT INTO` is a sink operation on either provider. The target schema and
table ID/schema ID are checked before writing and on each input batch.
DataFusion passes one asynchronous stream of Arrow batches to a writer
isolated for that statement. The existing Fluss client owns row/block grouping
and routing when batches mix partitions with different historical bucket counts.
Compatible log blocks use views/slices and interleaved destination groups use
Arrow gathers; KV retains its required row-format encoding. On logs INSERT appends. On KV tables,
INSERT performs full-row upsert; duplicate keys in one input count as two
submitted rows, and unordered parallel inputs have no guaranteed winner.
The sink waits for ACK after each batch, even when the input never ends.
The final `count` is available only after the source reaches EOF. Cancellation
or a later failure leaves already committed rows intact; no cross-batch
rollback, source/sink checkpoint, or exactly-once execution is promised.
Writer buffer budgets and ACK policy belong to Fluss Config; separate sink
options cap ACK waiting and retry attempts.

The sink charges retained input/cast/gather buffers, native encoding/transport,
routing metadata and key/row scratch to the real execution pool. Owners keep
reservations through worker/buffer/frame lifetimes, even after cancellation.
Input/operator charges can overlap conservatively; client queue slots have their
own configured limiter. Preparation and metadata checks are finite; a shared
enqueue/ACK deadline covers one batch, not idle input. Cancellation aborts the
dedicated writer and wakes blocked producers; sent requests may still commit.
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
merge engine, is rejected before sending. Optimized empty DELETE/UPDATE and
unsupported row-restriction protection are backported in the native DataFusion
core; see [DELETE contract](delete-contract.md) and vendor provenance.

The Rust working tree now plans finite-source MERGE using DataFusion full
joins and ordered CASE expressions. Predicates use SQL three-valued logic;
only the first eligible WHEN clause modifies each joined row. Matched
UPDATE/DELETE, unmatched INSERT and NOT MATCHED BY SOURCE use the existing
upsert/delete writer. INSERT requires all target columns; updating primary
or partition keys and unbounded MERGE sources is rejected explicitly.
The sink detects repeated modifying actions for a primary key across input
batches and budgets the encoded-key set in DataFusion memory. Detection of
a duplicate in a later batch cannot undo earlier confirmed modifications.
Snapshot selection remains per bucket, without conditional writes or global
isolation. Configured native merge-engine tables do not provide ordinary SQL row
replacement and are rejected for SQL MERGE. Exact boundaries and evidence are in
[MERGE contract](merge-contract.md).

DataFusion's FilterExec can coalesce small batches up to the configured session
batch_size. A smaller batch size can favor sparse streaming latency at a batching
cost; the source does not add another SQL filter. The native source/sink acceptance
and replay semantics are in [continuous INSERT acceptance](streaming-write-acceptance.md).
