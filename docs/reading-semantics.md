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
Only non-partitioned KV tables are supported. No filter or global SQL limit is
pushed into `ScanKv`; DataFusion evaluates them exactly. Non-empty projections
are applied by the Arrow decoder after reading value records, and `COUNT(*)`
still decodes full rows. A changed schema or bucket layout requires replanning.

The append-only log source relies on the client's bounded offset reader. Its predicate
pushdown only prunes batches, so filters must still be evaluated exactly
in the engine. Only representable `Int32`/`Int64` comparisons are translated;
conjuncts may be pushed independently because DataFusion retains the whole
expression. `OR` and lossy numeric conversions are not pushed. A log table
needs `table.statistics.columns` set before writing for those batches to
carry useful pruning statistics. Keep projection, limits, and partitions
honest about these semantics.

The log provider supports **non-partitioned append-only logs**. Both providers
use the minimum of Fluss buckets,
DataFusion's target parallelism and an optional positive connector cap;
each reads a group of buckets. They share one offset capture per execution.
Each bucket starts at its earliest
**retained** offset; reusing the physical source plan with a new TaskContext
captures new offsets. The bounded read
finishes or fails with an explicit error on timeout. Offsets are collected
per bucket, not as a transactional cross-bucket snapshot. Non-empty SQL
projections are requested from the Fluss scanner; zero-column `COUNT(*)`
still fetches full rows before stripping columns locally. Exact filtering
and global SQL limits remain DataFusion operations.
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
The read-only catalog discovers names once; reload it after creating tables.
It selects the log or KV provider from the table's primary-key metadata.

The Python adapters take **already bounded** PyArrow tables/readers from the
existing binding: DuckDB registers them for local SQL; Polars and pandas
materialize local DataFrames. They are not Fluss table providers. Reopen
one-shot readers for a second query, and bound data before materializing a
DataFrame.
