# Reading semantics before engine adapters

The Rust client in `clients/rust/crates/fluss` already returns Arrow
`RecordBatch` objects. Its Python binding is in
`clients/rust/bindings/python`. Neither should be reimplemented here.

`TableScan::limit(n).create_bucket_batch_scanner(bucket)` yields at most `n`
rows **per bucket**. It is useful for explicit previews, but does not prove a
complete current view of a primary-key table. A log scanner yields changes,
not the current state of a primary-key table. Do not expose either as an
unqualified `read_table()` or an unrestricted SQL table.

Before adding a **primary-key** DataFusion `TableProvider`, define its bounded
full scan, deletion semantics, snapshot/log merge, cancellation, and the
consistency actually guaranteed between buckets. The existing append-only
log source relies on the client's bounded offset reader. Its predicate
pushdown only prunes batches, so filters must still be evaluated exactly
in the engine. Keep projection, limits, and partitions honest about these
semantics.

The first DataFusion provider supports **non-partitioned append-only logs**
only. It captures stopping offsets at execution time for one read of the
table's buckets and starts at each bucket's earliest **retained** offset;
another execution captures new offsets. The bounded read
finishes or fails with an explicit error on timeout. Offsets are collected
per bucket, not as a transactional cross-bucket snapshot. Filtering, SQL
limits and projection are performed by DataFusion over the source batches.
The read-only catalog discovers names once; reload it after creating tables.
It does not reinterpret KV changelogs as the current table state.

The Python adapters take **already bounded** PyArrow tables/readers from the
existing binding: DuckDB registers them for local SQL; Polars and pandas
materialize local DataFrames. They are not Fluss table providers. Reopen
one-shot readers for a second query, and bound data before materializing a
DataFrame.
