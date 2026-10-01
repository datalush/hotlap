# Reading semantics before engine adapters

The Rust client in `clients/rust/crates/fluss` already returns Arrow
`RecordBatch` objects. Its Python binding is in
`clients/rust/bindings/python`. Neither should be reimplemented here.

`TableScan::limit(n).create_bucket_batch_scanner(bucket)` yields at most `n`
rows **per bucket**. It is useful for explicit previews, but does not prove a
complete current view of a primary-key table. A log scanner yields changes,
not the current state of a primary-key table. Do not expose either as an
unqualified `read_table()` or an unrestricted SQL table.

Before registering a Fluss table as a DataFusion `TableProvider`, define a
bounded full scan for each table type, offset boundaries, deletion semantics,
snapshot/log merge for primary-key tables, cancellation, and what consistency
between buckets is actually guaranteed. Predicate pushdown for log scans
currently prunes batches; returned rows still require exact filtering in the
engine. Keep projection, limits, and partitions honest about these semantics.

The Python adapters take **already bounded** PyArrow tables/readers from the
existing binding: DuckDB registers them for local SQL; Polars and pandas
materialize local DataFrames. They are not Fluss table providers. Reopen
one-shot readers for a second query, and bound data before materializing a
DataFrame.
