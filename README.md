# Fluss connectors

This repository contains source copies of the working Fluss Java and Rust
clients, plus a small workspace for engine integrations. The client Rust
workspace already produces Arrow record batches and includes the `pyfluss`
Python binding. No second Fluss protocol implementation is planned.

```text
clients/
  java/                    Fluss Maven reactor (build with ./mvnw -pl fluss-client -am)
  rust/                    Fluss Rust workspace and bindings/python
crates/fluss-datafusion/    Bounded DataFusion sources for logs and KV tables
python/fluss_connectors/   Small DuckDB/Polars/pandas adapters for Arrow results
docs/reading-semantics.md  Contracts to satisfy before claiming full scans
docs/production-readiness.md  Verified guarantees, limits, and checks
```

The Java and Rust trees are source copies of the `fluss-clients` checkout at
`dc427e1290847b4a569b6745fcb87b256292bf6a`. Their original Apache 2.0
license headers, LICENSE, and NOTICE files are retained. No build outputs,
local credentials, or Git history were copied into these trees. Changes in
the copied Rust client expose its existing TLS settings in
`bindings/python/src/config.rs`, recognize offset progress when server-side
pruning returns no batches, and provide a paginated `ScanKv` snapshot reader
using its existing value-record-to-Arrow decoder.

## Validate the scaffold

```bash
cargo metadata --no-deps --format-version 1
cargo metadata --no-deps --format-version 1 --manifest-path clients/rust/Cargo.toml
(cd clients/java && JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64 \
  ./mvnw -pl fluss-client -am -DskipTests package)
uv lock
```

Python dependencies are optional by engine: `uv sync --extra duckdb`,
`uv sync --extra polars`, `uv sync --extra pandas`, or `uv sync --extra all`.
The `pyfluss` dependency points at the copied Rust binding; building it needs
a Rust toolchain. The Python adapters accept Arrow results **already read** by
that binding; the DuckDB helper registers a local Arrow result, not a live
Fluss table. Run their in-memory contract tests with:

```bash
uv run --extra all python -m unittest discover -s tests/python
```

For an explicitly **limited preview** of a Fluss table from this machine,
use the copied Rust client and the lab's ignored `.env` file:

```bash
uv run --extra all --env-file ../lab/.env python examples/bounded_preview.py \
  lab_spark demo --per-bucket-limit 10
```

This example passes the resulting Arrow data to DuckDB, Polars and pandas.
Its limit is **per bucket**, not a complete current-state table scan. It
requires the Fluss bootstrap, CA, SASL user and password in the environment;
no credentials are stored in this repository.

## DataFusion (logs and primary-key tables)

`FlussLogTable::open` and `FlussKvTable::open` register tables
explicitly. `FlussCatalog::load` discovers database/table names once and
selects the appropriate provider, allowing SQL such as
`fluss.lab_spark.demo_log` or a KV table (reload the catalog after DDL).

For **logs**, execution captures each bucket's earliest retained and latest
offsets once, streams Arrow batches from those starting offsets until their stopping
offsets, and errors on timeout rather than claiming a partial result is
complete. DataFusion's required non-empty projection is pushed to Fluss;
`COUNT(*)` still fetches rows because Fluss cannot scan zero columns. Simple
`Int32`/`Int64` comparisons and safe `AND` clauses can prune Fluss record
batches; pushdown is **Inexact** and DataFusion always evaluates the full
filter again. `OR`, unsupported expressions and out-of-range literals stay
in DataFusion. Global limits are not pushed per bucket. Each query opens a
new finite read.

For **KV**, the server opens an isolated RocksDB snapshot for each bucket's
`ScanKv` session. The Rust reader paginates and decodes every live row,
including upserts and deletions, into Arrow. Each execution opens fresh
sessions. SQL filters and limits are applied by DataFusion; nonempty column
projections are pushed into the Arrow decoder. There is no single
transactional snapshot shared between buckets: sessions start when each
bucket is first read, so concurrent writes may be visible in some buckets
but not others. A failure, expired session or timeout fails the query rather
than returning an incomplete table. If an in-flight KV continuation is
cancelled, the scanner cannot resume it: open a fresh query instead.

Partitioned **logs and KV** discover their partition list once at execution
start; the selected partition/bucket pairs are distributed over DataFusion's
physical scan partitions. Safe `region = 'north'`-style string equalities on
partition keys prune partitions. `AND` may contribute a supported conjunct;
`OR` never prunes. Filters remain exact in DataFusion. New partitions created
after discovery are visible on the next execution, and removed partitions may
cause an in-progress scan to fail. After a partitioned-table bucket rescale,
old and new partitions are each scanned using their own reported bucket count;
the table default is not used for old partitions. Missing or invalid
per-partition counts cause an error instead of an incomplete result. The
integration test rescales the table default from two to three buckets and
verifies SQL over both layouts, including rows stored in the new third bucket.

To see actual pruning, enable batch statistics **when creating the log table**
with `table.statistics.columns: id` (or `*`). Existing batches written without
statistics cannot be pruned retroactively. The integration tests create their
own tables in `datafusion_tests` and remove them afterward; their fully
pruned query finishes without exhausting the overall timeout.

With the isolated lab running and its ignored `.env` in `../lab/`:

```bash
uv run --env-file ../lab/.env cargo run -p fluss-datafusion --example query -- lab_spark demo_log
```

For the isolated lab integration tests (empty log, new writes visible on the
next execution, COUNT, source projection, exact SQL filter, configurable
parallelism, several batches, metrics, timeout failure, LIMIT cancellation,
KV state after upserts and deletions, pagination, and catalog dispatch):

```bash
uv run --env-file ../lab/.env cargo test -p fluss-datafusion --test live_log_sql -- --ignored
```

The native table providers currently use **DataFusion Rust 55.1**. Registering
them in the upstream `datafusion` Python `SessionContext` requires a matching
major version through `datafusion-ffi`; the published Python wheel is 54.0,
so this repository does not advertise an incompatible Python provider.

The sources use `min(buckets, DataFusion target_partitions)`, optionally capped
with `FlussLogTable::with_max_partitions(n)` or
`FlussKvTable::with_max_partitions(n)` (positive `n` only). Log partitions share
**one** capture of latest offsets for each query;
reusing a physical source plan with a new TaskContext captures new offsets.
Concurrent executions of that plan must use distinct `TaskContext` instances
(as normal DataFusion queries do); sharing the *same* context across
overlapping executions does not define a separate query identity. A late
partition in one execution keeps that execution's captured offsets, even if
new writes arrive before it starts.
This is not a globally atomic snapshot across buckets. A limited
per-bucket scan is not a full primary-key table read; see
[reading semantics](docs/reading-semantics.md).

`EXPLAIN ANALYZE` exposes assigned bucket groups, `output_rows` and batches,
`fluss_offset_capture_time`, `fluss_read_time`, `arrow_decoded_bytes`,
`arrow_output_bytes` and `fluss_peak_decoded_arrow_batch_bytes`. These byte
figures describe decoded/delivered Arrow arrays; **none measures network
traffic**. The peak is the largest decoded batch in a partition, not process
RSS or total query memory. A `LIMIT` may consume one batch containing more
rows than the SQL limit before cancelling the source.
Use `EXPLAIN ANALYZE VERBOSE` to see the `buckets` label for each physical
partition.
`fluss_active_partition_streams` falls to zero when source streams finish,
fail or are dropped by cancellation. The lab integration writes 512 rows in
multiple batches, checks the largest decoded batch remains smaller than the
sum of decoded bytes, and verifies this active-stream gauge after `LIMIT` and
timeout. This measures stream lifecycle and batch size, not JVM/Rust process
RSS or a bound on DataFusion's downstream operators.
