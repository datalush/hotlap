# Fluss connectors

This repository contains source copies of the working Fluss Java and Rust
clients, plus a small workspace for engine integrations. The client Rust
workspace already produces Arrow record batches and includes the `pyfluss`
Python binding. No second Fluss protocol implementation is planned.

```text
clients/
  java/                    Fluss Maven reactor (build with ./mvnw -pl fluss-client -am)
  rust/                    Fluss Rust workspace and bindings/python
crates/fluss-datafusion/    Batch/streaming log and snapshot KV providers
crates/fluss-datafusion-python/   DataFusion Python FFI bridge for those providers
python/fluss_connectors/   DataFusion, DuckDB, Polars and pandas integration
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

`FlussLogTable::open` preserves the existing finite/batch API;
`FlussLogTable::open_with_options(..., LogReadOptions::default())` follows new
log events. `FlussKvTable::open` reads a finite KV snapshot. Register tables
explicitly. `FlussCatalog::load` discovers database/table names once and
selects the appropriate provider, allowing SQL such as
`fluss.lab_spark.demo_log` or a KV table (reload the catalog after DDL).

For **batch logs**, execution captures each bucket's earliest retained and latest
offsets once, streams Arrow batches until their stopping offsets, and errors
on timeout rather than claiming a partial result is complete. For **streaming
logs**, the same scanner subscribes from earliest, latest or complete explicit
offsets and stays open across idle periods. DataFusion declares this source
unbounded with incremental emission and no global order. The scanner's network
timeout remains a connection setting; the batch-wide timeout does not terminate
an idle stream. After the source detects a changed table/schema or partition
topology, it fails explicitly rather than silently skipping new data. An
execution can subscribe to source-side `LogDelivery` events: `(execution_id,
bucket, base_offset, next_offset, rows)`. These describe batches offered by
the source, not bytes prefetched, rows surviving SQL filters or data committed
by an engine/sink; a lagged observer must not be used for resumption.
DataFusion's required non-empty projection is pushed to Fluss;
`COUNT(*)` still fetches rows because Fluss cannot scan zero columns. Simple
`Int32`/`Int64` comparisons and safe `AND` clauses can prune Fluss record
batches; pushdown is **Inexact** and DataFusion always evaluates the full
filter again. `OR`, unsupported expressions and out-of-range literals stay
in DataFusion. Global limits are not pushed per bucket. Each batch query opens
a new finite read; a streaming query continues until cancelled or a compatible
`LIMIT` completes it. `COUNT(*)`/global `ORDER BY` over an unbounded source
do not produce a finite final answer: select batch explicitly for those.

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

The native providers use **DataFusion Rust 55.1 / Arrow 59**. The optional
`crates/fluss-datafusion-python` wheel exports those very providers to the
upstream Python `SessionContext` through `datafusion-ffi`. It requires
**DataFusion Python 55.0.0** (whose Rust engine is 55.1); the latest published
wheel is still 54.0.0, so build the matching Python host from the pinned
upstream revision. Do not use the 54.0.0 wheel with these providers.

For a reproducible Python 3.12 development installation, starting from this
checkout (use a fresh virtualenv and a separate directory for upstream):

```bash
git clone https://github.com/apache/datafusion-python.git /tmp/datafusion-python-55
git -C /tmp/datafusion-python-55 checkout 5ef2856f5b02cddcd3d7d3559669d95ef181ccf4
uv venv /tmp/fluss-datafusion-env --python 3.12
uv pip install --python /tmp/fluss-datafusion-env/bin/python 'maturin>=1.9,<2' 'pyarrow==25.0.1' cloudpickle typing-extensions
# In /tmp/datafusion-python-55:
VIRTUAL_ENV=/tmp/fluss-datafusion-env CARGO_BUILD_JOBS=2 /tmp/fluss-datafusion-env/bin/maturin develop --uv
# In crates/fluss-datafusion-python of this checkout:
VIRTUAL_ENV=/tmp/fluss-datafusion-env CARGO_BUILD_JOBS=2 /tmp/fluss-datafusion-env/bin/maturin develop --uv
# Back in this checkout's root:
uv pip install --python /tmp/fluss-datafusion-env/bin/python --no-deps -e .
```

For the **wheel-install check**, build three wheels (`maturin build --profile
dev --out /tmp/fluss-wheels` from upstream and the FFI crate, and `uv build
--wheel --out-dir /tmp/fluss-wheels` from this repository root). Install those
wheels into a *different*, fresh Python 3.12 virtualenv with `uv pip install
--no-deps`, then install `pyarrow==25.0.1`, `cloudpickle`, the local
`clients/rust/bindings/python` package, and check the complete environment
with `uv pip check`. This wheel-install check passed, including all six Python
tests. It uses unoptimized dev wheels to check the installation/FFI contract;
it is not a release-build performance measurement.

Pass a dictionary of Rust `fluss::config::Config` field names and typed values
to `fluss_connectors.datafusion.Connection`. Configure DataFusion's own memory
pool and partition target on its `SessionContext`. `register_log` and
`register_kv` also accept `timeout_ms`, `max_partitions`, and
`max_assigned_buckets`. Log mode defaults to streaming in this new API;
`mode="batch"` keeps finite SQL queries finite. Register both explicitly:

```python
import datafusion
from fluss_connectors.datafusion import Connection, register_kv, register_log

connection = Connection({"bootstrap_servers": "localhost:9123"})
ctx = datafusion.SessionContext(
    datafusion.SessionConfig().with_target_partitions(2),
    datafusion.RuntimeEnvBuilder().with_greedy_memory_pool(256 * 1024 * 1024),
)
register_log(ctx, connection, "events", "my_database", "events", mode="batch")
register_kv(ctx, connection, "current", "my_database", "current")
rows = ctx.sql("SELECT COUNT(*) FROM events").collect()
# Keep the context alive while queries run; close the connection when finished.
connection.close()
```

For a continuous log read, use a dedicated registration and iterate the
DataFusion stream rather than calling `.collect()` without a `LIMIT`:

```python
# DataFusion's FilterExec batches filtered rows. Use a small batch_size when
# individual events must be visible promptly; larger values favor throughput.
live_ctx = datafusion.SessionContext(
    datafusion.SessionConfig().with_target_partitions(1).with_batch_size(1),
)
live = register_log(live_ctx, connection, "live_events", "my_database", "events",
                    start="latest")  # mode="streaming" by default
observed = live.subscribe_deliveries()  # subscribe before executing
for batch in live_ctx.sql("SELECT id FROM live_events WHERE id >= 1").execute_stream():
    print(batch.to_pyarrow())
    print(observed.drain())  # source-side progress, NOT a checkpoint
    break  # dropping the iterator cancels the source
live_ctx.deregister_table("live_events")
```

To resume from explicit positions, pass `start="offsets"` and a complete
`start_offsets={(table_id, partition_id_or_None, bucket_id): next_offset}` map.
Wrong table IDs, missing buckets, offsets outside retention and schema or
topology changes fail rather than silently completing. `LogReadOptions` in
Rust and Python keeps these read settings separate from connection settings.

### SQL writes through the same providers

DataFusion `INSERT INTO` consumes Arrow batches and uses the Rust Fluss
writer, including from Python over FFI. On a log it appends; on a KV table
it **upserts full rows** using the primary key. The `count` returned by a
finite INSERT counts input rows acknowledged by Fluss, not distinct KV keys.
If the registered tables have the columns `id` and `value`, for example:

```python
from fluss_connectors.datafusion import WriteOptions

# Supply optional per-connection defaults when opening a *new* Connection:
# connection = Connection(settings, WriteOptions(ack_timeout_ms=10_000, max_retries=2))
ctx.sql("INSERT INTO events (id, value) VALUES (1, 'new event')").collect()
ctx.sql("INSERT INTO current (id, value) VALUES (1, 'new state')").collect()
ctx.sql("INSERT INTO events SELECT id, value FROM another_finite_source").collect()
```

Each INSERT creates an independent Fluss writer. Its ACKs are checked after
each input batch (also for sparse continuous inputs), writer memory remains
bounded by the Fluss client settings, and this provider caps its own writer
retry budget (default 3) and ACK wait (default 30s). A batch may mix Fluss
partitions: its rows are routed individually by the client, including when
old and new partitions have different bucket counts after rescale. An
INSERT may have already committed some rows when a later batch errors or
the statement is cancelled; **there is no rollback or exactly-once job
checkpoint**. Retries of a log INSERT may append duplicates. SQL overwrite,
update and merge are not implemented. Rust KV providers now implement
`DELETE ... WHERE` (or all snapshot-selected keys without WHERE), using
DataFusion predicates and Fluss deletes. It requires a table policy allowing
deletes and does not offer conditional deletion or global isolation.
SQL DELETE is not yet transported through the current DataFusion Python FFI.

The current Rust sink charges its retained input batch to the DataFusion
memory pool; the client writer buffer has its own per-execution limit.
Its timeout covers enqueue plus ACK for one batch; cancellation cooperatively
stops the blocking row loop and aborts the writer, without rollback of writes
already sent. Fire-and-forget `writer_acks=0` is rejected for SQL DML.
These post-dev2 changes are in the working tree: the installed dev2 wheel
still contains the previous INSERT implementation, not Rust DELETE or this
additional hardening. A new uniquely versioned wheel is required to ship them.

A continuous `INSERT INTO destination SELECT ... FROM live_source` sends and
acknowledges batches while the source is running. Like other unbounded DML,
its `count` result does not appear until the input ends: start consuming its
DataFusion `execute_stream()` in an async task and cancel the task to stop.
For a sparse SQL filter use a small DataFusion session `batch_size` as above;
it controls DataFusion's FilterExec coalescing, not the Fluss writer's ACK.

The opt-in live FFI check queries populated log and KV tables on the isolated
lab using `../lab/.env` and these installed wheels:

```bash
FLUSS_PYTHON_LIVE=1 uv run --no-project --env-file ../lab/.env /tmp/fluss-datafusion-env/bin/python -m unittest discover -s tests/python -p test_datafusion_live.py -v
```

Use `FLUSS_PY_DATABASE`, `FLUSS_PY_LOG`, and `FLUSS_PY_KV` to override the
default existing fixtures `lab_spark.demo_log` and `lab_spark.demo`. The
live test also creates/deletes one isolated log to check that the Python FFI
stream receives writes made after the query starts. The binding does not
expose an alternate protocol, SQL executor or retry loop.

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
