# Fluss connectors

This repository contains source copies of the working Fluss Java and Rust
clients, plus a small workspace for engine integrations. The client Rust
workspace already produces Arrow record batches and includes the `pyfluss`
Python binding. No second Fluss protocol implementation is planned.

```text
clients/
  java/                    Fluss Maven reactor (build with ./mvnw -pl fluss-client -am)
  rust/                    Fluss Rust workspace and bindings/python
crates/fluss-datafusion/    Bounded DataFusion source for append-only log tables
python/fluss_connectors/   Small DuckDB/Polars/pandas adapters for Arrow results
docs/reading-semantics.md  Contracts to satisfy before claiming full scans
```

The Java and Rust trees are source copies of the `fluss-clients` checkout at
`dc427e1290847b4a569b6745fcb87b256292bf6a`. Their original Apache 2.0
license headers, LICENSE, and NOTICE files are retained. No build outputs,
local credentials, or Git history were copied into these trees. The only
change inside a copied client is in `clients/rust/bindings/python/src/config.rs`:
the Python `Config` now accepts the TLS settings already supported by its
underlying Rust client.

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

## DataFusion (append-only log tables)

`FlussLogTable::open` registers an explicit, non-partitioned Fluss log table
in DataFusion. `FlussCatalog::load` discovers database/table names once and
allows SQL such as `fluss.lab_spark.demo_log` (reload the catalog after DDL).
KV tables remain visible in that catalog but return an explicit unsupported
error if queried. Execution captures each bucket's latest offset once, streams
Arrow batches from the earliest **retained** offsets until those stopping
offsets, and errors on timeout rather than claiming a partial result is
complete. DataFusion's required non-empty projection is pushed to Fluss;
`COUNT(*)` still fetches rows because Fluss cannot scan zero columns. Filters
and global limits remain DataFusion's responsibility; no Fluss filter/limit
pushdown is claimed. Each query opens a new finite read. KV and partitioned
tables are rejected.

With the isolated lab running and its ignored `.env` in `../lab/`:

```bash
uv run --env-file ../lab/.env cargo run -p fluss-datafusion --example query -- lab_spark demo_log
```

For the isolated lab integration tests (empty log, new writes visible on the
next execution, COUNT, source projection, exact SQL filter, timeout failure,
and rejection of a KV table):

```bash
uv run --env-file ../lab/.env cargo test -p fluss-datafusion --test live_log_sql -- --ignored
```

The source currently runs one execution partition across the table's buckets;
it does not promise a globally atomic snapshot across buckets. A limited
per-bucket scan is not a full primary-key table read; see
[reading semantics](docs/reading-semantics.md).
