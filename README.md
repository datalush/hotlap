# Fluss connectors scaffold

This repository contains source copies of the working Fluss Java and Rust
clients, plus a small workspace for engine integrations. The client Rust
workspace already produces Arrow record batches and includes the `pyfluss`
Python binding. No second Fluss protocol implementation is planned.

```text
clients/
  java/                    Fluss Maven reactor (build with ./mvnw -pl fluss-client -am)
  rust/                    Fluss Rust workspace and bindings/python
crates/fluss-datafusion/    DataFusion adapter scaffold (not yet a TableProvider)
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

Native DataFusion table registration and unrestricted Fluss scans are **not**
implemented yet. In particular, a limited per-bucket scan is not a full
primary-key table read; see [reading semantics](docs/reading-semantics.md).
