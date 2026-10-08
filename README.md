# Hotlap

Hotlap is a native Rust streaming engine with an Arrow-native incremental core
(`hotlap-engine`) and an embedded SQL/DDL surface (`hotlap-sql`). The
DataFusion-based Fluss provider integration (`fluss-datafusion`) and the Fluss
Rust client (`fluss-rs`) retain their component names and responsibilities; the
earlier `differential-dataflow` spike has been removed.

```text
crates/hotlap/             Public facade over the incremental core
crates/hotlap-core/        Engine contract (Plan IR, IncrementalCore, ZSetBatch)
crates/hotlap-engine/      Arrow-native incremental engine (no differential-dataflow)
crates/hotlap-connectors/  Source/Sink SPI and Fluss source/sink
crates/hotlap-runtime/     Kernel composition root, engine thread and checkpointing
crates/hotlap-sql/         Embedded SQL/DDL surface and catalog
crates/fluss-datafusion/   DataFusion Fluss provider integration
clients/rust/crates/fluss/ Native protocol, metadata, routing, Arrow codecs and writers
vendor/datafusion-55.1.0/  Published core with documented generic DELETE/UPDATE backport
docs/                      Contracts, ownership boundaries and verification evidence
```

The incremental engine owns Arrow-native Z-set batches and row-format key
encoding behind the `hotlap-core` `IncrementalCore` contract, so the
`differential-dataflow`/`timely` dependency is gone. `hotlap-connectors` carries
the Source/Sink SPI and the Fluss source/sink; `hotlap-runtime` is the
composition root that selects a kernel and drives the engine thread, so the
connector crate never depends on a concrete engine. `hotlap-sql` translates
embedded SQL/DDL into engine plans. Durability is engine-owned:
checkpoints and recovery, capacity-gated sink 2PC coordination and materialized
views created after `START` (see [durability](docs/hotlap-durability.md)).

The imported Rust client originates at `dc427e1290847b4a569b6745fcb87b256292bf6a`.
Its Apache licenses/notices remain intact. Java/reference and non-Rust bindings
are removed from the current tree; their provenance remains in Git history.
The client includes the protocol schema needed for regeneration without Java.
See [Hotlap layout and migration](docs/hotlap-layout.md).
The repository history is also [selectively cleaned](docs/history-cleanup.md);
older evidence SHA identifiers resolve through the preserved old→new commit map.

## Native API and semantics

- `FlussLogTable::open` reads a finite batch range captured per execution.
- `FlussLogTable::open_with_options(..., LogReadOptions::default())` streams from
  earliest retained offsets; explicit latest/complete offset maps are supported.
- `FlussKvTable::open` reads finite server KV snapshots, one per bucket rather
  than one atomic cross-bucket transaction.
- Register providers explicitly or load the optional snapshot `FlussCatalog`.
- SQL `INSERT INTO` appends to logs or sends native full-row KV upserts, finite
  or continuous. Native writers handle mixed partitions/effective bucket layouts.
- SQL KV DELETE selects snapshot keys with exact DataFusion predicates and
  requires an allowing table policy. MERGE uses native join/filter/CASE operators,
  requires a finite source and ordinary KV replacement semantics, and cannot
  change primary/partition keys or perform partial-column INSERT.
- `subscribe_progress()` observes offered/excluded source positions;
  `subscribe_writes()` observes ACKs and conservative partial outcomes. Neither
  is an engine checkpoint. Final SQL write counts appear only after EOF/success.

There is no statement rollback, conditional write, globally consistent snapshot,
automatic job replay or exactly-once source/sink commit. Engine/application owns
pool policy, concurrency, job state, checkpoints and reconciliation. Source/sink
leases and native writer guards account retained resources in the real supplied
DataFusion pool, not a separate allocator or a process-RSS limit.

The dependency stack remains DataFusion 55.1 / Arrow 59. The core backport preserves
empty optimized DELETE/UPDATE selection and rejects unsupported row restrictions;
see [vendor provenance](vendor/README.md).

The runnable native example uses an explicit provider, the normal DataFusion
parser/planner/operators and a bounded host pool. Results are printed batch by
batch rather than collected into an unbounded output Vec:

```sh
FLUSS_BOOTSTRAP=localhost:9123 DATAFUSION_POOL_MIB=64 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo run --locked -p fluss-datafusion --example native_query -- my_database my_table log 'SELECT COUNT(*) FROM fluss_source'
```

Use `kv` for a primary-key table. Fluss TLS/SASL settings use `FLUSS_CA_FILE`,
`FLUSS_USER` and `FLUSS_PASSWORD`; engine memory/concurrency remain separate
`DATAFUSION_*` settings. The default SQL is COUNT; supplied SQL is planned by
DataFusion with the registered name `fluss_source`.

## Build and verification

Functional builds use DEBUG and eight jobs. Release is for profiles/delivery.

```sh
cargo metadata --no-deps --format-version 1
cargo metadata --no-deps --format-version 1 --manifest-path clients/rust/Cargo.toml
cargo fmt --all -- --check
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```

Live native-sni tests use exported `FLUSS_BOOTSTRAP`, `FLUSS_CA_FILE`, `FLUSS_USER`
and `FLUSS_PASSWORD` credentials. They create isolated tables and clean them up.
An optional environment-file launcher can load the lab's ignored `.env` without
any Python package/virtualenv dependency in this project:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
```

Long storage/STS/resource profiles have separate opt-in commands and isolated
object prefixes; consult their evidence before rerunning them. Credentials and
local build outputs are never part of the source repository.

## Current acceptance scope

Architecture, reads and finite/continuous writes have verified scoped milestones.
Native functional/failure checks and final sustained profiles have scoped evidence.
Clean Git verification is [recorded](docs/native-checkout-verification.md), including
the passing remote-read smoke after recovery of the existing RustFS endpoint. The
engine under acceptance is **DataFusion native in this repository**: caller
SessionState/RuntimeEnv, planning/operators, concurrency/backpressure,
cancellation/reexecution and recovery.
Provider tests alone do not establish those engine guarantees. FFI/Python is not
an active delivery phase or a dependency of Rust validation.

- [Canonical Rust contract](docs/rust-contract.md)
- [Reading semantics](docs/reading-semantics.md)
- [Implementation audit/history](docs/rust-implementation-audit.md)
- [Production-readiness evidence](docs/production-readiness.md)
- [Read pressure](docs/read-pressure-verification.md)
- [Write pressure](docs/write-pressure-verification.md)
- [Write observations](docs/write-observation-contract.md)
- [DELETE](docs/delete-contract.md) and [MERGE](docs/merge-contract.md)
- [Continuous INSERT acceptance](docs/streaming-write-acceptance.md)
- [Native engine acceptance](docs/native-engine-acceptance.md)
- [Native failures](docs/native-failure-verification.md)
- [Final-route profiles](docs/native-profile-plan.md)
