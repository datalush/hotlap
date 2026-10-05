# Fluss Rust–DataFusion

Native Rust providers for Apache Fluss logs and primary-key tables, built on the
existing Fluss Rust client and DataFusion/Arrow. The active project is **Rust only**:
FFI/Python integration and its development host/adapters have been removed.
Bindings may be reconsidered only after complete Rust and consuming-engine acceptance.

```text
clients/rust/crates/fluss/  Native protocol, metadata, routing, Arrow codecs and writers
clients/java/              Imported Fluss server/client reference source
crates/fluss-datafusion/   Native providers, planning adapters, observations and tests
vendor/datafusion-55.1.0/  Published core with documented generic DELETE/UPDATE backport
docs/                     Contracts, ownership boundaries and verification evidence
```

The imported client trees originate at `dc427e1290847b4a569b6745fcb87b256292bf6a`.
Their Apache licenses/notices remain intact. The client validation workspace contains
only Rust crates; imported upstream bindings are excluded from that workspace.

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
Clean Git verification is [recorded](docs/native-checkout-verification.md); its
remote-read smoke currently awaits availability of the configured RustFS endpoint. The
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
