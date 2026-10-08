# Hotlap: native Rust project layout

Hotlap is the project/engine identity. It ships its own Arrow-native incremental
engine (`hotlap-engine`) behind the `hotlap-core` contract, with `hotlap` as the
public facade; `hotlap-sql` and `hotlap-connectors` are the SQL-plan and runtime
boundaries. DataFusion remains the SQL/planning/operator/runtime foundation for
the Fluss provider integration in `fluss-datafusion`, and `fluss-rs` is the
native protocol/client implementation. The earlier `differential-dataflow` spike
has been removed. Component names and public API versions remained stable across
the Rust-only migration.

> **Superseded (2026-10-06):** Hotlap is now an explicit **engine** layer built on
> DataFusion. Its incremental core is the Arrow-native `hotlap-engine` kernel
> behind an `IncrementalCore` boundary; the earlier
> `differential-dataflow` spike was replaced. The Fluss provider lives in
> `fluss-datafusion`. See
> [incremental core decision and boundary](hotlap-incremental-core.md).

## Current tree

```text
hotlap/
  Cargo.toml / Cargo.lock
  crates/hotlap/            # public facade over IncrementalCore
  crates/hotlap-core/       # engine contract (Plan, IncrementalCore, ZSetBatch)
  crates/hotlap-engine/     # Arrow-native incremental engine kernel
  crates/hotlap-sql/        # SQL to Plan boundary
  crates/hotlap-connectors/ # runtime/connector integration
  crates/fluss-datafusion/  # DataFusion Fluss provider integration
  clients/rust/
  vendor/datafusion-55.1.0/
  docs/
  LICENSE / NOTICE
```

The Java/reference tree, Python/C++/Elixir bindings, imported website and binding-
specific build/release tooling have been removed. Git history retains their
import provenance. Original notices/licenses for retained code are preserved.
Local Java ignored files are archived outside the repository rather than erased.

The Rust client includes `proto/FlussApi.proto` and checked-in generated Rust;
normal builds have no Java dependency. Regeneration uses the vendored schema,
not an external monorepo fallback. The short STS fixture is now Rust and reuses
AWS CLI signing against the existing RustFS, preserving its 900s/policy contract.
There are no Python sources required by Hotlap. Shell build/protocol utilities
and external Docker/AWS CLI/lab credentials remain tooling/runtime dependencies.

## Paths and identity

The project directory moves from `fluss-connectors` to sibling `hotlap`; sibling
`../lab/.env` paths remain valid. Historical evidence documents retain the original
paths/SHA/artifact names as history, not current installation instructions.
The Kata project is renamed in place, preserving its issue IDs/history.
Git linked worktrees are repaired after the main-directory move. The configured
Git remote is preserved; this operation does not rename a GitHub repository.

All paths used for current commands should be relative to the Hotlap root or use
the new directory. There is no compatibility symlink/fallback at the old path.
Native functional checks use DEBUG/eight jobs; profiles use RELEASE/eight jobs.

## Migration verification

From `/home/midnattsol/code/datalush/fluss/hotlap`, after removing the imported
trees and repairing linked worktrees:

- Root workspace: 29 core and 3 planner tests pass; opt-in integrations compile.
- Client: 836 tests pass, 2 ignored, both serially and on the final default-parallel
  rerun. The first parallel run observed a metrics-counter test failure (2 vs 1);
  serial/default reruns pass without changing client source. This observation is
  retained rather than claiming every run passed or a proven cause.
- Root/client four-member workspace all-target clippy with warnings denied,
  formatting and diff checks pass. The Rust `gen` crate compiles with its
  vendored schema and no Java path/fallback.
- Native-sni SQL: eight write/DML/streaming cases pass (44.16s), including the
  separately built native query example against log and KV from the new path.
- The new Rust STS endpoint passes the real RustFS **preflight** (15.84s): actual
  server 900-second credentials are issued and the first remote scan succeeds.
  This does not claim a repeated 900-second expiry/renewal run; the historical
  full-expiry evidence remains separately scoped.
- Locked resolved graphs remain 453 root / 431 client packages, with unchanged
  DataFusion55.1/Arrow59 and no active non-Rust binding dependencies.
- Existing linked worktrees retain their HEADs and resolve the new common Git
  directory. Kata remains project ID26 with its original issue history, default
  binding `hotlap`, and the obsolete local directory alias removed.

Removed source/ignored local artifacts are preserved under
`/tmp/opencode/hotlap-retired-source`, including the entire Java tree, bindings,
website/tooling and old project virtualenv/wheels/caches. This archive is not a
build fallback or deliverable. External `../lab` installations were untouched.

The directory rename is a local filesystem operation. Git versions the
Rust-only source, branding and metadata
changes, while GitHub remote repository naming is a separate operation.
