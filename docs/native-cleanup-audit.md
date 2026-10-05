# Native source/cleanup inventory — rm21, prad, 7yt8

Working-tree audit, 2026-10-05. Phase 4 accepts the actual native DataFusion engine;
phase 6 removes substituted infrastructure and prepares native delivery. Neither
phase implements or accepts Python/FFI bindings.

The source series is now versioned: `2a85deb` retirement, `043a346` faults,
`1a4556b` native engine/example, `4b78eca` profiles/instrumentation. Export results
below remain historical evidence; final clean Git verification follows this series.

## Resolved inventory

| Piece | Disposition and evidence |
| --- | --- |
| Former active Python/FFI crate, host extensions/adapters, build scripts, Python examples/tests and root Python manifests | Removed in `e38t` working tree; root workspace contains only `fluss-datafusion`. No fallback references to FlussPlanner/OpaqueQueryPlanner/ResourceProvider/RuntimePlan/session_with_runtime/PyCapsule remain in active connector Rust sources. |
| Imported upstream Python/C++/Elixir binding source | Retained as imported history/reference, explicitly excluded from the client workspace. No active dependency/gate. Imported licenses/notices are retained. |
| Native client | Necessary single implementation of protocol/auth, metadata, bucket routing, codecs, scanners, writer buffers/queues/ACK/retries. Root execution and independent client validation use their own pinned locks. |
| DataFusion provider/execution adapters and catalog | Necessary TableProvider/ExecutionPlan integration, identity/offset/snapshot contracts and optional metadata catalog; real caller sessions/planners/runtime execute them. No alternate engine, transport or scheduler. |
| Native MERGE extension | Necessary adaptation for the pinned DataFusion API. Its helper graph uses caller planning and standard join/filter/CASE operators, not a second SQL/runtime implementation. Native UDF/planner tests and action/NULL/data checks exercise it. |
| `resources.rs`, native WriterMemoryAccounting and Arrow owner leases | Necessary admission/ownership adapters over the caller's real MemoryPool and native client owners. Last-buffer/worker/frame lifetime tests and pressure/cancel profiles justify retention; not a global allocator/RSS limiter. |
| Offsets, partition discovery and progress/terminal summaries | Necessary finite/continuous execution identity and conservative application knowledge. Reexecution/overlap/invalidation/lag/partial-ACK tests justify retention; not persistent checkpoints/replay. |
| Typed errors/options and fixed-label metrics | Necessary operation bounds and original error propagation. ScanKv now participates in the existing RPC metrics path, fixing unreported KV read bytes. No alternate retry hierarchy. |
| Vendored DataFusion core override | Necessary generic empty DELETE/UPDATE and unsupported-restriction protection for 55.1/Arrow59. Provenance/checksum and removal condition remain in `vendor/README.md`; three generic planner tests and real native DELETE tests verify it. |
| Profile-only Feed/collector/histograms | Necessary controlled StreamingTable input and bounded measurement over standard interfaces. They live only in tests/support, introduce no production pool/allocator/decoder/transport, and replace resetting debug-recorder measurements in the writer profile. |
| Native query example | Standard SessionContext/RuntimeEnv/providers, separate Fluss and engine configuration, output streamed by batch. No external application/scheduler is required for acceptance. |

Historical FFI findings remain identified in `rust-implementation-audit.md`. Lab
installations, wheels and external credentials are not project delivery artifacts
and were not removed or modified as a cleanup shortcut.

## Dependency graph and isolated-source evidence

An export of git-visible current source (tracked surviving files plus untracked
source/tests/docs, excluding ignored caches/secrets and `.git`) contained 4,281
files and SHA256 `a376f2ab58d320b580c4f7e97390236aa0eb67a151b448d5b5f608fb6f59273c`.
It was copied to `/tmp/opencode/native-source-a773908601384a5fb6cb43d308dde670`.
An initially empty, separate `CARGO_TARGET_DIR` rebuilt the native graph **offline
and locked**, using already fetched registry dependencies, in 4m30s.

- Root resolved graph: 453 packages; workspace `fluss-datafusion`; DataFusion
  55.1.0, Arrow 59.3.0, Fluss Rust client 1.0.0.
- Client resolved graph: 431 packages; workspace `fluss-rs`, `gen`,
  `fluss-test-cluster`, `fluss-examples`; its independent lock pins Arrow 59.0.0.
  As a root dependency, the client instead uses the root lock's Arrow 59.3.0.
  Both are Arrow59; this is not a bindings ABI/parity claim.
- Neither active resolved graph contains PyO3, arrow-pyarrow, datafusion-ffi,
  datafusion-ffi-ext, datafusion-python-util, fluss-datafusion-python, Stabby or
  Rustler. Optional/imported manifests are not mistaken for active resolved nodes.
- From the isolated source/target: **29 core + 3 planner**, **8 real write SQL
  (41.57s)** and **9 real live read SQL (78.83s)** passed. Credentials/CA/kubeconfig
  are external lab inputs, not copied into the export.

This proves an isolated build/run of that exported native working-tree snapshot.
The allocation-control option and native example were added after that export.
A final code snapshot of 4,284 files at
`/tmp/opencode/native-source-7027470b6d924c44999d3e2c27d8140b` has SHA256
`e1033ccaf11a6211ffab63206bbead9fd04e9cedb190f816bf4001d100222c80`.
The unchanged native code reused the isolated target cache; its 29+3 core/planner
tests pass. The example was built offline from that source and run as a separate
process against both log and KV tables in the isolated DML fixture: expected
counts 6 and 1, 16 MiB pool/target partitions 2, successful context/connection
cleanup. The DML/example case passed in 12.25s. Root all-target clippy with warnings
denied also passed offline from the final source (2m27s); original-tree checks pass.

After building the example in that source copy:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/opencode/native-repro-target-a7739086 cargo build -p fluss-datafusion --offline --locked --example native_query
FLUSS_NATIVE_QUERY_EXAMPLE=/tmp/opencode/native-repro-target-a7739086/debug/examples/native_query DATAFUSION_POOL_MIB=16 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/opencode/native-repro-target-a7739086 uv run --no-project --env-file /home/midnattsol/code/datalush/fluss/lab/.env cargo test -p fluss-datafusion --offline --locked --test write_sql insert_log_and_upsert_kv_from_sql -- --ignored --test-threads=1
```

Documentation result updates after export do not change its tested native code.
After adding the measured continuous-reader mode, the latest export is
`/tmp/opencode/native-source-8881fb542df84f43b689e4291533af43`, 4,284 files,
SHA256 `4ac0ada63aefeb5f7e635e62ad4f058772215117ef66a8738b47e50b31e82103`.
Its offline all-target clippy and example build pass using the isolated target;
the new unbounded reader/INSERT RELEASE profile also passed its full 60s/300s run.
One equivalent divisibility expression was changed to `is_multiple_of` after
clippy identified it; no profile policy/data path was changed. All source checks
and the client/test-cluster all-target clippy, formatting and diff checks pass.

These exports are not a clean Git checkout
of an authorized commit. `rm21`/phase closure still requires the final committed
source and final-profile evidence, rather than treating the old HEAD as current.

## Canonical documentation

- API/capabilities/delivery boundaries: `rust-contract.md`, with read/DML details
  in `reading-semantics.md`, `delete-contract.md`, `merge-contract.md`.
- Observations/metric meanings: `write-observation-contract.md`.
- Engine acceptance map: `native-engine-acceptance.md`.
- Permission/fault and pressure evidence: `native-failure-verification.md`,
  `read-pressure-verification.md`, `write-pressure-verification.md`.
- Measured workloads/results: `native-profile-plan.md`; historical remote/STS
  results stay in `production-readiness.md` with their actual scope/build mode.
- Entry points/build/example commands: root README. Its acceptance scope names
  native DataFusion directly rather than requiring an external Rust job engine.

This inventory resolves dispositions; it does not close the issues or assert
completed delivery before profiles, final-source verification and authorized
commits are present.

The now-versioned series has [clean Git verification](native-checkout-verification.md).
All listed native/code/example/fault checks passed; a planned RustFS smoke failed
at external endpoint preflight. Phase closure retains that blocker rather than
relabeling the source-export evidence as a complete current remote acceptance.
