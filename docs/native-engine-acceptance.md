# Native DataFusion engine acceptance — ydvk

The engine is this repository's DataFusion SessionContext/SessionState and
RuntimeEnv, executing the real Fluss Rust providers. No external application or
new scheduler/checkpoint implementation is required for this gate.

## Stack and reproducibility boundary

Working-tree evidence on 2026-10-05, based on
`adc4f70f760904e795a9cd81a7ce728581286f55` (changes not committed): DataFusion
55.1.0, resolved Arrow 59.3.0, Fluss Rust client 1.0.0 and Docker Fluss
`ghcr.io/midnattsol/fluss:1.0.0-midnattsol.6`. The native DataFusion planner
backport and original archive checksum are documented in [vendor provenance](../vendor/README.md).
The root and client Cargo locks pin the active native graphs.

Caller-session/operator/example coverage is versioned in `1a4556b`, following
retirement `2a85deb` and failure hardening `043a346`. Profile harnesses and the
ScanKv instrumentation are versioned in `4b78eca`. The recorded working-tree runs
below precede those commits; clean Git verification is reported separately.

Clean-checkout reproduction remains `rm21`, after authorized commits include
the source/test changes and `e38t` removal. Running this working tree is not
evidence that the current HEAD alone reproduces the new tests.

## Engine-level coverage

| Contract | Actual engine execution / assertion |
| --- | --- |
| Caller session/planner/UDF/runtime | `write_sql::insert_log_and_upsert_kv_from_sql` combines RecordingPlanner, registered `is_two` UDF, target partitions 4 and the caller's bounded 16 MiB GreedyMemoryPool. Pool identity is checked. DELETE and MERGE helper graphs pass through the caller's planner; both use its UDF. |
| Native sink distribution | That test supplies three input partitions; the standard optimizer enforces the sink's single-partition requirement. Executing the same physical INSERT twice stores two copies, with explicit final-data checks. |
| Operators and NULL semantics | `live_log_sql::empty_log_and_new_offsets_on_each_query` compares seven real Fluss SELECTs against the same engine reading an Arrow reference: projections, residual comparisons, NULL filters, grouped COUNT/SUM, inner/left joins and ORDER BY NULLS FIRST/LIMIT. |
| Host resource policy | The reference test uses target partitions 2, batch size 64, a 16 MiB pool and native sort spill reservations of 1 MiB per partition. Reservations return to zero after results, operators, reexecutions and failures drop. |
| Catalog / metadata / projection / metrics | `bounded_log_and_kv_sql_against_native_sni`, `partitioned_logs_and_kv_discover_each_execution` and their helpers exercise catalog registration, source schema/projection, effective bucket counts, physical parallelism and EXPLAIN ANALYZE metrics. |
| Finite and continuous lifecycle | The live suite exercises empty starts, fresh stopping offsets per execution, three overlapping executions of one source plan, slow retained consumers, late appends, idle waits and cancellation. |
| Snapshot/schema/topology identity | The live suite checks KV snapshot isolation/evolution, recreated/changed table rejection and streaming topology invalidation, rather than silently switching identities. |
| Native DML and partial effects | The write SQL, authorization and pressure suites exercise INSERT/DELETE/MERGE, NULL/action ordering, routing, peer isolation, cancellation, ACK uncertainty and final stored data. See [native failure evidence](native-failure-verification.md). |
| Failure and recovery | The live suite exercises isolated coordinator failover and explicit fresh-query recovery. Memory rejection, table invalidation and socket-loss/restart use native errors and cleanup; remote/STS evidence is linked from production readiness. |

### Discovered host sort-policy limit

The first expanded live-suite run passed eight tests but its reference-operator
test failed with ResourcesExhausted: two default 10 MiB sort spill reservations
do not fit a 16 MiB pool. The host SessionConfig now sets
`execution.sort_spill_reservation_bytes = 1 MiB` for these tiny reference rows.
The provider does not modify runtime policy. This is a real cooperative pool
rejection, not a source decoder failure or evidence of a process RSS bound.
The targeted test then passed, followed by all nine live tests.

## Verified commands/results

Functional builds: DEBUG, eight compilation jobs. The changed DML session test
passed in **10.63s**; the complete live engine suite passed **9/9 in 81.55s**.
The failure/permission verification separately records eight write SQL tests,
pressure/recovery, authorization and native core/client/planner regressions.

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql insert_log_and_upsert_kv_from_sql -- --ignored --test-threads=1
KUBECONFIG=/tmp/opencode/native-sni.kubeconfig CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test live_log_sql -- --ignored --test-threads=1
```

The failover test restarts only the active coordinator in the isolated native-sni
namespace. Table fixtures use unique names and remove their own tables. Lab
credentials stay in the ignored environment file.

The [short final-route RELEASE read profile](native-profile-plan.md) also passed:
four concurrent log/KV SELECTs over a log dataset larger than the pool, slow
consumers, repeated waves/cancellation and explicit resource cleanup. It measured
128 scans in 120.3s, process VmHWM 118 MiB and reservation peak 32 MiB. This adds
measured runtime evidence; it does not replace sustained/write-route profiles.

Final-route profiles now pass: the 60s/300s four-case continuous writer matrix and
5+30-minute reader, with latency/resource/data/cleanup checks and direct allocation
controls. The final isolated-source example also executes native log/KV SQL with
separate host/runtime policy; see [source evidence](native-cleanup-audit.md).

`ydvk` awaits clean Git reproduction of the now-versioned series;
profile-specific guarantees stay scoped to the recorded setup. ACK/offered offsets
remain observations, not durable engine checkpoints or exactly-once recovery.
