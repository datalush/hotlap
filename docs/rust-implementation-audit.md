# Rust implementation audit — h3e4

Date: 2026-10-04. Scope: first architectural audit of the current working tree,
not production acceptance or an implementation refactor.

Baseline: `d06288a` (MERGE), `f66e90c` (INSERT/DELETE), `89e111b`
(batch/streaming), with uncommitted FFI-related changes on top. The working-tree
changes are part of the inventory, not assumed to be approved architecture.
No implementation changes, new bindings, live fault injection or benchmarks
were performed for this audit. Test evidence below is historical unless stated
otherwise; inspected tests are not equivalent to newly executed tests.

References use repository-relative paths and symbols; line ranges describe this
working tree and will move during refactoring. Upstream API observations refer
to the locally resolved DataFusion 55.1.0 source, not an assumed future release.

## 1. Responsibility boundaries

- **Client Rust** owns protocol/auth/metadata, table/partition/bucket routing,
  scanning, schema decoding, encoding, writer queues, ACKs and client retries.
- **Connector Rust** owns DataFusion table contracts, execution adapters,
  conservative pushdown translation, capabilities, per-execution progress,
  resource integration and the actual semantics of supported Fluss writes.
- **DataFusion** owns SQL parsing/typing, exact expression evaluation, planning,
  optimization, operator execution, distribution requirements and its pool.
- **Bindings** expose these contracts; they must not dictate core Rust planning
  or reimplement SQL, routing, retries or a memory manager.
- **Engine** owns processed checkpoints, persistent jobs, reconciliation and
  business conflict policies. Offered offsets and ACKs are not engine commits.

### Connector inventory (15 source modules at the audit snapshot)

All paths in this table are under `crates/fluss-datafusion/src/`.

| Module | Owner/responsibility | Disposition | Follow-up |
| --- | --- | --- | --- |
| `lib.rs` | Connector public exports | Keep thin; its bounded-sources-only introduction is stale after streaming/writes | `991c`, `re7r` |
| `catalog.rs` | Connector snapshot catalog adapter | Keep optional; discovery policy/timeouts are not an engine catalog | `991c`, `tq3s` |
| `log_table.rs` | Connector log provider, schema/pushdown/parallelism contracts | Keep; use native planning and exact filters outside source | `re7r`, `gpze` |
| `kv_table.rs` | Connector snapshot provider and DML entry points | Keep responsibilities; review direct default planner in DELETE | `re7r`, `jwyv` |
| `log_options.rs` | Connector read mode/start/deadline options | Keep separate from client transport config; clarify deadline scope | `991c` |
| `log_progress.rs` | Connector offered batch progress | Extend contract, not into a checkpoint store | `pceb` |
| `scan.rs` | Connector log partition streams and per-execution validation/progress | Keep adapter; refactor retained-batch accounting and lifecycle | `tq3s`, `gpze`, `w8ap` |
| `kv_scan.rs` | Connector bucket snapshot streams and page metrics | Keep; assess decode/projection and reservation scope | `gpze`, `w8ap` |
| `execution.rs` | Connector scan plan display/metrics wrapper around native leaf source | Keep unless equivalent native observability is demonstrated; not an FFI graph-hiding wrapper | `re7r` |
| `filter.rs` | Connector conservative DF-expression → Fluss pruning translation | Keep; exact SQL residual evaluation is necessary, not duplicated row filtering | `gpze`, `991c` |
| `partitions.rs` | Connector per-execution partition discovery/pruning/budgets | Keep; routing and protocol metadata stay in client | `pceb`, `tq3s` |
| `offsets.rs` | Connector shared captures by execution context/generation | Keep purpose; document/test generation assumptions | `pceb`, `tq3s` |
| `metrics.rs` | Connector metrics and stream-lifetime guards | Keep native metrics integration; distinguish batch peak from total retained memory | `w8ap`, `bqrq` |
| `merge.rs` | Connector supported MERGE semantics built from DF operators | Keep semantic composition; review planner bypass and source adapter | `re7r`, `9h56` |
| `write.rs` | Connector sink, destination validation, writer execution/results | Refactor cohesive boundaries; client owns batch routing/encoding | `3etr`, `yeqf`, `bqrq` |

### Client and binding inventory

Client paths are under `clients/rust/crates/fluss/src/`.

| Area | Owner/responsibility | Disposition / evidence |
| --- | --- | --- |
| `client/connection.rs`, client metadata | Client connections, shared metadata and write-client lifetime | Keep. Dedicated write connection currently isolates cancellation/flush; reducing connection overhead must preserve this isolation |
| `client/table/scanner/{api,builder,runtime,subscriptions,status,requests,responses,fetch,polling,batches,records,poll_timing}.rs` | Client subscription/fetch/polling and batch production | Keep protocol/state machinery; audit decoded poll limits and queued data, not another scanner in connector |
| `client/table/log_fetch_buffer.rs`, `reader.rs` | Client raw/completed fetches, bounded range clipping and buffered batches | Keep range handling and cleanup; decoded queues require explicit resource accounting |
| `client/table/remote_log.rs` | Client remote download concurrency, disk-byte permits, cleanup/retries | Keep actual byte/slot permits; do not confuse disk budget with Arrow RAM budget |
| `client/table/kv_scanner.rs`, `batch_scanner.rs`, `read_context_resolver.rs` | Client per-bucket snapshot pages and schema-aware decoding | Keep protocol correctness. Evaluate unnecessary full-row decoding/projection materialization separately |
| `client/table/{append,upsert,partition_getter}.rs` | Client row/batch write APIs and physical partition selection | Improve Arrow batch grouping here, before using it in sink |
| `client/write/{writer_client,bucket_assigner,accumulator,batch,sender,broadcast}.rs` | Client authoritative routing, queue admission, encoding, retries, ACK/error delivery | Keep single implementation; sink consumes ACK semantics instead of creating a retry/transport stack |
| `record/arrow.rs`, record KV decoding/encoding and column writers | Client wire/Arrow conversion, schema normalization | Retain format-required materialization; eliminate avoidable Arrow → row → Arrow reconstruction for compatible log batches |
| `crates/fluss-datafusion-python/src/{lib,extension}.rs`, Python helpers | Binding export/config and experimental session extension | Defer redesign to `n91g`/`dz3m`; no requirement for Rust acceptance |
| `crates/datafusion-ffi-ext/src/{memory,provider,runtime,registry}.rs`, `tools/datafusion-python-ext.patch`, `tools/build-ext-host.sh` | Experimental foreign-resource and opaque-plan adapters | Inventory as transitory; not a second core architecture. Resolve in FFI phase and remove replaced pieces in `prad`/`7yt8` |

This groups client subsystems rather than claiming a complete review of every
RPC or format implementation. The critical routing/queue/decoding paths below
were inspected directly; deeper allocation measurements belong to later tasks.

## 2. Data and resource flows

### Log reads

```text
remote file/RPC → completed raw fetch → client Arrow decode
  → collect_batches Vec<ScanBatch> → bounded-reader or streaming VecDeque
  → connector prepare_output_batch → projection/view → DataFusion operators
```

- `scanner/batches.rs:24–59` produces up to 100 batches per poll with a **soft**
  64 MiB cap. It accounts raw/decoded size after decoding the fetch; the last
  fetch may exceed the cap. This is not a pre-allocation memory-pool permit.
- `reader.rs:493–546` buffers decoded batches from a poll. Streaming similarly
  queues them in `scan.rs::ActiveReader::Streaming` (`scan.rs:130–151`).
- `scan.rs:273–286` charges only the batch selected for output after decoding.
  Native projection clones references to columns rather than rebuilding data.
- `scan.rs:203–207` frees that reservation on the next pull. The comment assumes
  a previous batch was consumed; a downstream collector/operator can retain
  it while asking for more. This reservation is **not a lifetime-bound account
  of all output buffers**. Operators may have separate reservations, but that
  does not make this assumption generally true.
- Remote-file permits in `remote_log.rs::PrefetchBytesPermit` and download
  admission (`reserve_bytes`, actual-size adjustment before writing chunks)
  bound a separate disk/concurrency resource. Preserve that distinction.

### KV reads

```text
server per-bucket snapshot page → compact KV records → schema decoder
  → RowAppendRecordBatchBuilder → full Arrow batch → projection
  → connector reservation → DataFusion operators
```

`batch_scanner.rs::value_records_to_record_batch` (368–392) decodes records and
builds Arrow columns row by row, then projects. That row-format conversion is
not zero-copy transport of existing Arrow columns. There may be work to avoid
building unused fields, but schema-evolution/null semantics must remain correct.
`kv_scanner.rs` requests approximately 1 MiB pages; that is not a strict bound on
decoded Arrow memory. `kv_scan.rs:129–181` frees the previous reservation on pull
and reserves after the page is decoded, with the same retention caveat as logs.

### Writes

```text
DataFusion input batch → connector validation/reservation
  → spawn_blocking → ColumnarRow view per row → client routing
  → accumulator + memory permit → row/Arrow encoding → sender/retries
  → flush/ACK → confirmed count (final only at EOF)
```

- `write.rs::FlussWriter::enqueue` uses one reusable typed row view; its
  `RecordBatch::clone` and MERGE action projection do not copy payload buffers.
- For log row writes, `record/arrow.rs::RowAppendRecordBatchBuilder::append`
  (256–261) writes values into column writers and builds new Arrow arrays.
  Thus the current log path does perform **Arrow → row views → new Arrow
  columns**, before format encoding. It is not zero-copy merely because the
  input row views reference the original batch.
- `PrebuiltRecordBatchBuilder` (128–184) retains compatible batch arrays and
  avoids that reconstruction. `prepare_append_record_batch` (860–917) clones
  compatible columns but casts supported differing encodings; casts may allocate.
- Even the prebuilt path encodes wire bytes: `ArrowLogWriteBatch::build`
  (`client/write/batch.rs:315–321`) materializes/caches `Bytes`. Compression and
  wire encoding are distinct from avoidable reconstruction of Arrow columns.
- The sink moves its input reservation into the blocking worker
  (`write.rs:408–424`), an intentional guard while that worker retains the batch.
  Encoded client queues have a separate `MemoryLimiter`, not the same pool.
  Its permits are associated with incomplete batches until completion/abort.
  Accounting shared input arrays twice can be conservative admission, not
  evidence of twice the physical allocation.

## 3. Findings and required decisions

**Confirmed** means the source demonstrates the behavior. **To verify** means
the architectural risk needs a targeted scenario before assigning a correctness
bug or deleting code. Every finding has an existing task; no duplicate issues.

| ID | Finding / classification | Required decision and owner |
| --- | --- | --- |
| A01 | **Confirmed:** direct `DefaultPhysicalPlanner` in DELETE/MERGE was added for FFI (`kv_table.rs:275–277`, `merge.rs:281–290`, uncommitted diff) | `re7r`: recover a composable Rust planning path without blindly restoring calls that could recurse. Test custom planner, aliases/UDFs and all input partitions |
| A02 | **Confirmed:** sink coalescing was added for an opaque FFI graph (`write.rs:115–124`) | `re7r`: native `DataSinkExec` already requires `SinglePartition`; let native distribution enforcement own it where possible. Test multipartition writes before removing explicit coalesce |
| A03 | **Confirmed:** compatible log inputs are reconstructed by row builders on current sink path | `3etr`: move batch optimization into client and use prebuilt path where valid. No promise to eliminate required wire encoding |
| A04 | **Confirmed invariant mismatch; consequence to verify:** `append_arrow_batch` groups by table bucket count (`append.rs:199–231`), while `WriterClient::assign_bucket` uses live per-partition routing count (`writer_client.rs:175–215`) | `3etr`: grouping must use the same effective layout as send. Different counts can cause representative-key grouping to combine rows that route differently. Current sink uses row routing; do not switch it before testing old/new layouts |
| A05 | **Confirmed:** batch append assumes first-row partition and uses `take_rows` for multiple bucket groups | `3etr`: validate homogeneous partition input or support correct mixed grouping in client; contiguous slices vs justified gather. Do not reproduce routing in bindings |
| A06 | **Confirmed:** queued decoded log batches are not covered by the output-batch reservation | `tq3s`, `gpze`, `w8ap`: inventory raw/decoded/pending/output ownership and limits; reserve/admit appropriate retained resources, not only the last emitted batch |
| A07 | **Confirmed:** next-pull reservation release is not equivalent to final buffer release | `tq3s`, `w8ap`: define what the source accounts and downstream must account; test retained batches, collectors and cancellation. Do not claim a total RSS cap |
| A08 | **Confirmed:** KV reconstructs full Arrow rows before projection | `gpze`: evaluate projected decoding, especially wide unselected values; preserve schema evolution and required conversion |
| A09 | **Confirmed:** ACK deadline starts after connection/table/metadata checks and MERGE key work (`write.rs:257–408`) | `991c`, `yeqf`: specify preparation/network/enqueue/ACK scopes and enforce missing limits using existing client mechanisms. Idle input need not be a write failure |
| A10 | **Confirmed:** MERGE key scratch uses twice full batch bytes + per-row allowance; encoded keys are copied into persistent set (`write.rs:380–405`) | `yeqf`, `9h56`: budget selected key representation/overhead accurately enough; retained duplicate-key state is legitimate, but scratch can reject large non-key payloads unnecessarily |
| A11 | **Confirmed:** progress emits only offered batches; gaps from pruning explicitly allowed (`log_progress.rs`, `scan.rs:289–305`) | `pceb`: initial/empty/pruned progress and execution identity must be observable without a first output batch; not an engine checkpoint |
| A12 | **Confirmed:** captures use TaskContext pointer + repeated-partition generations (`offsets.rs:67–85`); overlapping reuse invariant not established here | `pceb`, `tq3s`: document supported execution identity assumptions and test overlap with same context, not only new contexts/sequential reuse |
| A13 | **Confirmed:** sink returns final count; failures after confirmed batches do not return a structured partial result (`write.rs:351–435`) | `991c`, `bqrq`: retain SQL count contract and expose truthful streaming confirmations/partial knowledge without claiming rollback |
| A14 | **Confirmed:** dedicated write connection + synchronous abort isolates operations; aborting a sender is not rollback nor joined completion (`write.rs:138–164`, `writer_client.rs:263–280`) | `tq3s`, `yeqf`, `cf5y`: preserve isolation and verify bounded cleanup under saturated buffers/ACK loss. Do not pool writers before this is established |
| A15 | **Confirmed:** snapshot catalog times each names RPC, but provider lookup opens table without the same wrapper (`catalog.rs:25–51,102–120`) | `991c`, `tq3s`: clarify per-operation vs total discovery timeout and existing client RPC deadlines; no automatic global catalog service required |

### Precise planning observation (A01/A02)

DataFusion 55.1.0 `SessionState::create_physical_plan` first runs logical
optimization and invokes the session's `QueryPlanner`
(`datafusion/src/execution/session_state.rs:777–784`). Direct
`DefaultPhysicalPlanner::create_physical_plan` creates and physically optimizes
its initial graph (`datafusion/src/physical_planner.rs:154–171`), using the
session's physical optimizers (`optimize_physical_plan`).

Therefore the change does **not disable every physical optimizer**. It bypasses
the session planner and, for SessionState, that entry point's logical optimization.
This distinction matters for projection pruning and custom extension planning.

`DataSinkExec` declares `Distribution::SinglePartition` and executes input
partition zero (`datafusion-datasource/src/sink.rs:277–287,339–354`). Removing
manual coalescing is safe only when the enclosing native planning path actually
enforces that requirement, not when executing an unoptimized graph directly.

Exact residual SQL filters are intentional: `filter.rs` prunes **whole batches**
conservatively; `log_table.rs::scan` leaves exact filtering/global limits to
DataFusion. Removing the residual to avoid perceived duplication would change
results. Partition pruning is similarly a discovery reduction, not row filtering.

## 4. Transitory-code register

| ID | Piece | Target disposition / acceptance | Task |
| --- | --- | --- | --- |
| T01 | FFI-motivated default planner substitutions in Rust DELETE/MERGE | Replace/justify within native composition; no blind rollback | `re7r` |
| T02 | FFI-motivated explicit sink coalesce | Remove when native enforcement is verified, or retain only with independent Rust need | `re7r` |
| T03 | Binding `FlussPlanner` / required `FlussExtension` for DML | Prefer official table FFI; remove when it covers accepted contracts | `n91g`, `dz3m`, `prad` |
| T04 | `OpaqueQueryPlanner`, opaque registry/tokens/codecs | Remove substituted transport; not a durable/distributed plan mechanism | `dz3m`, `prad` |
| T05 | `session_with_runtime`, `ResourceProvider`, `RuntimePlan` in generic FFI crate | Replace by proper resource contract; do not force Rust to depend on session reconstruction | `n91g`, `dz3m`, `prad` |
| T06 | Generic memory capsule/adapter | Candidate for upstream contract, not automatic deletion: consumer/reservation identity and resources must survive correctly | `n91g`, `dz3m`, `prad` |
| T07 | Host patch, build script, markers/version constraints/helper exports | Remove parts substituted by official support; keep only an explicit justified delta if still required | `7yt8` |
| T08 | Current row-loop append route | Replace where safe batch path supersedes it; retain row APIs for real supported use, not duplicate sink fallback by inertia | `3etr`, `prad` |
| T09 | `merge.rs::InputPlan` physical-source → provider adapter | Not automatically obsolete; MERGE receives a physical source but builds DF logical composition. Keep unless a simpler native mechanism preserves semantics/composition | `re7r`, `9h56` |
| T10 | Historical docs/old API descriptions (including bounded-only public introduction) | Update canonical active docs; preserve verified history in Kata rather than executable compatibility branches | `991c`, `7yt8` |

The scan metrics wrapper, partition/offset validation, writer ACK handling,
schema conversion and conservative pruning are **not** classified as slop by
their being connector-specific. Small repeated error-mapping functions do not
justify a generic framework by themselves. Split `write.rs` by coherent concerns
only after public contracts are defined, not into layers forwarding every call.

## 5. Evidence map and next verification

| Evidence | Existing scope | Remaining targeted checks |
| --- | --- | --- |
| `npcv`, `tests/live_log_sql.rs` | Projection, pruning vs exact filters, schema evolution, snapshots, reexecution/concurrency, recreated tables | Custom planner/native planning path; lifetime of multiple retained batches; projection after planner cleanup |
| `knx0`, `tests/remote_retention.rs` | Historical sustained RAM/disk/concurrency profiles and memory pressure | Decoded poll queues, admission before decode, retained batches and changed batch-write path |
| `a7fy`, `7306be7`, `tests/remote_retention.rs` | Real S3 errors, retention and real STS expiry/renewal | Repeat only changed failure/lifecycle paths; no new 900s expiry run for a static audit |
| `sz09`, `89e111b`, `tests/live_log_sql.rs` | Late appends/idle/cancel, explicit topology failures and streaming sources | Empty/pruned initial progress, same-context overlapping executions, pending Arrow queues |
| `vacq`, `f66e90c`, `tests/write_sql.rs` | Log/KV INSERT, DELETE, 4 MiB input under 2 MiB writer budget, concurrent/continuous writes, mixed layouts | Direct batch append with mixed partitions/old-new counts, blocked ACK/enqueue cancellation, preparation deadline and structured partial results |
| `vacq`, `d06288a`, `tests/write_sql.rs` | Ordered MERGE actions, null logic, duplicates, composite PK/rescale | Wide non-key payload scratch budgets, later-batch duplicates/partial knowledge, concurrent writers/custom sources |
| Client tests in `record/arrow.rs`, `accumulator.rs`, scanner tests | Schema normalization/null checks, limiter close/wakeup, reader/scanner boundaries | Buffer identities through compatible prebuilt path, correct effective-layout grouping, actual retained bytes vs permits |
| FFI dev4 historical tests | Some real cross-library buffer/lifetime and pool rejection cases | Deferred to `cc71`; not proof of native decode, routing or full-runtime parity |

Many integration cases are ignored by default and require the isolated Fluss or
Docker/RustFS fixtures. Reading their source and historical evidence is not a
fresh pass. New tests should target integration contracts, not mirror DataFusion
SQL/operator implementations or mechanically assert a refactoring layout.

## 6. Handoff and acceptance of this audit

The next executable task is `991c` (public Rust contracts), then `re7r` and
`tq3s`. Findings A01–A15 and dispositions T01–T10 are the input to those tasks
and the later Arrow/resource/DML tasks. Rust acceptance `rm21` resolves native
transitories before FFI starts; `prad`/`7yt8` audit the full-cycle removal later.

For h3e4: the inventory, source-grounded findings, ownership/disposition matrix
and evidence/gap map are delivered in this document. Architectural risks marked
“to verify” remain follow-up work, not claimed reproduced bugs. No audit finding
authorizes deleting unknown working-tree changes or creating a new SQL/runtime
framework. Commit/versioning and Kata close must follow explicit authorization
and the project's evidence requirements.

## 7. Resolution in re7r (2026-10-04, working tree)

- A01/T01: DELETE and MERGE SELECT helper graphs now go through the supplied
  session's `create_physical_plan`, restoring its planner/logical optimization.
  A recording custom planner observes those internal graphs in the real SQL test.
- A02/T02: removed FFI-motivated manual coalescing. A three-partition MemTable
  INSERT verifies native distribution enforcement, all rows and repeated
  execution of the same physical sink plan. Other write tests cover continuous
  input, backpressure and old/new partition layouts.
- Removed the duplicate target/sink field container: `FlussWriteTarget` is the
  sink itself; `plan_write` names its shared INSERT/DELETE/MERGE construction.
- Added `capabilities.rs` (a sixteenth module) with a small immutable provider
  view, derived from selected mode/metadata. No capability registry or planner.
- T09 retained: `InputPlan` adapts a supplied physical MERGE source into the
  logical operator graph; the multipartition MemTable MERGE verifies this native
  purpose. `FlussScanExec` retains its distinct metrics/display responsibility.
- Native DELETE UDF/table qualifiers and MERGE aliases pass. DELETE target alias
  qualification is rejected by DataFusion 55.1 before provider planning; the
  regression records this limitation instead of creating a SQL workaround.

Verification: 15 Rust unit tests, all four ignored `write_sql` integrations on
isolated native-sni fixtures, and `cargo clippy -p fluss-datafusion --all-targets
--all-features --locked -- -D warnings`. Functional builds use DEBUG/8 jobs.
These results concern Rust; historical dev4 FFI results do not establish binding
parity after this cleanup. Resource findings A06–A15 retain their own follow-ups.

## 8. Resource foundation in tq3s (2026-10-04, working tree)

- A06: decoded reader queues are now charged after a completed poll via the
  native source reservation. The client exposes `buffered_arrow_bytes()` without
  depending on DataFusion. Pre-decode/unfinished-poll/raw buffers remain a separate
  transient/client-budget concern for `gpze`/`w8ap`; no strict RSS claim.
- A07: `resources.rs` attaches leases to Arrow buffers using its custom-allocation
  owner, retaining the original immutable buffer and its reservation. No payload
  copy or alternative Array type is introduced. Projection/slicing/consumer
  retention keep leases even when source state/context is dropped; independent
  batches release independently. Whole-batch charges remain conservative after
  projection. Array/header owners allocate metadata, and visible custom buffer
  capacity need not equal original backing capacity charged by the lease.
- Batch log and KV deadlines now share execution-context/partition generations
  with the existing capture mechanism; late partitions cannot restart the budget.
  A12's same-context overlapping execution limitation remains explicit, not fixed
  by a second scheduler or identity registry.
- Source deadline errors expose `FlussScanTimeout` through DataFusion's External
  variant. Other local classifications and blocked writer cleanup remain their
  scoped follow-ups; the original native TaskContext/pool and writer isolation
  stay intact, with no reconstructed session or global memory manager.

Evidence: 18 Rust unit tests (buffer-pointer preservation, nested/null/slice
lifetimes, failed admission and deadline generations included), real bounded
log/KV integration with retained-batch leases/concurrency and late-partition
timeout causes, streaming append/cancel integration, four write_sql integrations
and six existing client MemoryLimiter tests. DEBUG/8 jobs; core clippy all-targets/
all-features passed. Stressing actual blocked ACKs/queues and complete allocation
profiles is still `yeqf`/`w8ap`/`cf5y`, not established by these foundation tests.

Pre-commit review additionally rejects clock-unrepresentable scan deadlines as
errors instead of panicking, and rejects ACK budgets below one millisecond before
client conversion. Both have regression tests; final reviewed unit count is 20.

## 9. Reading Arrow in gpze (working tree)

A08 now uses selective compacted field traversal and selected-only Arrow builders
in the client. Projection, duplicate references, old/new field-ID alignment and
row-only KV COUNT are supported without the connector's full-row/count workaround.
Preview limits select record ranges before materialization. See
[arrow-read-inventory.md](arrow-read-inventory.md) for before/after boundaries,
semantic changes in validation of unrequested values, concrete buffer evidence
and retained log/format materializations. This does not eliminate raw/decode peaks
or replace pending resource stress/throughput profiles.

## 10. Source progress in pceb (working tree)

A11 now has separate bounded `LogProgress` observations without changing legacy
`LogDelivery`: initial ranges for every executing physical partition (empty
buckets included), offered batches, excluded ranges and terminal status. Identity
is assigned before the first output, shared across partitions; observations do
not contain error payloads/credentials or engine checkpoint state.

Client read-only metadata reports fetch offsets, queued batch bases and remaining
stopping bounds. The connector clamps resumable source advancement to the first
unoffered buffered batch and finite stop, including buckets unsubscribed after
fetch completion. This covers pruned trailing ranges without skipping queued data.
Loss is exposed by standard broadcast Lagged errors; consumers must reject missing
initializations/incomplete evidence, and explicit offset maps retain validation.
A12's overlapping same-context execution limitation remains documented.

Live regressions verify all-pruned multibucket advancement/completion, same-filter
resume after a new append and rejection of incomplete offsets; all empty buckets
initialize without batches; idle streaming initializes and cancellation terminates.
Existing reexecution, concurrent contexts and topology/error cases remain native
DataFusion/client contracts, not a new scheduler or observer-driven retry loop.
