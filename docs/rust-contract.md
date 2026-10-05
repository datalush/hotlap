# Fluss–DataFusion Rust contract

Decision record for `991c`, 2026-10-04. This is the canonical **target contract**
for the Rust-first cycle, not a declaration that all requirements are implemented.
Each pending requirement below names its implementation/acceptance task.
[The audit](rust-implementation-audit.md) records source evidence;
[reading semantics](reading-semantics.md) describes current implementation details.
FFI/Python is deferred until `rm21` accepts Rust.

## 1. API and responsibility decisions

Keep `FlussLogTable`, `FlussKvTable`, `FlussCatalog`, `LogReadOptions`,
`LogReadMode`, `LogStart`, `LogDelivery` and `FlussWriteOptions` as the existing
entry points. SQL uses DataFusion's `TableProvider`, `SessionContext`, streams
and `DataSinkExec`. Do not introduce another session, SQL executor, scheduler,
operator framework, memory manager or retry controller.

- Client Rust owns protocol/auth, metadata, routing, decoding/encoding, queues,
  requests, retries and ACKs. Correct Arrow batch routing belongs in the client.
- Connector Rust maps DataFusion contracts onto client operations, validates
  destination identity/schema, declares limits and exposes truthful observations.
- DataFusion owns exact SQL evaluation, logical/physical planning, distribution
  enforcement, operator execution and its memory pool. Native Rust composition
  must honor the caller's planning contract rather than compensate for FFI.
- Engine owns processed checkpoints, job recovery, conflict policies and
  reconciliation after uncertain writes. Source progress and ACKs are inputs to
  those decisions, not a substitute for them.

### Capability matrix

| Operation / property | Log provider | KV provider |
| --- | --- | --- |
| Finite read | Captured per-bucket retained offset range | Live rows from paginated per-bucket snapshot sessions |
| Continuous read | Explicit streaming mode | Unsupported: no KV changelog in this contract |
| Start position | Earliest retained / captured latest / complete explicit bucket offsets | Current per-bucket snapshot, no log offsets |
| INSERT | Append | Full-row upsert |
| Continuous INSERT input | Supported; confirm batches without waiting for EOF | Supported full-row upsert, same confirmation rule |
| DELETE | Unsupported | Exact selection over finite snapshot, then key delete if policy allows |
| MERGE | Unsupported | Finite source; ordered supported clauses and full-row upsert/key delete |
| INSERT OVERWRITE / REPLACE | Unsupported | Unsupported |
| Direct UPDATE / TRUNCATE | Outside cycle | Outside cycle; UPDATE clauses inside MERGE are included |
| Global snapshot / statement atomicity / conditional CAS | Not promised | Not promised |
| Exactly-once across job restart or lost ACK | Not promised | Not promised; repeatable full-row upsert does not make the whole job exactly-once |

Capabilities describe **connector support and observed table metadata**, not
permission to execute or assurance that metadata will remain unchanged. A table
policy forbidding/ignoring deletes makes deletion unavailable; authorization and
destination identity must still be checked when executing. A capability view
must not write data or silently open a different table to probe support.

`FlussLogTable::capabilities()` and `FlussKvTable::capabilities()` now expose
`FlussCapabilities`: selected `FlussReadCapability`, `FlussInsertCapability`,
`delete` and restricted `merge` support. The KV delete flag reflects policy at
provider opening. A new provider snapshots the client's observed metadata again,
not necessarily fresh server state; execution still validates metadata/policy.
MERGE support does not override a false delete flag for delete
clauses. This immutable view is not a permission grant, mutable registry or
planner extension.

### Existing usage (not new APIs)

```rust
use std::{sync::Arc, time::Duration};
use datafusion::prelude::SessionContext;
use fluss::metadata::TablePath;
use fluss_datafusion::{FlussLogTable, LogReadOptions, FlussWriteOptions};

// In an async function, with an existing Arc<FlussConnection> named connection:
let events = FlussLogTable::open_with_options(
    Arc::clone(&connection),
    TablePath::new("db", "events"),
    LogReadOptions::batch(Duration::from_secs(45)),
).await?.with_write_options(FlussWriteOptions::default())?;
let ctx = SessionContext::new();
ctx.register_table("events", Arc::new(events))?;
let rows = ctx.sql("SELECT COUNT(*) FROM events").await?.collect().await?;
```

`FlussLogTable::open` remains the finite convenience entry point.
`open_with_options(LogReadOptions::default())` explicitly chooses streaming;
use DataFusion stream consumption or a terminating plan, not an unconditional
finite `collect()`. KV registration uses `FlussKvTable::open` and the same native
`register_table` API. Provider mode/start and write options are separate from
client transport configuration and DataFusion SQL/memory configuration.

## 2. Read correctness, ordering and progress

### Selection and completion

- Log batch selects starts/stops per bucket at execution, not registration.
  Offsets are inclusive at start and exclusive at stop. Captures are not a
  transactionally simultaneous cross-bucket snapshot.
- KV snapshots open lazily per bucket; pages continue the same session. A failed
  session never silently restarts against a newer snapshot. No global KV snapshot
  is promised while concurrent writes occur.
- Every planned selected bucket must be assigned exactly once across source
  partitions, using its effective layout, including old partitions after rescale.
- SQL limits are global DataFusion operators. Batch pruning and partition
  pruning do not replace exact row filters; retain residual SQL evaluation.
- A read can offer batches before failing. Such batches do not make a complete
  successful query: no silent truncation on timeout, retention loss or metadata
  invalidation. Consumers of streams must retain the terminal success/error state.
- Logs preserve bucket offsets as the ordering reference. No order across buckets
  or partitions, or from unordered SQL, is promised. KV has no declared global
  row order. Clients requiring order use applicable DataFusion operators.
- Metadata changes may require replan/restart with valid positions; unsupported
  in-flight topology changes fail explicitly. Automatic discovery/recovery is not
  required. Identity includes table ID, not just its path/name.

### Progress: offered is not processed

The future source observation contract (`pceb`) extends the purpose of
`LogDelivery`; it must not silently reinterpret its existing batch events.

Required observations:

1. **Initialization:** execution identity, table/schema identity, selected
   partition/bucket layout and resolved start/stop (where finite), including
   empty ranges. Identity must exist even if no batch is offered.
2. **Offered batch:** bucket, inclusive base, exclusive next and row count.
3. **Excluded range / advancement:** progress through ranges conclusively
   excluded by the source predicate, distinguished from offered data.
4. **Terminal state:** completion, failure or cancellation, with completeness
   of the observation stream known to its consumer.

Rules:

- A resumable position cannot pass data for that bucket which is still buffered
  and has not been offered. Scanner fetch/consumed positions may lead delivery;
  they are diagnostics, not interchangeable resume positions.
- Exclusion is valid only for the same selection semantics. Resuming under a
  different filter/start/layout cannot assume skipped data was processed.
- Downstream SQL can discard an offered batch; source observation does not know
  whether the engine processed it or committed its sink/state.
- Observer overflow/lag means incomplete evidence. Never silently skip events
  and continue calling the result a complete resumable progress record.
- Observation is optional and bounded; loss must be detectable without making
  ordinary SQL depend on an engine checkpoint consumer.
- Independent concurrent executions use distinct native TaskContexts. Overlapping
  executions of the same plan with the same context are unsupported under the
  present identity mechanism; `pceb` must document/test that boundary, not infer
  identity from arbitrary pointer reuse. Sequential reexecution captures anew.

`LogDelivery` retains its offered-batch-only meaning. `subscribe_progress()` now
returns the bounded Tokio broadcast receiver for `LogProgress`: Initialized
(execution/table/schema, partition/count and `LogReadPosition` ranges), Offered,
Excluded and Terminated (`LogTermination`). Consume/handle `RecvError::Lagged` or
`TryRecvError::Lagged` as incomplete evidence; the source cannot attest to a
consumer that discarded events. Do not derive a complete resume map without all
partition initializations. Existing explicit-offset validation rejects missing,
stale or out-of-range bucket mappings; query/selection compatibility and processed
checkpoints remain the engine's responsibility. This is not a checkpoint service.

## 3. Memory and ownership

MemoryPool is admission/accounting for registered consumers, not an allocator
that automatically covers process RSS. Do not advertise `pool_limit` as a total
memory cap, or assume a `RecordBatch::clone` duplicates payload memory.

Both providers expose `with_max_retained_batch_bytes(bytes)` (positive, default
64 MiB). It rejects an admitted batch whose original Arrow backing capacities
exceed the ceiling, independently of available pool capacity. The native pool
remains the shared policy; no separate pool/counter allocator is introduced.
This is post-decode **retention admission**, not a promise to prevent decoder
allocation/expansion. The limit is shown in EXPLAIN, and the native gauge
`fluss_retained_source_buffer_bytes` follows buffer leases, including retained
outputs after stream completion/drop. It is conservative for shared/projected
backings and does not equal process RSS or the entire pool's reservations.

The engine/application selects budgets/concurrency and the MemoryPool. The
connector admits/retains against that pool and propagates native client settings;
the client/Arrow owns format parsing and limits during allocation. Pre-decode
inspection/codec hardening belongs there, not in a connector IPC parser, global
allocator, scheduler or engine-recovery loop.

| Resource | Owner and required contract | Current gap / task |
| --- | --- | --- |
| Decoded pending source batches | Client produces them; connector charges decoded bounded/streaming reader queues after polling, before output | Queue accounting implemented in `tq3s`; pre-decode/unfinished-poll transients and hard bounds remain `gpze`/`w8ap` |
| Offered source batches | Arrow buffer owners retain whole-batch backing-storage leases across clones/slices/projections until final buffer drop | Implemented in `tq3s`; retained batches can remain charged after stream completion/cancellation |
| Operator-retained state | Native DF operators use their real session pool where their implementation reserves it | Preserve native context/planning: `re7r`, `tq3s` |
| Sink input/gather Arrow batches | Log backing leases follow client-held buffers beyond enqueue/worker drop; KV worker guard covers row encoding; client routing scratch is admitted against the same pool | Implemented columnar ownership in `3etr`; saturation/actual byte verification: `yeqf` |
| Encoded writer queue, RPC frames, persistent routing | Client limiter plus real DF pool guards owned by batches/Bytes/frames/cache entries | Implemented `yeqf`; estimates and allocation peaks need sustained profiles `pc5n` |
| MERGE key set and encoding scratch | Selected PK representation/overhead in DF pool; row-format value encoding has its separate writer allowance | Selected-key scratch implemented `yeqf`; semantic coverage `9h56` |
| Remote files/download slots | Existing client disk-byte and concurrency permits, actual written bytes and cleanup | Retain separate budget: `w8ap` |
| Raw responses/decompression/temporary allocations | Explicit client limits/transient behavior; not magically included by the emitted-batch reservation | Measure/document applicable bounds: `gpze`, `w8ap` |

Required invariants:

- Queue budgets and per-execution budgets are finite/observable in the accepted
  profiles; oversize work must reject clearly rather than silently truncate.
- Unknown decoded size may require a transient allocation. State that boundary
  and measure it; no claim of pre-allocation protection from a later reservation.
- Sharing arrays across source/sink can lead to conservative overlapping charges.
  Explain them; do not equate accounting totals with unique allocated bytes or
  free a real retention guard solely to reduce the reported total.
- Budgets aggregate across concurrent executions: per-writer limits multiply.
  Do not implement a second global memory coordinator inside the connector.
- Drop/error/cancel releases owned queues/permits in bounded operation time;
  externally retained Arrow arrays remain valid and owned by their consumers.
- Data bytes, batch peaks, queued bytes, pool reservations and process RSS are
  distinct metrics. A peak-batch gauge cannot demonstrate bounded pending queues.

Arrow views/slices/projections should preserve storage where semantics allow.
Row-format decoding, type normalization, gathers, SQL operator results,
compression and wire encoding may allocate. Claims of zero-copy require buffer
identity/storage-range and lifetime evidence for the named boundary, not only
value equality or the name of an Arrow API (`gpze`, `3etr`, `pc5n`).

## 4. Deadlines, retries and cancellation

| Scope | Contract decision | Implementation status |
| --- | --- | --- |
| Batch source execution | A common source budget starts when its first physical partition executes; includes discovery/capture/open/poll/decode waits. Not a whole SQL query deadline | Shared partition deadline implemented in `tq3s`; further operation/failure coverage in `w8ap` |
| KV source execution | Same source-budget principle, including page waits; invalid snapshot fails rather than restarting | Shared deadline implemented in `tq3s`; snapshot/failure coverage remains `w8ap` |
| Streaming source | No completion timeout for ordinary idle input; network/storage/metadata operations have finite applicable waits | Preserve idle behavior; effective operation limits verified by `w8ap` |
| Write destination preparation/metadata | Shared finite connection/table/partition scope, independent bounded check between batches | `preparation_timeout` implemented `yeqf`, default30s; read catalog has its separate contract |
| Enqueue + ACK for one input batch | One deadline starts before validation/admission/key encoding; native retries/backpressure share it | Implemented `yeqf`, typed EnqueueAndAck cause, saturated log/KV cases |
| Waiting for the next streaming input batch | Normal input wait, not expiry of an outstanding batch ACK | Preserve: no unsent batch means no ACK clock to expire |
| Writer cleanup | Graceful bound on success, cooperative native abort/closure on drop; partial RPC frames have independent finite drain | AbortHandle/limiter races corrected `yeqf`; small-frame recovery observed≤3s, stalled frame bound30s; complete fault matrix `cf5y` |

Keep existing `FlussWriteOptions` names/defaults during refactoring:
`ack_timeout = 30s`, `max_retries = 3`. `ack_timeout` must be representable as
at least 1 ms because client buffer waits are millisecond-based; max 3600 s.
Validation now rejects positive sub-millisecond durations so client conversion
cannot silently produce a zero timeout. Retry budget is positive (zero currently unsupported)
and caps the existing client mechanism; do not define it as a new connector loop
or promise a fixed number of physical network sends without checking client usage.

Rust write options additionally expose `preparation_timeout=30s` (1ms..=3600s)
and positive `max_retained_batch_bytes=64 MiB`. The write ceiling applies after
materialization to all input providers, not only Fluss source output. Encoding,
framed transport, routing cache and reusable KV scratch consumers use the same
native session pool; reservations follow their respective owners, including idle
writers and cancelled frame drains. See [write-pressure-verification.md](write-pressure-verification.md)
for admission estimates, RPC/kernel cleanup boundaries and real log/KV fault cases.

Do not rename `batch_timeout` or call it a SQL-wide deadline. Public doc comments
reflect the shared source-execution deadline, not a query-wide limit.
Preparation options, if required after client-limit inventory, must be explicit
and small; no blanket deadline manager or unbounded inherited retries.

Cancellation stops accepting new input, signals blocking enqueue work, aborts
the dedicated writer and wakes buffer waiters. Cancelling an async JoinHandle
does not itself stop a running blocking closure. The input batch reservation
remains until that closure relinquishes its batch. No ACK, timeout, abort or
connection close can retroactively undo requests already applied by Fluss.

## 5. Writes, confirmations and partial knowledge

### SQL count

- `count` is the number of selected/submitted **operations acknowledged under
  the configured supported ACK policy**, not net rows inserted/changed/deleted.
- Log INSERT appends. KV INSERT is full-row upsert; two operations on the same
  key can count twice without producing two final rows.
- DELETE counts acknowledged deletes of selected snapshot keys, not proof that
  each row still existed when the request arrived. Table policy must allow it.
- MERGE counts modifying actions, not join rows or no-op clauses.
- Final count is produced after finite input EOF and successful completion.
  Continuous input confirms batches while running but has no final count before
  EOF. A terminal error is not replaced by a successful partial-count batch.
- Supported ACK modes are `all`/`-1` and `1`. ACK=1 is not the same replication
  guarantee as `all`; retain the policy in diagnostics. ACK=0 is rejected.

### Arrow batch implementation (`3etr`)

The log sink now uses the client's Arrow batch route for finite and continuous
input. Mixed partitions use effective layouts; contiguous groups share backings,
interleaved groups use Arrow take, and byte slices preserve client backpressure.
Input and cast/gather leases use the real session pool until final buffer release.
See [arrow-write-inventory.md](arrow-write-inventory.md) for named copy boundaries,
KV's required row-format encoding, admission estimates and verification.

### Independent operation knowledge (`bqrq`)

Use execution/batch identity and cumulative confirmed-operation counts. At least
distinguish these facts; exact public Rust names remain an implementation detail:

| Fact | What can be concluded |
| --- | --- |
| Not enqueued | This work was rejected before submission; earlier work in the statement is a separate fact |
| Submitted, outcome unknown | Some work may have applied; lost ACK/timeouts cannot establish which rows did |
| Acknowledged | Confirmation under the declared ACK policy is known for the reported operations |
| Input/execution ended | Separate terminal status: complete, failed or cancelled; includes whether observations are incomplete |

Errors and application knowledge are separate axes. Examples:

- Batch 1 ACKed, batch 2 loses ACK: batch 1 remains confirmed; batch 2 is unknown,
  not automatically failed-with-no-effect and not automatically confirmed.
- Enqueue errors after some rows of a batch: the whole batch cannot be called
  not-enqueued. Expose only the granularity the client actually establishes.
- All batches ACKed, cleanup errors: retain known confirmations; cleanup failure
  does not turn known writes into unknown writes or erase the error.
- Input errors after previous ACKs: fail the statement and preserve those facts.

Expose bounded optional observations/summary alongside normal DataFusion results,
reusing client result handles and standard async primitives. Do not retain every
row indefinitely or infer row-level outcomes from a single aggregate `flush()`
error. Detect observer lag and preserve known cumulative lower bounds; no generic
event framework, implicit statement retry, compensations or reconciliation loop.
Both providers now expose `subscribe_writes()` for bounded `FlussWriteProgress`
events and `FlussWriteSummary` terminal snapshots. Whole-batch native flush success
confirms the batch before EOF; a failed attempted batch remains conservatively
uncertain. Native DataSink metrics follow execution and resource-owner lifetimes.
Original count/error behavior remains intact. See
[write-observation-contract.md](write-observation-contract.md) for exact granularity,
lag/completeness rules, metric scope and the engine's reconciliation boundary.

### DELETE/MERGE boundaries

DELETE uses exact DataFusion filters over a finite KV snapshot and the existing
key writer. No WHERE selects all visible snapshot rows; no matches yields zero.
DataFusion 55.1 rejects predicates qualified by a DELETE target alias (for
example `DELETE FROM state AS s WHERE s.id = 2`) during SQL resolution. Use the
table-name qualifier or unqualified columns; the connector does not add a SQL
alias-rewriting workaround. MERGE target/source aliases remain supported.
Unqualified DELETE alias predicates work. The native core now backports upstream
empty-input/restriction protection: optimized FALSE/NULL returns zero; subquery
join plans and DELETE LIMIT reject before invoking the provider. Proven-empty
plans do not run a sink or emit Fluss write observations. See
[delete-contract.md](delete-contract.md) and [vendor/README.md](../vendor/README.md)
for provenance and verified selection/count/concurrency boundaries.
Reject `ignore`/`disable` and implicit ignore for merge-engine policies where
applicable. Concurrent changes may invalidate the user's intended selection;
there is no compare-and-delete/global isolation (`jwyv`).
The pinned server rejects in-place ALTER of `table.delete.behavior`; tests verify
that explicit failure rather than assuming supported live policy mutation.

MERGE uses DataFusion join/filter/CASE semantics and SQL null logic, with first
eligible clause precedence. Supported modifying actions are matched UPDATE/DELETE,
unmatched INSERT and admitted NOT MATCHED BY SOURCE actions. Source must be finite.
Configured native merge-engine tables are rejected for SQL MERGE because ACKed
first-row/versioned/aggregation upserts do not mean ordinary row replacement;
capabilities and planning/execution enforce that boundary.
INSERT supplies all destination columns; UPDATE cannot change PK/partition keys.
Repeated modifying actions for one PK are rejected, including across batches,
but discovery in a later batch cannot roll back earlier ACKed work. No atomic
statement, concurrent-writer conflict resolution or automatic repair (`9h56`).
The selected NOT MATCHED action uses native upsert, not conditional INSERT, and
snapshot-predicate UPDATE is not CAS. Later duplicate rejection preserves earlier
ACKs. See [merge-contract.md](merge-contract.md) for verified action/null/precedence,
resource, duplicate and writer-concurrency semantics.

## 6. Errors: preserve causes, separate policy

Continue using `datafusion::common::Result` and `DataFusionError`. Preserve
`fluss::error::Error` inside `External` and its source chain; callers can inspect
`Error::api_error()`/`is_retriable()` instead of parsing display strings.
`is_retriable()` describes client requests, **not safety of retrying a statement**.

| Condition | Required distinguishability / handling |
| --- | --- |
| Invalid configuration/input or unsupported operation | Native Plan/NotImplemented categories; reject before sending when known |
| Pool admission or client buffer exhaustion | ResourcesExhausted or preserved BufferExhausted cause; do not relabel as missing data |
| Authentication/authorization | Preserve typed API/transport/storage cause; no blind permissions retry |
| Retention loss / invalid requested position | Explicit position/retention cause with bucket context; never silently advance |
| Table recreation/schema/topology/snapshot invalidation | Identity/layout/snapshot context; replan/restart decision belongs to caller |
| Deadline | Identify preparation/scan/enqueue/ACK/cleanup scope, preserving known write effects |
| Permanent transport/storage/corruption failure | Original cause and operation context; fail explicitly after applicable client attempts |
| Cancellation | Distinguish observed cancellation/abort from successful completion; external Drop itself need not return an error |
| Uncertain write | Application knowledge attached to observed error, not guessed from its text/category |

Source deadline expiry now carries `FlussScanTimeout` with read semantics inside
`DataFusionError::External`, without parsing its display string. Connector-generated
source identity/schema/topology/retention checks now use `FlussReadInvalidated`
with typed reasons; streaming operation expiry uses `FlussOperationTimeout`.
Some shared-capture/validation failures still have native diagnostic strings;
`yeqf`/`bqrq` must make write-specific decisions inspectable where callers
need machine decisions. A small connector-specific error payload implementing
`std::error::Error` can be boxed in the existing DataFusion error; do not wrap
every client variant in another hierarchy or introduce an automatic retry policy.
Not all source-chain errors can be exhaustively classified; preserve unknown
causes rather than guessing. Error/Debug/metrics contexts must exclude secrets.

## 7. Acceptance scenarios and ownership

| Scenario | Required evidence | Task |
| --- | --- | --- |
| Register log vs KV, unsupported DML and delete policy | Honest capabilities; rejected work not sent; permissions checked separately | `re7r`, `jwyv`, `cf5y` |
| Native custom planner/optimizer, multipartition input | Compose without FFI-driven bypass; consume all intended rows | `re7r` |
| Retain several source batches and pull more | Valid buffers, explicit accounting/queue limits; distinguish source charge vs actual retained bytes | `tq3s`, `gpze`, `w8ap` |
| Wide projected KV and compatible log batch write | Correct schema/nulls, measured materializations and buffers | `gpze`, `3etr` |
| Mixed partitions with old/new counts | Correct routing for every row; batch grouping uses effective partition layout | `3etr` |
| Empty/all-pruned source and late-starting partitions | Complete initial identity/assignment and progress without skipping queued data | `pceb` |
| Reexecution/concurrency | Fresh captures; distinct contexts isolated; unsupported overlap documented | `pceb`, `tq3s` |
| Late partition, metadata wait, idle stream | Common source budget where required; idle does not expire streaming completion | `w8ap`, `yeqf` |
| Saturated buffer, ACK wait and cancellation | Enqueue wakes, workers release retained batches/permits in bounded time; no rollback claim | `yeqf`, `cf5y` |
| ACK lost/input failed/cleanup failed after ACK | Known counts preserved, unknown outcomes not invented, SQL still fails | `bqrq`, `cf5y` |
| DELETE concurrent row changes | Count means acknowledged operations, not conditional deletion/net change | `jwyv` |
| MERGE duplicate in later batch, wide non-key payload | Earlier effects retained, duplicate detected, appropriate key/scratch budget | `9h56`, `yeqf` |
| Writer_acks=1 vs all and retry replay | Declared guarantees distinct; log duplicates possible; no exactly-once inference | `bqrq`, `cf5y` |

Functional tests use DEBUG/8 jobs; profiles/benchmarks/delivery use RELEASE/8 jobs.
Reuse historical evidence where the path is unchanged and repeat affected tests.
This decision record requires no new wheels or performance runs. `rm21` accepts
the Rust implementation of these requirements; `cc71` checks Python equivalence
later and cannot redefine or block native Rust contracts.

Outside this cycle: persistent jobs/checkpoints, global recovery, business
conflict policies, multi-bucket transactions, KV changelog/snapshot-changelog,
automatic Python catalog, direct UPDATE/TRUNCATE. No claims of total RSS bounds,
global ordering, statement rollback or whole-job exactly-once.
