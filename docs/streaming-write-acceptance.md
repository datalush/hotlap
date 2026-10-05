# Native continuous INSERT acceptance — bsjm

Scope is the same Rust DataFusion sink used by finite INSERT, with unbounded
input toward log append and full-row KV upsert. RecordBatch is its processing
unit, not a finite-query requirement. DELETE uses finite KV selection and MERGE
requires finite input. Engine checkpoints/job retry/reconciliation remain external.

## Real Fluss source to log/KV destinations

`tests/write_sql.rs::streaming_source_routes_log_kv_confirms_sparse_batches_and_replays_explicitly`
creates an isolated one-bucket log source and two partitioned destinations:
`north` retains **2 buckets**, `west` is created with **3 after rescale**. It runs
two concurrent `INSERT ... SELECT ... FROM feed WHERE id % 2 = 0` statements using
the native Fluss streaming provider and supplied DataFusion pool.

- A single 24-row Arrow append interleaves destination partitions and bucket keys.
  Each sink confirms 12 filtered operations before input EOF, with distinct write
  execution IDs. Stored rows total 12, six per north/west for both destinations.
- Idle lasts **1.3s**, longer than the configured **1s ACK allowance**, while both
  statements stay live and emit no final SQL count.
- A sparse singleton yields cumulative ACK13 for both sinks without filling an
  ideal client batch. Cancelling the log INSERT during input wait reports
  confirmed13/uncertain0 and closes that private writer only.
- A further singleton is delivered to the KV peer, which stays live and confirms14.
  The log destination stays at13. KV idle cancellation reports confirmed14/uncertain0.
- All owned source/sink/client reservations return to zero within the established
  bounded observation after cancellation.

## Explicit replay is not exactly-once

Fresh executions start explicitly from source earliest, with new IDs. Both sinks
confirm **14 operations**. The append log now has **27 rows** (13 old plus14 replay),
while the KV destination still has **14 keys**. Upsert replay's operation count is
not a net-key count or a guarantee of business-level deduplication. Both replay
statements still require explicit cancellation because their input is continuous.

For an explicit-offset restart the engine must provide complete valid bucket
positions and retain its own processed/checkpoint knowledge. Offered read progress,
ACK observations and process-local write IDs are not coordinated engine commits.
Replaying uncertain requests may duplicate append rows or overwrite PK values;
the connector does not retry the statement or reconcile automatically.

## Joint acceptance matrix and evidence reuse

| Requirement | Verified evidence on the final Rust route |
| --- | --- |
| Real continuous source, multi-partition/bucket old/new routing | New native-sni source→log/KV test above; finite routing counterpart compares native row API bucket membership/order/nulls |
| Small/sparse confirmation and visibility before EOF | Native source test, original continuous log SQL test and both-target Docker feed matrix |
| Idle, no fabricated completion/count | Real 1.3s>ACK1s idle; Docker log/KV idle>ACK2s; terminal/EOF summary tested independently |
| Faster producer/backpressure | Final Docker matrix proves exhausted64KiB client buffer/32KiB target with1MiB input under paused server, then cooperative producer cancellation and pool recovery; finite4MiB/2MiB regression remains passing |
| Source/sink/client buffer ownership | Existing read `w8ap` pressure/retained-consumer tests and write `yeqf`/`bqrq` owner gauges/actual pool admission remain valid; one-batch source pulls and Arrow leases are unchanged |
| Failure after confirmations | Both log/KV Docker source-error-after-ACK retains confirmed1; later blocked ACK yields failed SQL, confirmed1/uncertain1, original cause retained |
| Cancel waiting input | Real source log/KV and explicit replay executions stop without erasing ACKs |
| Cancel with buffer full | Both-target Docker matrix, confirmed1/uncertain256; source/worker/frame/routing guards release in observed bound |
| Cancel waiting ACK | New explicit both-target singleton case proves terminal stage `Ack`, cancelled status, confirmed1/uncertain1 and pool release, separate from timeout and buffer admission |
| Independent executions | Real same-source/same-context distinct write IDs; log cancellation does not stop KV delivery; Docker private peer survives cancellation during a paused destination |
| Replay/duplicates/uncertainty | Explicit earliest replay checks log27/KV14 with14 ACKed operations per new execution; documented upper-bound batch uncertainty and no rollback/automatic restart |

The Docker feed uses native DataFusion StreamingTable and the same connector
execution, with an instrumented real GreedyMemoryPool for deterministic fault
placement. It does not replace the production Fluss source. Pairing those controlled
fault cases with actual Fluss-source delivery/routing avoids another transport,
writer, scheduler, pool policy or generic event framework.

## Resource/observation limits

Input retention ceiling, native queue limits and caller-selected shared pool remain
their existing policies. Arrow decode/gather/frame admission is not a global
allocator/RSS ceiling. Externally retained buffers/operators stay charged after
query cancellation until their last owners drop. Native partial-frame send/drain
has its own30s bound; tests observing small-frame recovery<=3s do not assert that
every kernel/socket can be forcibly killed in3s.

Broadcast observers are bounded; Lagged/missing terminal events mean incomplete
history. Earlier ACKs remain known, attempted current-batch outcomes remain
conservatively uncertain. `count` only appears on EOF and successful cleanup.
No replacement/transitory streaming writer remains: log uses the client Arrow
route, KV uses its required native row-format encoding, both through DataSinkExec.

See [write-observation-contract.md](write-observation-contract.md),
[write-pressure-verification.md](write-pressure-verification.md) and
[read-pressure-verification.md](read-pressure-verification.md) for exact boundaries.

## Verification

Final native-sni SQL suite: **8 tests passed**, including the new real-source test.
Final owned Docker matrix passed with added explicit ACK cancellation for both
targets. Core clippy all-targets/all-features `-D warnings`, package formatting and
`git diff --check` passed. DEBUG/8jobs; no release profile/benchmark, bindings rebuild
or wheel replacement is part of this acceptance. Fault permissions/failover matrix
`cf5y` and sustained profiles/Rust final acceptance `dqar` remain separate gates.
