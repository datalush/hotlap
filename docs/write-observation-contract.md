# Native write observations and metrics — bqrq

`FlussLogTable::subscribe_writes()` and `FlussKvTable::subscribe_writes()` return
the standard Tokio broadcast receiver for `FlussWriteProgress`. Subscribe before
execution. Capacity is **128 events per provider**, shared by its planned writes;
cloned providers share that channel. Each physical execution gets a distinct
process-local ID, including reexecution of the same plan and concurrent queries.

This reports application knowledge of append/upsert/delete/MERGE operations.
It is not an engine checkpoint, a persistent job ledger, transaction isolation,
exactly-once delivery, or net changed-row accounting.

## Events and final summary

| Event | Meaning |
| --- | --- |
| `Initialized` | Execution ID, destination path/table/schema, operation, declared ACK policy and write option budgets, before input/preparation |
| `BatchReceived` | A nonempty input batch was seen, with sequential batch ID and cumulative received/pending operations; admission may still reject it |
| `BatchOutcome::Confirmed` | Whole batch's native `flush()` completed successfully under declared ACK mode; published before waiting for more input/EOF |
| `BatchOutcome::RejectedBeforeEnqueue` | Metadata, validation, quota or other preparation rejected this received batch before its enqueue worker started |
| `BatchOutcome::Uncertain` | Enqueue was attempted but whole-batch confirmation was not established; some, all or none of its operations may have applied |
| `Terminated(FlussWriteSummary)` | Complete cumulative snapshot of received operations and execution termination, including stage and whether source EOF was observed |

`FlussWriteCounts` separates `received`, `confirmed`, `rejected_before_enqueue`,
`uncertain` and `pending`. During execution these form a partition of the received
prefix; at termination pending is zero. Uncertain is a **conservative upper bound
on operations possibly applied without whole-batch confirmation**, not proof that
they failed or were submitted. Unread source rows are not part of received counts.

The native client currently exposes an aggregate flush outcome for this route.
An enqueue can submit some bucket groups before failing, and a failed flush can
contain ACKed and unknown groups. The connector does not infer a row-level split
from that error or from retry counts. It preserves earlier whole-batch ACKs and
classifies the current attempted batch as uncertain. No per-row result handles
or unbounded retained batch history are introduced.

Failed/cancelled **execution status** is separate from operation knowledge:

- ACKed batch 1 plus timeout/cancellation in batch 2 preserves batch 1 as confirmed.
- Source error after ACK, while waiting for more input, has no uncertain current batch.
- Metadata/validation/quota rejection before enqueue is known not submitted.
- Cleanup failure after EOF/ACK keeps confirmed operations and `input_exhausted=true`.
- Cancellation before any input can report zero received operations.
- Planning failures happen before sink execution and therefore produce no initialization.
- Proven-empty optimized DELETE/UPDATE plans return SQL count zero without invoking
  a sink, so they also emit no Fluss sink observations; no missing sink execution
  is inferred from that upstream no-op.

ACK policy is `FlussWriteAck::{All, Leader}`. Leader corresponds to `writer_acks=1`
and is not the same replication guarantee as all/-1. Unsupported modes remain
rejected by the existing write validation; initialization records no supported ACK
policy for those attempts.

Confirmation is a historical ACK fact, not proof that a server checkpoint or
disk flush completed. An immediate crash of the single-replica `.6` fixture lost
its just-ACKed prefix; the recovery test preserves it after a six-second server
checkpoint window. See [native failure verification](native-failure-verification.md)
for the observed boundary and the exact recovery profile.

## SQL compatibility and errors

DataFusion's final `count` still counts confirmed operations, appears only after
EOF and successful cleanup, and is not produced as a successful partial count on
failure. Continuous INSERT emits confirmations while its SQL count remains pending.
KV repeated upserts may count twice while leaving one key; DELETE counts acknowledged
selected-key operations; MERGE counts modifying actions, not join/no-op rows.

The observer is independent of normal SQL execution. Original DataFusion/Fluss
errors, typed write timeout phases and their source chains remain unchanged.
Observations carry no row buffers, SQL text, error payloads or credentials.
Applications can retain the native error alongside the structured terminal snapshot.

## Loss and reconciliation

Handle `RecvError::Lagged` / `TryRecvError::Lagged` as missing batch detail. Never
silently drain past it and claim a complete observation history. A later terminal
snapshot has its own cumulative totals but cannot restore lost per-batch events.
If a terminal event or initialization is missing, the observer cannot infer successful
completion from silence or from a closed/dropped receiver. Events from different
execution IDs must not be combined into one execution's counts.

To reconcile an uncertain result, the engine needs the destination identity, ACK
policy, its own source/input lineage, confirmed prefix, attempted uncertain upper
bound and actual stored-data checks appropriate to append vs PK upsert/delete.
Neither job replay nor reconnect erases possibly applied requests. This connector
does not retain input keys/rows for reconciliation or automatically retry the job.
Execution IDs are process-local diagnostic identities, not durable resume tokens.

## Native metrics

The sink implements DataFusion `DataSink::metrics`; its standard `DataSinkExec`
exposes them to execution-plan metrics/EXPLAIN ANALYZE. One fixed set is created
per planned sink. Reexecutions aggregate that plan's counters, and concurrently
running instances add/subtract shared gauges. Execution IDs are in observations,
not metric labels, so repeated execution does not grow metric cardinality.

- Operation counters: `fluss_write_received_operations`,
  `fluss_write_confirmed_operations`, `fluss_write_rejected_before_enqueue_operations`,
  `fluss_write_uncertain_operations`, `fluss_write_confirmed_batches`.
- Execution counters/gauges: `fluss_write_failed_executions`,
  `fluss_write_cancelled_executions`, `fluss_write_active_executions`,
  `fluss_write_pending_operations`.
- Owner-lifetime gauges: `fluss_write_retained_arrow_bytes`, `fluss_write_encoded_bytes`,
  `fluss_write_transport_bytes`, `fluss_write_routing_metadata_bytes`,
  `fluss_write_routing_scratch_bytes`, `fluss_write_kv_scratch_bytes`,
  `fluss_write_merge_key_bytes`.
- Times: `fluss_write_preparation_time`, `fluss_write_metadata_time`,
  `fluss_write_input_wait_time`, `fluss_write_enqueue_time`, `fluss_write_ack_time`,
  `fluss_write_cleanup_time`.

Byte gauges follow existing buffer/Bytes/frame/worker reservations through their
actual owners. A cancelled execution can be inactive with bytes still retained by
a finishing worker/frame; they stay visible until their owners drop. Idle routing
and KV scratch remain legitimately charged. These are cooperative admission values,
including conservative estimates/overlap from [write-pressure-verification.md](write-pressure-verification.md),
not unique allocated bytes, native queue slots or process RSS. Pool admission policy
still belongs to the supplied DataFusion pool; metric guards do not create another pool.

## Verification

Core tests cover rejected vs attempted cancellation, ACK preservation after idle
cancel/cleanup error, detectable observer lag with terminal totals, distinct IDs and
fixed metric cardinality across 200 executions. The real Docker log/KV matrix now
also observes ACK before EOF, source failure after ACK, whole-batch uncertainty on
blocked ACK and saturated cancellation, pre-enqueue quota rejection, EOF completion
and native sink metrics/owner-byte recovery. SQL INSERT/DELETE/MERGE regression counts
remain the established contract. Joint streaming acceptance `bsjm` and final native
fault/profile/consuming-engine acceptance remain their own gates.

Verification: **29 core tests, the complete dual log/KV Docker matrix and all four
native-sni SQL regressions passed**; core clippy all-targets/all-features
`-D warnings`, package formatting and `git diff --check` passed. The combined
verification command exceeded its outer limit during SQL after the other suites
passed; the SQL suite was rerun separately and all four passed. The exact owned
table pair left by that timeout was removed; no Docker pressure fixtures remain.
