# Native KV MERGE contract and verification — 9h56

MERGE composes native DataFusion Full join, exact predicates, CASE and projection
operators, then uses the existing full-row KV upsert/key-delete writer. The SELECT
helper graph goes through the supplied Session planner/optimizer. `InputPlan`
adapts the supplied physical source through TableProvider; it is not another SQL
engine, row evaluator, scheduler or reconstructed session.

## Supported semantics

- Source must be finite. Values and other providers/planners are accepted when
  their physical source reports boundedness; continuous log/CDC input is rejected.
- Ordinary primary-key tables support matched UPDATE/DELETE, unmatched INSERT
  and admitted NOT MATCHED BY SOURCE UPDATE/DELETE. Append-only targets reject MERGE.
- Configured `table.merge-engine` tables are rejected for SQL MERGE. First-row,
  versioned and aggregation upserts can ignore/version/aggregate rather than
  replace a row, so an ACK would not establish ordinary SQL UPDATE semantics.
  Capabilities record this restriction, and planning/execution recheck it. Native
  INSERT/upsert retains the underlying table's native behavior.
- Conditions use DataFusion SQL null logic; only TRUE selects a clause. A NULL
  first predicate does not suppress a later applicable clause.
- The first eligible clause wins. Matched/target-only/source-only presence is
  represented independently of null column values, so nullable payloads do not
  imply absent rows.
- UPDATE retains unassigned target columns and cannot assign any PK/partition
  column, even if an expression appears to preserve its current value.
- INSERT supplies every target column exactly once, with supported reordering.
  Missing/duplicate/unknown columns or a null required PK reject. Partial INSERT
  is not implemented by silently inventing nullable/default values.
- Count is acknowledged modifying operations, not join rows, clauses, source rows
  or net changed rows. No-op rows do not enter duplicate detection or write counts.

## Multiple matches and partial application

Duplicate detection tracks the encoded PK of **modifying actions** for the whole
finite execution. Two source rows matching one target can be accepted if only
one creates a modifying action; duplicate no-op rows are harmless. Two modifying
actions for the same PK reject, whether they land in one input batch or later ones.

Same-batch duplicate validation completes before enqueue of that batch. A later
duplicate cannot undo a previous batch ACK. The verified late-duplicate case has
confirmed=1, rejected_before_enqueue=1, uncertain=0, failed SQL and the first new
value still stored. It does not restore the original value or retry the statement.

Other transport/ACK failures follow the `bqrq` contract: already confirmed batches
remain known; an attempted batch lacking whole-batch flush confirmation is uncertain.
No successful partial count is emitted. See
[write-observation-contract.md](write-observation-contract.md).

## Concurrency and ownership

Selection is based on finite per-bucket target snapshots, not a global transaction
or compare-and-set at the writer. A concurrent update after selection can be
overwritten even if the snapshot predicate no longer matches the current value.
A source row selected as not matched can collide with a concurrently inserted PK;
the native full-row upsert replaces that row, rather than implementing conditional
INSERT or a uniqueness-conflict transaction. The engine owns conflict/replay policy.

PK routing, composite/partition keys and effective old/new layouts remain client
responsibilities. MERGE does not migrate keys between partitions. Source physical
plans and native operator reset/reexecution rules remain DataFusion's contract.

Duplicate-key state and RowConverter scratch reserve against the supplied pool;
scratch uses selected PK columns, not arbitrary non-key payload. Reusable KV row
encoding, buffers and RPC owners retain their existing separate reservations.
Metrics show merge-key/KV scratch lifetime. A failed query releases those owners,
but an externally retained physical HashJoin plan can still own target snapshot
build-side buffers: source leases stay charged until that retained plan/state drops.
Do not force source reservations to zero merely to make a sink cleanup assertion pass.

## Verified coverage

- Existing `write_sql` evidence: UPDATE/DELETE/INSERT in one statement, first clause
  precedence, no-op, NOT MATCHED BY SOURCE DELETE, same-batch duplicates, external
  three-partition MemTable source/custom planner, composite `(region,id)` old2/new3
  layout and PK/partition update rejection.
- New `merge_preserves_null_logic_precedence_and_rejects_unsupported_variants`:
  NULL predicate followed by UPDATE, nullable payload, target-only DELETE/UPDATE,
  INSERT column reordering, preserved unassigned columns, duplicate no-op/one-active
  match and rejected PK update, partial INSERT, null PK, first-row target and
  unbounded source. Counts, summary and stored rows are checked.
- New `merge_duplicate_in_later_batch_preserves_earlier_ack_and_releases_state`:
  one-row batches expose the first ACK before later duplicate rejection, failed
  terminal summary preserves that ACK, stored value proves no rollback, and pool
  state releases within the established bounded cancellation observation.
- Owned Docker matrix: snapshot UPDATE vs concurrent upsert and source-only INSERT
  vs concurrent insert demonstrate native non-CAS behavior. Later ACK stall preserves
  confirmed prefix and uncertain MERGE result, checks final submitted-prefix rows,
  observes admitted key state and verifies key/KV scratch release. Target source
  buffers retained by the inspected physical plan remain charged until plan drop.

Functional builds use DEBUG/8 jobs. This does not imply transaction isolation,
exactly-once semantics, global snapshots or unbounded MERGE source support.
Permissions/failover acceptance is `cf5y`; joint continuous INSERT acceptance is
`bsjm`; binding parity remains the later FFI/Python phase.

Final verification: **7 native-sni SQL tests, 29 core tests and the complete Docker
matrix including MERGE races/partial ACK passed**. Core clippy all-targets/all-features
`-D warnings`, package formatting and `git diff --check` passed. Failed assertions
were corrected to test actual ownership: sender cancellation is asynchronous, and
an externally retained HashJoin plan legitimately retains source buffers. No source
lease is freed early and no owned Docker pressure containers remain.
