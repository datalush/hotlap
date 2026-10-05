# Native KV DELETE contract and verification — jwyv

DELETE uses the existing `UpsertWriter::delete` and DataFusion operators to
select/filter keys from finite per-bucket KV snapshots. There is no separate SQL
parser, row evaluator, transaction coordinator or DELETE transport.

## Selection and count

- No WHERE selects all rows visible to the snapshot scans. No matching keys
  produces the standard UInt64 count zero.
- Predicates follow DataFusion SQL null logic: `value = NULL`, `FALSE`, `1 = 2`
  and an impossible null conjunction affect zero rows, not every row.
- UDFs, exact residual filters, OR/IS NULL and table-name qualifiers use the
  caller's planner/function registry. Unqualified predicates on a DELETE target
  alias work. DataFusion 55.1 rejects predicates qualified by the target alias
  during SQL resolution; there is no connector alias rewriting workaround.
- Count is acknowledged **selected-key deletion operations**, not net removed
  rows. A key removed by a concurrent statement still counts if its delete
  request ACKs. A subsequent SQL DELETE whose snapshot selects no keys counts zero.
- PK encoding/routing remains native. Composite `(region,id)` keys with the same
  id in another partition are distinct; old2/new3 effective bucket layouts remain
  respected after rescale.
- Append-only logs reject individual DELETE. Clearing log rows through another
  API, retention or an administrative operation is not SQL row deletion.

Snapshot selection and key deletion are **not atomic or conditional**. A value
matching the WHERE predicate can be replaced after selection, and the native
key delete can remove that replacement. Each bucket opens its own snapshot;
there is no common transaction across buckets/partitions. The engine owns
conflict policy, job retry/reconciliation and isolation requirements.

## Table policies

The connector checks effective metadata when planning and executing DELETE and
before batch submission. Only `table.delete.behavior=allow` is accepted. It
rejects `ignore`/`disable` instead of converting an ignored request's ACK into a
successful SQL deletion count.

Regular PK tables default to allow. Configured first_row/versioned/aggregation
merge engines default to ignore when no delete policy is specified; the server
materializes the effective property for supported descriptors. First_row/versioned
do not permit an allow descriptor; aggregation's explicit allow follows server
validation. Unknown/non-allow effective behavior is rejected conservatively.

The pinned server does **not** support in-place ALTER of `table.delete.behavior`.
The live test verifies that rejection and that the original allow plan remains
valid; it does not simulate a policy transition the server cannot perform.
Cached capabilities are descriptive, not authorization or future metadata proof.
Permissions and the full failover matrix remain the separate `cf5y` acceptance.

## Partial execution

The `bqrq` observation contract applies with `FlussWriteOperation::Delete`.
Whole input batches ACKed before a later failure stay confirmed. A subsequent
attempted batch without successful aggregate flush remains uncertain; SQL returns
the original error and no successful partial count. Some or all uncertain keys
may be deleted after timeout/cancellation. No compensating inserts or rollback
are attempted. See [write-observation-contract.md](write-observation-contract.md).

## Required native DataFusion backport

The new `value = NULL` regression exposed DataFusion 55.1 turning optimized
`EmptyRelation` into an empty filter vector passed to `delete_from`, which means
unconditional deletion to the provider. It deleted all three isolated fixture
rows rather than zero. This information is lost before the connector hook, so a
provider-only fix cannot distinguish the case from legitimate DELETE without WHERE.

With user authorization the workspace now patches the **published DataFusion
55.1 core crate**, retaining Arrow 59/catalog/session/FFI versions:

- Backport upstream PR [24657](https://github.com/apache/datafusion/pull/24657),
  commit `2306a4b7599dc88490c0b39f082b4cdf554a5fb9`.
- Proven empty DELETE/UPDATE input returns zero without invoking the provider.
- Joins/unsupported optimized restrictions reject instead of losing selection.
- Generic fail-closed `Limit` rejection covers upstream issue
  [24998](https://github.com/apache/datafusion/issues/24998); DELETE LIMIT is not
  implemented. IN/EXISTS subquery join plans are not supported by filter-only hooks.

The only changed upstream source is `vendor/datafusion-55.1.0/src/physical_planner.rs`.
Archive checksum/license/source provenance and removal conditions are in
[vendor/README.md](../vendor/README.md). A direct pin to the merged upstream
workspace would require Arrow 60; this backport avoids that unrelated migration.

An optimized proven-empty statement never invokes the native sink: it returns
SQL count zero and has no Fluss write observation events. This is distinct from
an executed sink receiving an empty snapshot, which initializes/terminates with
zero received/confirmed counts. Do not invent a sink execution ID for an upstream
no-op plan.

## Evidence

- `tests/delete_planner.rs`: three transport-independent native MemTable tests
  for empty DELETE, empty UPDATE and rejected subquery/LIMIT restrictions, with
  rows preserved and zero/normal counts.
- `tests/write_sql.rs`: five native-sni tests. The expanded existing cases cover
  FALSE/NULL/contradiction, no WHERE/cero, UDF/OR/null/alias, append-only rejection,
  composite PK and old2/new3 data preservation. New policy coverage exercises
  default/allow/ignore/disable/first_row, proves ignored native ACKs leave a row,
  checks SQL rejection/capabilities, native missing-key ACK and unsupported ALTER.
- `tests/write_pressure.rs`: the owned Docker matrix adds deterministic
  snapshot-vs-upsert and concurrent-delete races, both returning selected-key
  ACK count one; it then confirms an initial snapshot batch before a paused
  subsequent ACK, verifies failed SQL plus confirmed/uncertain DELETE summary,
  checks remaining rows and releases pool reservations.

Functional checks use DEBUG/8 jobs. Bindings remain in their later migration
phase. No native client/connector route was replaced or kept as a reserve path:
the fix belongs to the generic native planner which originally lost selection.

Final evidence on the patched dependency: **5 native-sni SQL tests, 3 generic
planner regressions and the complete Docker matrix including DELETE races/partial
ACK passed**. Core clippy all-targets/all-features `-D warnings`, package formatting,
upstream-width vendor-source formatting and `git diff --check` passed. Archive
comparison confirmed only `physical_planner.rs` differs in the vendored package.
The two exact table pairs left by failed regression assertions were removed;
owned Docker pressure fixtures were torn down. No bindings were rebuilt.
