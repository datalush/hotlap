# Native permissions, failures and recovery — cf5y

Working-tree verification on 2026-10-05, after `adc4f70`. Functional builds use
DEBUG and `CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0`. These results exercise
the native Fluss client and DataFusion providers/planner/runtime.

## Real permissions and preserved causes

`tests/authorization.rs` creates a uniquely named, owned SASL/ACL Docker fixture
with authorization enabled. Its admin/reader/writer credentials are fixture-only.
The reader has Describe/Read permissions and the writer additionally has Write.

- Reader SELECT returns the seeded log and KV rows.
- Reader log INSERT, KV INSERT, DELETE and MERGE fail with native
  `AuthorizationException` available through the DataFusion error source chain.
  This is checked with native writer idempotence both disabled and enabled.
- Failed KV executions publish Failed terminal summaries with zero confirmed
  operations. Both tables retain their original data after denied writes.
- Authorized INSERT, MERGE and DELETE return their expected counts. Final KV
  contents are exactly `(1, 'authorized')` and `(3, 'new')`.
- Each completed/failed query releases its owned pool reservations within the
  fixture's three-second observation bound.

This exposed three client defects: ACL result code `0` was treated as an error;
send/connect failures could replace an API error with NetworkException; writer-ID
allocation aborts could discard the native cause in favor of a generic broadcast
diagnostic. ACL result parsing now accepts absent/zero codes, sender handling
retains API error codes, and accumulator flush prioritizes its stored native cause.
The ACL regression covers create/drop/filter results with absent, zero and nonzero
codes; the real authorization fixture covers both writer initialization and send.

## Real socket loss and explicit recovery

`tests/write_pressure.rs::real_writer_connection_loss_and_recovery` is separate
from the saturation/timeout matrix and owns its own single-tablet Docker fixture.
For both log append and KV upsert it:

1. Confirms row `9900` before source EOF and gives that prefix a six-second server
   checkpoint window.
2. Gates row `9901` after metadata admission, stops its tablet immediately (real
   sockets close), then releases encoding. The container filesystem is retained.
3. Requires failed SQL with an External cause, a Failed terminal summary with one
   confirmed and one conservatively uncertain operation, and pool cleanup.
4. Restarts the same tablet and waits up to 60 seconds for leader/offset recovery,
   retaining the last observed offset/error in the failure diagnostic.
5. Uses a fresh connection and explicit new execution to write row `9902`.
   Final SELECT is exactly `[9900, 9902]`; the gated batch never reached the stopped
   server. Source/sink pool reservations return to zero after results are dropped.

The recovery matrix has a 180-second fixture deadline and panic/error teardown.
The separate pressure matrix retains its 90-second deadline and covers actual
buffer exhaustion, blocked ACK, timeout, cancellation, peer isolation, input error
after ACK, topology/schema invalidation and DELETE/MERGE partial-ACK races.

### ACK versus crash durability: observed boundary

The first version stopped this one-replica `.6` server immediately after its warm
ACK. After restart it repeatedly reported offset `0` for 60 seconds. Increasing
the leader wait did not recover that prefix. With the six-second checkpoint
window, both log and KV preserved the prefix and the final-data checks passed.
The server's default `log.replica.high-watermark.checkpoint-interval` is five
seconds. The test gives that window; it does not inspect checkpoint contents or
establish a universal fsync/durability guarantee.

Confirmed observations remain historical ACK facts under the declared policy.
They are not proof of an engine checkpoint or immediate crash persistence.
Recovery in this fixture is same-tablet restart, not replicated leader promotion.
Replication/disk durability and reconciliation must be accepted for the chosen
server profile; this fixture does not establish those stronger guarantees.

## Evidence reuse and remaining gates

- Current pressure/SQL cases cover lost/blocked ACK, exhausted attempts, input
  failure after confirmed batches, write cancellation, peer isolation and final
  data/resource checks. The client suite also covers bounded cancelled-frame drain.
- [Read pressure verification](read-pressure-verification.md) retains the verified
  source cancellation, retained-buffer ownership, invalidation and native-sni
  coordinator failover/fresh-query evidence. These scanner paths were not changed
  by the ACL/sender fixes.
- [Production readiness](production-readiness.md) retains the real S3 transient/
  permanent HTTP, retention and STS expiry/renewal evidence. This pass did not
  repeat the historical 900-second STS run or sustained resource profile.
- `cf5y` remains open for review/authorized commit and profile-scope assessment.
  `pc5n`, `ydvk` and clean reproduction remain distinct `dqar` gates.

## Verification

Final runs on the changed sources: authorization **1 passed (6.20s)**; pressure
and recovery **2 passed (121.68s)**; native-sni write SQL **8 passed (41.38s)**;
client **836 passed, 2 ignored**; connector core **29 passed**; generic DataFusion
DELETE/UPDATE planner **3 passed**. Root and affected client/test-cluster clippy
all-targets passed with `-D warnings`. No owned authorization, pressure or recovery
containers remain after the runs.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test authorization -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib --test delete_planner
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```
