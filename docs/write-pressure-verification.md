# Native write resources and fault verification — yeqf

This follows `3etr` (`4c585fe`). Scope is native Rust execution, not Python/FFI
acceptance or sustained benchmark acceptance. Functional builds are DEBUG with
`CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0`.

## Ownership and admission

All connector write consumers register against the supplied TaskContext's real
DataFusion pool. The client still owns its buffer limiter, protocol, routing,
queues, ACKs and retries. `WriterMemoryAccounting` transfers admission guards to
those native owners; it is not a second pool/allocator or retry policy.

| Resource | Admission / owner | Release |
| --- | --- | --- |
| Log input, cast and gather buffers | Existing Arrow backing leases; input/gather ceiling | Last corresponding buffer, including a client-held slice after worker drop |
| KV input batch | Worker reservation | Worker finishes/drop; caller cancellation does not free a still-held batch |
| Routing/group scratch | Client estimate admitted in the worker | Worker finishes |
| Persistent assigner/queue metadata | Native client `reserve_routing` guards, `FlussWriteRoutingMetadata` | Assigner replacement or private writer/accumulator destruction |
| Encoded batches and codec scratch | Native admission before constructing a new batch, `FlussWriteEncoded` | Batch and all returned encoded `Bytes` clones are dropped |
| Framed RPC bytes | Actual framed Vec capacity admitted after serialization, `FlussWriteTransport` | Frame send/drain finishes, including cancelled caller |
| KV reusable row/key encoder scratch | Conservative two-input-backing allowance plus metadata, owned with FlussWriter | Writer's last owner, including a cancelled query's running worker |
| MERGE duplicate-key encoding | Selected PK column backing bytes, not arbitrary non-key payload | Temporary encoding completes; retained key set lasts through finite MERGE |

Encoding allowances are **estimates**, not claims of exact heap usage: prebuilt
Arrow uses four times the uncompressed/framing hint plus 4 KiB; KV uses twice its
native builder capacity plus 4 KiB; native row-log builders use four times their
capacity plus 4 KiB. They account for reusable builder/cache/compression work;
framed RPC capacity has a distinct guard. Persistent routing entries reserve
4 KiB per physical path/assigner and 512 bytes per bucket. Those reservations stay
visible during idle input instead of pretending all writer memory vanished at ACK.
Profile refinement/actual RSS and allocator peaks remain `pc5n` obligations.

Caller admission runs outside the native deque locks. A pool may reject promptly;
it must not wait for another consumer to release memory. Buffer waiting remains
the existing client's blocking limiter. Local RPC memory rejection is not mapped
to a network retry: the original typed `WriterMemoryAdmission` and DataFusion
cause remain available through the flush failure.

Arrow casts/gathers and RPC framing allocate before their retention admission.
The pool is cooperative accounting, not a global allocator or strict RSS ceiling.
Source/sink whole-batch leases can overlap conservatively; cloned slices do not
imply a second payload allocation. Native client permits remain a separate
configured resource, and per-writer limits multiply across concurrent queries.

## Options and deadlines

Existing `ack_timeout=30s` and `max_retries=3` retain their names/defaults. Rust
`FlussWriteOptions` also exposes:

- `preparation_timeout=30s`, representable in **1ms..=3600s**: one shared allowance
  for connection/table/partition setup, and an independent bounded metadata check
  between input batches.
- `max_retained_batch_bytes=64 MiB`, positive: a post-materialization backing
  ceiling for all input providers, independent of available pool capacity.

The batch enqueue/ACK deadline starts before schema/null/admission/MERGE-key work,
then covers worker enqueue, client buffer waits, existing retries and flush.
Validation/CPU work must not begin submission after that deadline has expired.
Idle waiting after an ACK has no batch completion deadline.

`FlussWriteTimeout` exposes `FlussWritePhase::{Preparation, Metadata, EnqueueAndAck}`
inside DataFusion's External cause. Pool, transport and API errors keep their own
causes. Timeout/drop cannot undo server-applied requests and does not establish
row-level application certainty. Structured confirmation results remain `bqrq`.

Each input batch checks table/schema identity. Partitioned execution refreshes
metadata and admits new partitions with their effective counts; changing or
recreating a previously known physical partition invalidates the open execution.
It never guesses the new partition's bucket layout or silently restarts the job.

## Cancellation and cleanup

- The limiter changes its closed predicate under the same mutex as acquire/wait,
  checks it after locking/waking and before acquisition, and rejects unrepresentable
  wait deadlines. Queue append checks closure before mutation/reacquisition.
- Native WriterClient retains an AbortHandle independently of its join handle.
  Cancelling a graceful close cannot detach an unabortable sender.
- Connection close keeps its writer visible until completion; abort/close forbids
  creating a replacement writer on that connection. Last-client Drop aborts sender.
- `spawn_blocking` is cooperative: abort wakes buffer waits and routing/row loops
  stop; an in-progress Arrow kernel is not forcibly preempted. Buffer leases stay
  charged until those owners finish. The input ceiling bounds accepted batch size.
- A partially sent RPC frame preserves framing through the existing cancellation-
  safe send future, now with a finite **30-second drain/write ceiling**. Its frame
  guard survives cancellation. Failure poisons the connection even if the original
  caller is already gone; a partial frame cannot be followed by another request.

The small-frame integration cases recover their pool reservations within 3 seconds.
That is observed evidence, not a universal promise to kill every kernel/socket in
3 seconds; the stalled-frame virtual-clock test covers the separate 30-second bound.

## Verified scenarios

`tests/write_pressure.rs` uses DataFusion StreamingTable/PartitionStream and an
owned plaintext Docker fixture. It pauses only its own coordinator/tablet. Its
pool probe delegates all policy to GreedyMemoryPool and coordinates a pause after
metadata admission, so an ACK test cannot accidentally exercise only metadata.
The native buffer-available gauge proves exhaustion before cancellation.

The matrix runs for **both log append and KV upsert**:

1. Singleton ACK before EOF; idle longer than ACK allowance remains live.
2. Full 64 KiB client buffer/32 KiB batches under a paused tablet, cancel producer,
   close input and release owned input/encoding/frame guards.
3. Concurrent private writer survives peer cancellation and completes after resume;
   a new explicit execution also writes successfully.
4. Fully enqueued singleton waits on a blocked ACK and returns typed timeout,
   never a successful final SQL count.
5. Blocked metadata and bootstrap/preparation have their own finite typed scopes.
6. A one-byte input ceiling rejects with spare pool capacity, without persisting row.
7. Source error after ACK fails statement while the earlier row remains visible.
8. Table recreation and live ADD COLUMN invalidate the open execution; later input
   does not enter the new table/schema.
9. ACK=0 rejects without writing; ACK=1 with idempotence disabled confirms one
   operation. Default all/-1 remains exercised by the existing SQL suite.
10. A new partition is usable during an open INSERT; a recreated known partition
    rejects later input instead of writing into its new identity.

The four native-sni `write_sql` regressions continue to pass INSERT/DELETE/MERGE,
concurrency, old2/new3 routing, input4MiB/client-buffer2MiB pressure, tiny-pool
rejection and continuous input. Core unit tests cover actual shared pool rejection
and owner lifetimes. Client tests cover append-after-close, cancelled graceful
close, encoded Bytes retaining guards and stalled frame drain/poisoning.

During test development a blocking pool probe exposed admission under a deque
lock; policy calls moved outside that critical section and encoding/transport
consumers became distinct. Two externally timed-out runs left exact owned fixtures
paused; those fixtures were resumed/removed explicitly. The final fixture has
phase diagnostics, a 90s internal deadline and panic/error teardown. Readiness
waits for tablet registration and leader assignment after table recreation.

Final verification: **835 client tests passed, 2 ignored; 25 core tests passed;
4 native-sni SQL tests passed; the complete dual log/KV Docker matrix passed**.
Core/client/test-cluster clippy `-D warnings`, package formatting and
`git diff --check` passed. No owned `df-write-pressure` containers remain.

Commands from repository root:

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-sync --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```

Permissions/failover/reconciliation matrix `cf5y`, structured write knowledge
`bqrq`, joint continuous acceptance `bsjm`, DELETE/MERGE concurrency semantics and
profiles retain their separate acceptance. Python source only inherits new Rust
defaults for its existing constructor; FFI builds/parity belong to the later phase.
