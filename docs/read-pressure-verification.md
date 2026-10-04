# Native read pressure/cancellation — w8ap

2026-10-04. Rust working-tree evidence after `f63a4f5` / `6f0ef1d`.
This is functional hardening, not final sustained performance acceptance.

## Changes and actual bounds

| Resource / wait | Enforced behavior | Limit of the claim |
| --- | --- | --- |
| Decoded Arrow batches per source poll | DataFusion streaming and bounded log reader use native `poll_with_batch_limit(..., 1)` | One batch may still be large; decode precedes pool admission |
| Streaming decoded queue | Removed the connector VecDeque; a poll returns zero/one batch directly, excess is an explicit error | Raw fetches and remote download queues are separate client resources |
| Bulk client poll | Existing `poll()` keeps its max-100-batch behavior; limited API validates 1..=100 | Existing 64 MiB raw/decoded bulk cap remains soft |
| Delivered buffers | Existing pool leases survive consumer retention, clones/slices and stream cancellation | Whole-batch charges conservative; not every SQL output allocation or RSS |
| Cancellation after decode | Batch poll returns decoded data without awaiting another fetch/metadata operation | More fetches are initiated when existing buffered data is drained; throughput impact needs R2 profiling |
| Missing schema | Existing collector returns already-decoded batches before awaiting schema lookup, preserving the raw cursor on cancellation | Metadata caches/transport still use their client mechanisms |
| Source execution | Shared batch/KV deadline remains unchanged | Not a whole SQL-query deadline |
| Streaming initialization/topology | Finite timeout using the connection's `scanner_remote_log_operation_timeout_ms` | This existing operation allowance is explicitly reused, not a second retry loop |
| Streaming poll | Same operation allowance plus the normal idle-poll interval | Idle is not a completion timeout; source can idle indefinitely over successive polls |
| Disk/prefetch | Existing remote slot and actual-written-byte permits are retained | Separate from Arrow RAM; existing readonly/isolated S3 prefixes retained |

There is no network await while the batch poll holds already-consumed decoded
output. Previously `send_fetches().await` ran after collecting batches, so an
error/cancellation at that await could discard data while consumed offsets had
advanced. Fetch initiation now happens on an empty/drained poll, not behind a
paused consumer holding newly decoded output. The bounded reader's public output
remains one batch at a time; no alternate scanner or transport is introduced.

Local source errors now live in `error.rs`, separate from buffer ownership:
`FlussScanTimeout`, `FlussOperationTimeout`, `FlussReadInvalidated` and its small
Identity/Schema/Topology/Retention reason enum are boxed in native DataFusion
External errors. Original Fluss/RPC/storage errors keep their existing source
chain rather than being rewrapped in a duplicate hierarchy. Display messages
remain compatible; invalidation never silently switches table/snapshot/offsets.

## Fresh functional evidence (DEBUG, eight jobs)

- **21 core unit tests**, including typed retention/source expiry, leases,
  deadline generations, offset completeness and safe progress frontiers.
- **14 client fetching tests**, including single-batch decode preserving the
  other completed bucket for a later poll, configured fetch limits, pruned tails,
  retention failure and error after decoding another bucket.
- **All nine native-sni read integrations**: batch log/KV, snapshot/evolution,
  empty/reexecution/concurrency, partition discovery/rescale, recreated/schema-
  changed tables, TLS rejection, streaming topology changes, idle/cancellation
  and coordinator failover with fresh-query recovery.
- Streaming pressure fixture produces **100 unique rows in 20 acknowledged
  groups**, while the consumer pauses 70 ms per batch and the producer keeps
  appending. Source pool capacity is 1 MiB. Reservations stay stable during the
  pause, return to zero after each last batch owner is dropped, and stream drop
  leaves no active source streams. Every expected ID is verified without duplicates.
- **Four write SQL integrations** pass with the changed input-source cadence:
  native planner, multipartition input, backpressure, continuous INSERT and
  old/new partition layouts.
- **Three Docker/RustFS/remote integrations** passed on the existing `.6` image:
  filesystem retention/remote cleanup, S3 retention/readonly profile, and real S3
  transient/permanent HTTP failures with cancellation and isolated object cleanup.
  Slow remote-consumer assertions now retain the batch explicitly: dropping the
  stream does not erase a lease for data the consumer still holds.
- Core clippy all-targets/all-features with warnings denied and formatting pass.

The first full native-sni run had an unlabeled failover `Elapsed` while eight
other tests passed. The pod had been replaced and was Ready. Added contextual
timeout diagnostics without increasing or suppressing the bounds; the targeted
rerun and subsequent two full nine-test runs passed. The original timeout's
precise stage was not captured; report this environmental/functional observation
instead of claiming the first run passed or inferring a proven root cause.

## Reused evidence and follow-up

Connector retention controls added after scope clarification: log/KV
`with_max_retained_batch_bytes` defaults to 64 MiB, rejects zero, checks summed
backing capacities with overflow protection and fails before taking a lease when
the batch exceeds it. It does not parse IPC or replace native MemoryPool policy.
EXPLAIN reports the configured ceiling; the native retained-source-bytes gauge
balances with lease lifetimes. Tests cover exact admission, sliced views retaining
larger backing storage, rejection without disturbing other live leases and real
log/KV rejection with an unbounded host pool. Updated unit count: 22; bounded
log/KV, continuous pressure/cancel and all four write integrations pass.

Policy/concurrency is configured by the engine/application. Ownership/admission
is connector responsibility; allocation-time format/decompression limits belong
to client/Arrow. No connector-side IPC estimator or duplicate decoder is added.

- `knx0` sustained 5+30-minute Rust profile remains historical baseline, not a
  fresh benchmark of the new pull cadence.
- `a7fy` / `7306be7` real 900-second STS expiry/renewal remains reused: auth/token
  code did not change and the long renewal run was not repeated. Short real
  HTTP/retention/download-cleanup cases were repeated because polling changed.
- `sz09` mode/idle/topology contracts remain in place, with current live cases
  repeated. Progress is source offered/excluded work, not processed checkpoints.

`pc5n` measures sustained latency/throughput/RAM/disk under the final pipeline.
`cf5y` covers the complete integrated failure/permission matrix. Large single
batch decode, raw replies, compression expansion and client metadata allocations
remain explicitly outside pre-allocation pool protection; do not turn a hard
batch-count bound or a stable representative profile into a universal RSS cap.
No Python/FFI build, mmap scheme, global allocation manager or new retry policy
is part of this work.
