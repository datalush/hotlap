# Final-route native profiles — pc5n

Acceptance criteria fixed before the new measurements, 2026-10-05. Profiles use
RELEASE with eight compilation jobs; correctness suites use DEBUG/eight jobs.
The runs below used the working tree based on `adc4f70`; that is their historical
source identity, not a claim that old HEAD contained the new code. The native
profile harnesses and ScanKv measurement path are now versioned in `4b78eca`, after
retirement/failure/engine commits `2a85deb`, `043a346`, `1a4556b`. Original artifact
paths and snapshot fingerprints remain evidence locations, not build prerequisites.
Clean Git reproduction of this versioned series is reported separately.

## First measured run: existing read-pressure workload

Reuse `remote_retention::datafusion_resource_pressure_rustfs`, whose existing
sampler checks actual process RSS, cooperative pool reservations and scanner
temporary-file bytes. Use the current native pipeline and `.6` image, a unique
RustFS object prefix, four CPU affinity IDs and no concurrent benchmark workload.

- Dataset: 4,800 log rows of 128 KiB (600 MiB decoded, greater than the pool) and
  64 KV rows (8 MiB). Existing native-client seeding is setup, not a SQL-sink
  throughput measurement.
- Pool: 512 MiB shared across four concurrent SELECTs (two log, two KV), with two
  physical source partitions per query.
- Client: existing 1 MiB remote chunks, two read operations/two downloads and
  two prefetched segments/64 MiB per scanner.
- Initial duration: 30s warmup plus 120s measurement; complete in-flight waves
  may extend elapsed time. This is a short measured profile, not the historical
  5+30-minute sustained acceptance.
- Hard observed budgets: sampled RSS and kernel VmHWM at most 1.5 GiB; scanner
  temporary files at most 2 GiB; pool/temporary bytes zero after each wave and
  cancellation; complete unique row IDs, 128 KiB value lengths and matching row
  prefixes for every full scan. The profile does not compare each full value byte.
- Report scan count/elapsed time, RSS after warmup/final/peak, peak reservations,
  peak temporary bytes and remote bytes. Require nonzero remote bytes to classify
  the run as remote-read evidence. The short-run harness does not enforce the
  full-profile post-warmup 256 MiB RSS-growth check; report the observed difference.
- Hardware: Intel Core i9-13900H, 20 online logical CPUs, 62 GiB RAM;
  rustc/cargo 1.97.1, Linux x86_64. Execution affinity is CPUs 0–3; compilation
  is eight jobs without that runtime affinity limit. Host has other workloads;
  RSS describes this test process, not Fluss server/Docker or total host memory.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_PRESSURE_ROWS=4800 FLUSS_PRESSURE_WARMUP_SECS=30 FLUSS_PRESSURE_MEASURE_SECS=120 CARGO_BUILD_JOBS=8 uv run --no-project --env-file ../lab/.env taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --exact --nocapture
```

## Short RELEASE result — passed

One measured run completed in **162.75s total**, with 56 warmup scans and
128 measured scans over **120.3s**. A scan is one complete SELECT; the measured
mix is 64 log and 64 KV queries. Mixed completion rate is about **1.06 scans/s**,
including the intentional one-millisecond consumer pause per returned batch.
This is not an unconstrained throughput ceiling or per-query latency percentile.

| Resource | Observed result |
| --- | ---: |
| RSS after warmup | 104 MiB |
| Sampled RSS peak | 116 MiB |
| Kernel process VmHWM | 118 MiB |
| Final RSS | 80 MiB |
| Sampled pool reservation peak | 32 MiB |
| Sampled scanner temporary-file peak | 414,912 bytes |
| Native remote download counter | 1,118,480,288 bytes |

Every wave's row/duplicate/value-shape checks, early cancellation and zero-owned
pool/temporary-byte checks passed. The native remote counter covers the workload
including warmup/setup/cancellation, not exclusively measured complete scans.
RustFS prefix cleanup returned success; no owned `datafusion-pressure` containers
remain. The fixture now uses a unique name and explicit stop/prefix cleanup even
when the workload returns an error or panics.

Base HEAD is `adc4f70f760904e795a9cd81a7ce728581286f55`; selected working-tree
native code/lock/profile diff SHA256 is
`086b54bf816c84b8fc889b4a76f7ebe4f571b0b6c6ad90966c223d403252f0d6`:

```sh
git diff -- Cargo.toml Cargo.lock clients/rust/Cargo.toml clients/rust/Cargo.lock clients/rust/crates/fluss/src clients/rust/crates/fluss-test-cluster/src crates/fluss-datafusion/src crates/fluss-datafusion/tests/remote_retention.rs | sha256sum
```

Initial release dependency compilation took 7m20s, followed by a 44.54s rebuild
of the updated profile harness. Build time is outside the measurement. This
fingerprint is a working-tree identifier, not an authorized commit or clean-source
reproduction. Logs/exit status were captured outside the repository under
`/tmp/opencode/native-profile-b2b6a64e2e504276942ee3a79263d6c4.{log,json}`;
exit code was zero. The historical DEBUG profile and this shorter RELEASE run
cannot isolate a connector-only speedup.

## Sustained continuous INSERT — passed

The four RELEASE cases completed in **1,461.30s** total, serially on CPUs 0–3,
with the fixed 60s/300s stages and normal old2/new3 routing. Each measured stage
confirmed **153,728 rows in 300.01s** (about 512.4 rows/s, including the boundary
batch). Each final table contained **184,576** fully verified rows including
warmup. Contiguous cases completed at EOF; interleaved cases cancelled while idle
after ACK. All terminal confirmations/data/zero-uncertainty/pool checks passed.

| Case | Confirmation p50/p95/p99 (ms) | Measured CPU (s) | Final RSS (B) | Process HWM (B) | Sampled pool peak (B) |
| --- | --- | ---: | ---: | ---: | ---: |
| Log partition-contiguous | 10 / 13 / 14 | 6.06 | 67,391,488 | 67,391,488 | 3,220,116 |
| Log interleaved | 10 / 13 / 16 | 6.39 | 74,473,472 | 77,553,664 | 3,220,116 |
| KV partition-contiguous | 13 / 19 / 23 | 6.10 | 81,166,336 | 84,316,160 | 4,510,184 |
| KV interleaved | 12 / 17 / 19 | 5.90 | 86,011,904 | 86,011,904 | 4,246,896 |

HWM is cumulative within the serial test process; sampled peaks are not exhaustive
allocator peaks. Millisecond percentiles are histogram upper bounds. Native
ACK/enqueue cumulative times are separately printed in the raw log; confirmation
also includes preparation/input/encoding. Native buffer availability never sampled
below 786,432 B and waiting-thread peaks were zero during measured stages. This
workload does not claim saturated-buffer capacity; fault-pressure evidence covers
that separate case. Transport gauges sometimes sampled zero because short frames
were missed, not because no transport allocations occurred.

Measured RPC body bytes sent: **7,095,149 / 7,150,829** for the two log cases,
**632,940,211** for each KV case. Corresponding received ACK/data response bytes
were 57,923–57,929. Compression/framing/protocol differences mean these are not
decoded Arrow throughput ratios. Source pool reservations remain cooperative
accounting, not unique heap bytes or server/Docker RSS.

KV contiguous readback of 184,576 rows materialized **774,635,752 / 3,047,584 / 0**
decoded Arrow bytes for all-columns/ID/COUNT, with **759,272,507 RPC body bytes**
for each query. Interleaved readback used **774,942,608 / 3,043,392 / 0** Arrow bytes
and **759,272,458 RPC body bytes** per query. All values/regions/IDs were checked;
CPU for all/ID/COUNT was 0.80/0.37/0.34s and 0.73/0.36/0.31s respectively. Less
Arrow materialization does not prune the native KV pages sent over the protocol.

The serial suite log is
`/tmp/opencode/native-profile-05ef9282f8b64afb9bc4f82a369a5ea8.log`. The writer
stage exited zero; the long reader stage follows separately in that log. Direct
allocation/slice/take controls are in [native allocation profiles](native-allocation-profile.md).

## Continuous workload criteria and command

The new `write_profile::native_continuous_writer_profile` fixes the continuous
workload before the full run: 128 rows/batch, 4 KiB values, at most four batches/s
(512 offered rows/s), one outstanding confirmation, input channel capacity one,
target partitions one, 64 MiB host pool and a separate 2 MiB native client buffer
with 256 KiB native batches. Each log/KV case uses old2/new3 partitions, first with
contiguous partition groups and then interleaved groups. Warmup is 60s and measured
duration 300s per case. Contiguous cases end at EOF; interleaved cases cancel while
idle after the last ACK. Every confirmed ID/region/full value is read back, with
zero uncertainty and zero owned pool reservations after completion/cancellation.

Fixed pass criteria: at least 90% of the paced offer (3.6 batches/s), confirmation
p99 at most 1s, no overflow in the bounded millisecond histogram, process RSS/HWM
at most 1.5 GiB, post-warmup RSS growth at most 256 MiB and pool peak within 64 MiB.
Report confirmation percentiles separately from native ACK/enqueue time, sampled
owner gauges/client buffer availability/waiting threads, process CPU seconds and
native RPC body bytes. Sample only the measured stage, excluding writer startup
and EOF closure. These are controlled paced workloads, not maximum throughput.

KV readback compares all columns, ID-only and COUNT on exactly the same data.
Check full data, less Arrow materialization for ID and zero decoded Arrow bytes
for COUNT, while recording actual RPC body bytes independently. `ScanKv` is now
included in the existing fixed-label RPC metrics whitelist; previously these
requests were not reportable and misleadingly appeared as zero bytes. The
profile's facade recorder retains counters across context destruction and ignores
facade histograms; it introduces no transport, memory policy or retry stack.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test write_profile native_continuous_writer_profile -- --ignored --exact --nocapture
```

`FLUSS_WRITE_CASE` can select one of `log-contiguous`, `log-interleaved`,
`kv-contiguous`, `kv-interleaved` for separate allocation traces. Trace runs are
short instrumented controls, not performance-acceptance results. Heaptrack tools
are extracted unprivileged outside the repository; no allocator or dependency is
added to the native connector. The long read profile now also records real SELECT
latencies in a fixed-size histogram (millisecond upper-bound p50/p95/p99).

## Measurement scope/status

### Continuous native source measurement

The finite repeated-scan workload below is distinct from unbounded reading. A
separate `FLUSS_PROFILE_LIVE_READ=1` case concurrently reads the real Fluss log
with `FlussLogTable::open_with_options(LogReadOptions::default())` while the
existing continuous INSERT produces old2/new3 data. It uses the same 64 MiB caller
pool, a one-millisecond pause after each read batch and fixed-size ID tracking
(200,000 slots; duration/rate cap below that). Warmup/measurement are 60s/300s.
Before completion it requires reader throughput at least 90% of the paced 512
rows/s, every confirmed ID/region/full value exactly once, the complete prefix
within 15s of producer termination, explicit reader cancellation and zero owned
reservations. The existing writer RSS/pool/confirmation criteria also apply.
RPC bytes/CPU in this case include both reader and writer, not isolated ACK costs.

The RELEASE run passed in **365.57s** total. During 300.010s measurement, the native
unbounded reader observed **153,728 rows** (about 512.4 rows/s) while the writer
confirmed the same measured count. It verified all **184,576** IDs/regions/full
values including warmup, then cancelled explicitly and released all owned leases.
Process HWM was **69,865,472 B**, sampled shared-pool peak **3,220,116 B**, measured
combined CPU **7.26s**, confirmation p50/p95/p99 **10/13/14ms**, histogram overflow
zero. Combined reader/writer RPC body bytes were 7,164,379 sent / 6,968,742 received.
This is native continuous readback of arriving Fluss data, not inferred from a
finite SELECT loop. Log/status:
`/tmp/opencode/native-profile-a7922664c0e546e2b56529eb2af94125.{log,json}`.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_CASE=log-contiguous FLUSS_PROFILE_LIVE_READ=1 CARGO_BUILD_JOBS=8 taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test write_profile native_continuous_writer_profile -- --ignored --exact --nocapture
```

### Full sustained RELEASE read — passed

The reader stage completed with exit zero in **2,118.91s total**: 5-minute warmup
(324 complete scans), then **1,796 measured scans over 1,803.4s**. The mix is 898
log and 898 KV queries with repeated early cancellation and zero pool/temporary-
byte checks between waves. Dataset/policy remain 600 MiB decoded log / 512 MiB
host pool, four concurrent SELECTs and CPUs 0–3.

| Resource / latency | Result |
| --- | ---: |
| Warmup RSS | 77 MiB |
| Sampled RSS peak / process VmHWM | 118 / 118 MiB |
| Final RSS | 67 MiB |
| Sampled pool peak | 35 MiB |
| Sampled temporary-file peak | 539,760 B |
| Native remote download counter, whole workload | 13,349,538,688 B |
| Complete SELECT p50/p95/p99 upper bounds | 115 / 3,968 / 4,236 ms |
| Latency histogram overflow | 0 |

Memory/disk/cleanup/data criteria and the post-warmup 256 MiB RSS-growth check all
passed. Percentiles combine fast 64-row KV and large 4,800-row log queries; they
are not log-only or protocol ACK latencies. Rate is about 0.996 mixed scans/s with
the intentional slow-consumer pause, not an unconstrained throughput ceiling.
Both serial-suite stages exited zero. Final job status is beside the log; no owned
profile containers remain and RustFS prefix cleanup returned success.

The historical DEBUG baseline measured 112 scans in 1,854s. This RELEASE run is
faster under its settings, but build mode and final pipeline changed together;
the difference is not attributed exclusively to the connector or another engine.

1. **Read duration/control comparison:** final-route 5+30-minute reference profile
   with the same dataset/concurrency, RSS growth at most 256 MiB after warmup, plus
   actual query latency samples. Compare to historical `knx0` only with compiler,
   build mode, hardware, dataset and source cadence differences explicit.
   Completed above on the final native pipeline.
2. **Continuous log/KV INSERT:** reuse native StreamingTable/PartitionStream
   sources and sparse/batched workloads from `bsjm`, with 60s warmup and 300s
   measurement per workload. Fix batch sizes, client buffers, target partitions
   and bounded host pool before running. Capture ACK latency/throughput and native
   buffer stalls separately from SQL EOF count; verify every confirmed ID and
   final log/KV contents, cancellation and zero owned reservations after cleanup.
   The four-case sustained run above completes this paced workload, not maximum
   throughput or arbitrary server-profile acceptance.
3. **Mixed partition routing and wide KV projection:** compare contiguous slices
   vs interleaved gathers, and all-columns/subset/COUNT on identical data. Measure
   CPU/RSS/network plus Arrow/native metrics independently; zero Arrow output bytes
   for COUNT does not imply zero raw-page traffic. Allocation/copy claims require
   direct measurements, not an inferred ratio from pool reservations.
   The sustained projection results and separate heaptrack controls above provide
   final-route evidence for these specific shapes; wider/different data needs its
   own measurement rather than a blanket allocation claim.

Historical `knx0` 5+30-minute profiles and `a7fy` real 900-second STS renewal remain
scoped baselines. The changed pull cadence, selective KV decode, writer routing
and owner accounting have scoped final-route measurements above. No Flink parity,
universal RSS cap or exactly-once guarantee follows from these profiles.
