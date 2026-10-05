# Final-route native profiles — pc5n

Acceptance criteria fixed before the new measurements, 2026-10-05. Profiles use
RELEASE with eight compilation jobs; correctness suites use DEBUG/eight jobs.
Current source is the uncommitted working tree based on `adc4f70`, not an accepted
release or a reproducible clean checkout.

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

## Continuous native source criteria

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

Results and allocation evidence are recorded after these predeclared criteria.
