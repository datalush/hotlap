# Native allocation/copy controls — pc5n

2026-10-05, RELEASE/eight-job build, execution affinity CPUs 0–3. Debian heaptrack
1.5.0 tools were downloaded/extracted unprivileged under `/tmp/opencode`; no
system installation, application dependency or connector allocator was added.
Traces are separate short controls, outside the sustained performance runs.

The controlled harness is now versioned in `4b78eca`. Artifact fingerprints and
pre-commit traces below retain their original measurement scope.

Each control writes 1,792 unique rows, with 128 rows/batch and 4 KiB values,
one-second warmup and two-second measurement. Confirmations, final contents and
EOF/cancellation/resource cleanup pass. Allocation totals include setup, test
runtime/collector, writes and readback; they are not pure per-row encoder costs
or allocations occurring only inside the measured stage.

## Partition contiguity is not bucket contiguity

In old2/new3 tables, grouping rows by partition still leaves hash-distributed
bucket rows interleaved. Both layouts can therefore require native bucket gathers.
Initial log controls observed 93,094 and 93,021 total allocation calls; that small
difference does not establish a copy reduction.

To isolate slice versus take, the same profile has a **one-bucket-per-partition
control** (`FLUSS_WRITE_SINGLE_BUCKET_CONTROL=1`). It uses normal client routing
and configuration; no hash/routing implementation is copied into the profiler.
Contiguous input can now be sliced by partition/bucket, while interleaved input
must be gathered. The old2/new3 sustained matrix stays unchanged.

| One-bucket control | Contiguous | Interleaved |
| --- | ---: | ---: |
| Complete-process allocation calls | 93,144 | 94,047 |
| Sampled retained Arrow backing peak | 526,976 B | 1,053,392 B |
| Sampled admitted encoded peak | 1,089,380 B | 2,132,608 B |
| Heaptrack global peak heap | 2.87 MB | 3.37 MB |

The interleaved trace contains the actual native stack
`FlussWriter::enqueue → AppendWriter::append_arrow_batch_with_retainer →
select_rows → arrow_select::take`. Its filtered report includes 168 allocation
calls attributed to `take_bytes` stacks and 56 to `take_impl` box-allocation
stacks. The contiguous trace has no `arrow_select::take` stacks; it still contains
IPC/schema/scratch allocations. Rust v0 symbols and parent `select_rows` frames
are present in the release trace. This directly observes materialization, rather
than inferring it from RSS. Not every whole-process allocation difference is
attributed to gather, and slice sharing is not an allocation-free codec promise.

Heaptrack reports approximately 154 KB outstanding at process exit, including
process-global/test-runtime state. These totals are not proof that all outstanding
allocations are leaks, nor does zero pool reservation imply zero global heap.
Owned source/sink buffers and pool cleanup are verified independently.

## KV controls and selective Arrow materialization

Old2/new3 KV controls observed **63,645** contiguous and **63,089** interleaved
allocation calls over complete processes, each verifying the same row count and
full values. The native KV route remains row/key encoding; these counts do not
claim a columnar KV protocol or a zero-allocation path.

On the contiguous dataset, all-columns/ID/COUNT used respectively
**8,203,632 / 41,920 / 0 decoded Arrow bytes**, while each received exactly
**7,369,977 RPC body bytes**. Interleaved all-columns used 8,138,096 decoded bytes;
ID and COUNT had the same small/zero materialization and unchanged raw-page traffic.
Backing charges include capacities, not extra network bytes. Native ScanKv metrics
now expose traffic through existing RPC instrumentation; allocation traces and
Arrow admission metrics remain separate.

## Reproduction

Slice control (change the case to `log-interleaved` for take):

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_CASE=log-contiguous FLUSS_WRITE_SINGLE_BUCKET_CONTROL=1 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 taskset -c 0-3 /tmp/opencode/heaptrack-tools/usr/bin/heaptrack --record-only -o /tmp/opencode/heap-log-slice target/release/deps/write_profile-c583ab388689dacc --ignored --exact native_continuous_writer_profile --nocapture
/tmp/opencode/heaptrack-tools/usr/bin/heaptrack_print -f /tmp/opencode/heap-log-slice.zst --filter-bt-function arrow_select --print-peaks 0 --print-temporary 0 --peak-limit 3 --sub-peak-limit 1
```

Use the actual Cargo-emitted executable path if its fingerprint differs.
Artifacts: `/tmp/opencode/heap-log-{slice,take,contiguous,interleaved}.zst` and
`/tmp/opencode/heap-kv-{contiguous,interleaved}.zst`. Native APIs/data shapes and
working-tree base are recorded in `native-profile-plan.md`; final commit and
clean-checkout reproduction remain separate.
