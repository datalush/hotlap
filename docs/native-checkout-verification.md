# Native clean Git checkout verification

History was selectively rewritten after this verification. Original test-run
SHA identifiers below are preserved as evidence; their current Rust-history
equivalents are in [the commit map](history-commit-map.tsv). For example,
`e12c2f7` maps to `7ba969b`, and `6f9749f` to `7f2cafa`. This does not change the
native source tested or erase the recorded initial failure/recovery.

## Versioned source

Verified detached checkout **`e12c2f79fd639f9ced67af9e97c5c9ee33ead64a`**, whose
working tree was clean before and after execution. This is an actual Git worktree,
not one of the earlier uncommitted source exports. The source series is:

| Commit | Scope |
| --- | --- |
| `2a85deb` | Retire active Python/FFI integration; native workspaces and locks |
| `043a346` | Preserve authorization causes; real permissions and socket recovery |
| `1a4556b` | Caller DataFusion session/runtime/operator coverage and native example |
| `4b78eca` | Native metrics and sustained read/write/continuous profile harnesses |
| `e12c2f7` | Scoped profile, allocation and cleanup evidence |

Checkout location for this run: `/tmp/opencode/native-commit-checkout`. The target
directory was external (`/tmp/opencode/native-repro-target-a7739086`) and reused
already-fetched dependencies/artifacts. No target, virtualenv, wheel, credentials
or ignored source was copied into the checkout. Builds used `--offline --locked`,
DEBUG, eight jobs and `CARGO_PROFILE_DEV_DEBUG=0`. Earlier source-export evidence
also records a rebuild with an initially empty target; these are distinct checks.

## Passed from the clean checkout

| Check | Result |
| --- | --- |
| Root workspace default tests | 29 core + 3 generic planner passed; 26 opt-in integrations initially ignored |
| Independent client library tests | 836 passed, 2 ignored; 0.86s test time |
| Native write SQL | 8 passed in 42.57s, including the separately executed native log/KV example |
| Native live read SQL | 9 passed in 79.63s, including isolated coordinator failover |
| Owned SASL/ACL fixture | 1 passed in 5.70s |
| Owned saturation/socket-recovery matrix | 2 passed in 121.78s |
| Four-case writer/routing/projection functional smoke | Passed in 19.26s, 1s warmup/2s measurement |
| Real unbounded-reader/concurrent-INSERT functional smoke | Passed in 8.11s, 1s/2s, exact complete prefix and cancellation |
| RustFS remote-read pressure smoke (after endpoint recovery) | Passed in 14.66s from clean `6f9749f`, 64 log rows, 1s/2s |
| Native example build/execution | Offline build; log/KV counts 6/1 with 16 MiB pool and target partitions 2 |
| Root and client/test-cluster clippy | All-targets passed with `-D warnings` |
| Root/client formatting and diff check | Passed |

Only owned table/container fixtures were removed. Native-sni failover uses its
isolated kubeconfig/namespace. Lab credentials and CA remain external inputs.

## Dependency graph and measured-code comparison

Both locked resolved graphs were checked: root 453 packages, client 431. Their
workspaces are respectively `fluss-datafusion` and the four native client crates.
No PyO3, arrow-pyarrow, datafusion-ffi/datafusion-ffi-ext/datafusion-python-util,
Stabby, Rustler or project Python bridge is active. Root pins DataFusion 55.1.0 /
Arrow 59.3.0; the independent client lock pins Arrow 59.0.0. When linked by the
root, the client uses the root lock; imported binding sources stay excluded.

Compared 237 native source/manifests/locks/planner/profile files against the last
verified source export `4ac0ada63aefeb5f7e635e62ad4f058772215117ef66a8738b47e50b31e82103`:
**236 byte-identical; one comment-only wrap in client `metrics.rs`; no executable
differences**. The previously completed RELEASE runs remain their recorded scoped
measurements: four 60s/300s writer cases, 5+30-minute finite reader, separate
60s/300s unbounded native reader, and allocation controls. The unbounded mode was
added after the first writer matrix, with that mode disabled for its workload;
the divisibility lint correction is equivalent. No production/measurement-policy
change justifies repeating an hour of long profiles merely to change Git identity.

## Remote-read smoke: initial blocker and verified recovery

The attempted clean-checkout RustFS pressure smoke (64 rows, 1s warmup/2s measure)
**did not pass**. Its preflight failed before creating a Fluss Docker fixture:
the configured `http://192.168.68.200:9000/fluss-lab` endpoint was unreachable.
A separate three-second health connection check also timed out. Read-only Docker/
Kubernetes inspection found no local RustFS service to use in that lab context;
no lab deployment, credentials or endpoint configuration was changed.

That attempt remains a failed preflight in the verification history. After the
user restored the existing endpoint, the exact same smoke command was rerun from
clean Git checkout **`6f9749fabe1c490c90b776b221261e0f75cb11af`** and **passed in
14.66s**. No source, lab credentials, endpoint settings or test limits changed.

The run completed 8 warmup scans and 12 measured scans over 2.7s; sampled RSS peak
110 MiB, process VmHWM 113 MiB, sampled pool peak 4 MiB, maximum temporary bytes
100,320, and native remote downloads **2,006,400 bytes**. SELECT p50/p95/p99 upper
bounds were 114/704/704ms with zero histogram overflow. Nonzero remote bytes verify
the remote path rather than a local-only fallback. Row/value-shape, early cancel,
pool/temp cleanup and prefix removal checks passed. No owned profile containers
remain and the tested checkout stayed clean.

The external blocker is resolved: every planned clean-checkout check now has a
passing result. The short recovery smoke is functional DEBUG evidence; the scoped
long RELEASE measurements remain separately recorded in `native-profile-plan.md`.
Acceptance stays within those documented configurations and semantic boundaries.

## Reproduction commands

Run from a clean checkout of the tested source, with a writable external target:

```sh
export CARGO_TARGET_DIR=/absolute/path/to/external/build-target
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --offline --locked
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --offline --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo build -p fluss-datafusion --offline --locked --example native_query
FLUSS_NATIVE_QUERY_EXAMPLE="$CARGO_TARGET_DIR/debug/examples/native_query" DATAFUSION_POOL_MIB=16 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test write_sql -- --ignored --test-threads=1
KUBECONFIG=/absolute/path/to/native-sni.kubeconfig CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test live_log_sql -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test authorization --test write_pressure -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test write_profile -- --ignored --exact native_continuous_writer_profile
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_CASE=log-contiguous FLUSS_PROFILE_LIVE_READ=1 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test write_profile -- --ignored --exact native_continuous_writer_profile
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_PRESSURE_ROWS=64 FLUSS_PRESSURE_WARMUP_SECS=1 FLUSS_PRESSURE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --exact --nocapture
```

Offline builds assume registry dependencies have already been fetched. No local
measurement log, exporter script or heaptrack tool is required to build the code.
