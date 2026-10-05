# Native clean Git checkout verification

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

## Current environment blocker: remote-read smoke

The attempted clean-checkout RustFS pressure smoke (64 rows, 1s warmup/2s measure)
**did not pass**. Its preflight failed before creating a Fluss Docker fixture:
the configured `http://192.168.68.200:9000/fluss-lab` endpoint was unreachable.
A separate three-second health connection check also timed out. Read-only Docker/
Kubernetes inspection found no local RustFS service to use in that lab context;
no lab deployment, credentials or endpoint configuration was changed.

This is an unresolved external availability blocker, not a successful current
remote regression. Historical/current pre-commit remote RELEASE results remain
recorded in `native-profile-plan.md`, with matching executable source as above.
The six-commit series can be recorded, but final phase/`rm21` closure must not
pretend that every planned clean-checkout check passed. Restore the existing
RustFS endpoint, repeat the short remote smoke, and append the result before
closing the blocked acceptance gate. No owned profile/fault containers remain.

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
