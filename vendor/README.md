# Patched native dependencies

## DataFusion 55.1.0 — filter-only DELETE/UPDATE hooks

`datafusion-55.1.0/` is the published Apache DataFusion crate, acquired from
`https://static.crates.io/crates/datafusion/datafusion-55.1.0.crate`.

Archive SHA256 (matches the original registry lock entry):
`14313430439ef858ed5f52ca9c840e9eea9844da34d3716d05a476449634da5d`.
Upstream license and notice files remain in the package.

The only upstream source file modified is `src/physical_planner.rs`:

- Backport of Apache DataFusion [PR #24657](https://github.com/apache/datafusion/pull/24657),
  merged as `2306a4b7599dc88490c0b39f082b4cdf554a5fb9`.
- Classify the optimized DELETE/UPDATE input before extracting filters: proven
  empty input returns the standard UInt64 zero count without calling a provider;
  joins/unsupported restrictions reject rather than becoming unconditional writes.
- Small fail-closed extension: reject `Limit`, whose row bound also cannot travel
  through `delete_from(session, filters)` (upstream issue
  [#24998](https://github.com/apache/datafusion/issues/24998)). The merged upstream
  classifier still allowed this shape; we do not claim to implement DELETE LIMIT.

All other package sources/manifests are the published crate. The workspace
`[patch.crates-io]` selects this core crate; its catalog/session/FFI/Arrow dependency
versions remain the same registry versions. This is a generic native planner fix,
not a Fluss parser, replacement planner, session wrapper or a registry-cache edit.
The upstream merged revision already uses Arrow 60, so pinning its full workspace
would expand the current Arrow 59 migration scope.

The patched source uses the upstream formatter setting `edition=2024,
max_width=90`; formatting it with the default width would create unrelated churn.

The patched source is committed directly for reproducible offline builds after
normal dependency fetch. To audit the backport, verify the archive hash, extract
the original package outside the repository and compare its
`src/physical_planner.rs` against this copy. No preparation script/ignored target
checkout is required to build from a clean checkout.

Remove this override and vendor directory once a compatible upstream release
contains equivalent empty-input/unsupported-restriction protection; verify the
native Fluss DELETE regressions before doing so. Future upgrades must not silently
restore the filter-loss behavior.
