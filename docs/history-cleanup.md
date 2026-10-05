# Selective Hotlap history cleanup

The original published `develop` tip was
`c0698eda43b1103aa618f7bc78e9d63f52a8fb05`. Its filtered equivalent is
`17af0dab9415deb4dfffe93f661e35f397ad97e9`. Before adding this documentation,
both tips had exactly the same tree hash:
`f8f3c82d442beaa8dfa55a9fd61e83bcade120aa` (406 files, 6,096,254 bytes).
All 39 source-history commits survived; no Rust implementation or license was
removed from the current tree.

Removed from every reachable version: copied Java sources/site/tooling, imported
non-Rust bindings and their website/release tooling, former active Python/FFI
integration/manifests/examples/tests and the replaced Python STS helper. The
DataFusion core/backport, native Fluss client, Rust fixtures, LICENSE/NOTICE and
evolution of native source remain. Earlier commits are historical snapshots;
path filtering does not promise every old build recipe is still executable.

The filtered fresh-clone pack is 1.48 MiB before this documentation, compared with
the original first push of 33.23 MiB. Reachable unique file versions shrink from
4,510 / 71,830,830 uncompressed bytes to 737 / 13,981,075 bytes. These are distinct
measurements (pack size versus uncompressed historical blobs), not checkout size.

## Traceability

[Full old→new SHA map](history-commit-map.tsv) preserves every original identifier.
Evidence reports/logs and Kata close events originally refer to the old history;
resolve those IDs through this map. Original measurement artifact filenames and
timestamps are preserved. Filtering changes ancestry, not the native measured
code or the test results. Kata receives mapping comments, not deleted/rewritten
historical events. Upstream third-party commit/archive hashes are unaffected.

## Backup and migration

External backup directory for this operation:
`/home/midnattsol/code/datalush/fluss/hotlap-history-backup-20261005/`.
It contains verified complete local/remote Git bundles, the original local Git
metadata archive and filter commit map. This backup is intentionally outside the
repository and must not be pushed or used as a new ref in the cleaned repository.

Only `develop` was published; no tags or other remote branches existed. Publishing
uses an explicit lease for the original tip, refusing any concurrently changed
remote. Existing local detached worktrees move to their mapped IDs. Old reflogs
are expired only after backup and migration, then unreachable old objects are
garbage-collected locally. GitHub may retain unreachable/internal objects for a
time; a fresh normal clone is the verification of reachable published history.

Old independent clones must be re-cloned or migrated using the SHA map before
publishing branches. Do not merge an old-history branch or create a remote backup
tag: either can reintroduce the retired history. The configured legacy `upstream`
remote is not fetched or published by this operation.
