//! Two-source SP8 recovery around a promoted or discarded interrupted commit.
//!
//! Either way both sources resume at their own captured offset: the offsets of
//! two inputs that both use split 0 never collapse into one, and the replayed
//! snapshot equals a full recompute with multiplicities.

#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/pending.rs"]
mod pending;
#[path = "common/recovery/resumable.rs"]
mod resumable;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array};
use hotlap::ZSetBatch;
use hotlap::state::StateBackend;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::InputStream;

use backend::SharedBackend;
use pending::{copy_to_pending, counting, drain, engine_with, seed};

/// Full recompute of a key-only join; rows are `(k, diff)`.
fn recompute_keys(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut rows: BTreeMap<i64, i64> = BTreeMap::new();
    for &(lk, ld) in left {
        for &(rk, rd) in right {
            if lk == rk {
                *rows.entry(lk).or_insert(0) += ld * rd;
            }
        }
    }
    rows.into_iter().filter(|(_, diff)| *diff != 0).collect()
}

/// A key-only MV snapshot as sorted `(k, diff)` tuples.
fn zset_keys(zset: &ZSetBatch) -> Vec<(i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let keys = zset
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len())
        .map(|row| (keys.value(row), diffs.value(row)))
        .collect()
}

/// The inner join of every record in `log_a` and `log_b`.
fn expected() -> Vec<(i64, i64)> {
    recompute_keys(&[(1, 1), (1, 1), (2, 1)], &[(1, 1), (2, 1)])
}

/// Every declared source must have resumed at its own captured offset.
fn assert_resumed(backend: &SharedBackend, pipe: &Pipeline, id: u64) {
    let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let saved = reader.read(id).unwrap().sources;
    for entry in &saved.entries {
        let applied = pipe.sources.get(entry.id).unwrap().source.state();
        for (split, offset) in &entry.state.offsets {
            assert_eq!(
                applied.offsets.get(split),
                Some(offset),
                "source {:?} split {split}",
                entry.id
            );
        }
    }
}

/// Start recovery over a re-drivable or non-re-drivable counting sink.
fn start(
    backend: &SharedBackend,
    pipe: &Pipeline,
    redriable: bool,
) -> (
    hotlap::Hotlap,
    InputStream,
    Arc<AtomicU32>,
    String,
    MetricsRegistry,
) {
    let (sink, commits) = counting(redriable);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let mut hotlap = engine_with(pipe);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();
    let warning = signal.lock().unwrap().clone().unwrap_or_default();
    (hotlap, stream, commits, warning, metrics)
}

#[test]
fn pending_commit_is_promoted_and_both_offsets_resume() {
    let backend = SharedBackend::default();
    let (pipe, valid) = seed(&backend, 3);
    let pending = copy_to_pending(&backend, valid);

    let (mut hotlap, mut stream, commits, warning, _) = start(&backend, &pipe, true);
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "commit must be re-driven"
    );
    assert!(warning.is_empty(), "a promotion is not a discard");
    assert_resumed(&backend, &pipe, pending);

    drain(&mut hotlap, &pipe.sources, &mut stream, 5);
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
}

#[test]
fn pending_commit_is_discarded_and_replayed_from_the_valid_one() {
    let backend = SharedBackend::default();
    let (pipe, valid) = seed(&backend, 3);
    let pending = copy_to_pending(&backend, valid);

    let (mut hotlap, mut stream, commits, warning, metrics) = start(&backend, &pipe, false);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(warning.contains("not re-drivable"), "reason: {warning}");
    assert!(
        warning.contains(&format!("replaying from checkpoint {valid}")),
        "destination: {warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
    assert!(
        backend
            .get(format!("checkpoint/{pending}/valid").as_bytes())
            .unwrap()
            .is_none()
    );
    assert_resumed(&backend, &pipe, valid);

    drain(&mut hotlap, &pipe.sources, &mut stream, 5);
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
}
