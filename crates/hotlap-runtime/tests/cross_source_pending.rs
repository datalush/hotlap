//! Two-source recovery over a *distinct* pending body.
//!
//! The valid checkpoint holds `k=1` on both sources; the pending one adds `k=2`.
//! Promote re-drives the sink commit and resumes the later offsets with no
//! replay; discard and a corrupt payload fall back to the earlier offsets and
//! replay both sources. The resumed offsets are observed on fresh sources, so
//! two inputs on split 0 never collapse into one.

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
use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, ZSetBatch};
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::source_checkpoint::decode_sources;
use hotlap_runtime::runtime::sources::{InputStream, Sources};

use backend::SharedBackend;
use pending::{
    counting, engine_with, fresh_pipeline, put_sources, seed_pair_with_redriable, sources_bytes,
};

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

/// The inner join of every record in `log_a` (`1,1,2`) and `log_b` (`1,2`).
fn expected() -> Vec<(i64, i64)> {
    recompute_keys(&[(1, 1), (1, 1), (2, 1)], &[(1, 1), (2, 1)])
}

/// Drain the recovered stream into `hotlap`, acking each event at its source.
fn drain(hotlap: &mut Hotlap, sources: &Sources, stream: &mut InputStream, limit: usize) {
    let mut pushed = 0;
    while pushed < limit {
        match futures::executor::block_on(stream.next()) {
            Some(Ok(event)) => {
                pipeline::ingest_event(hotlap, sources, &event).unwrap();
                sources
                    .get(event.input)
                    .unwrap()
                    .source
                    .commit(event.batch.split, event.batch.next_offset)
                    .unwrap();
                pushed += 1;
            }
            Some(Err(error)) => panic!("unexpected source error: {error}"),
            None => break,
        }
    }
}

/// Each fresh source must have resumed at the offset saved in checkpoint `id`.
fn assert_resumed(backend: &SharedBackend, pipe: &Pipeline, id: u64) {
    let encoded = backend
        .get(format!("checkpoint/{id}/sources").as_bytes())
        .unwrap()
        .unwrap();
    let saved = decode_sources(&encoded).unwrap();
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

/// The recovery results handed back to a test.
type Started = (Hotlap, InputStream, Arc<AtomicU32>, String, MetricsRegistry);

/// Start recovery over a re-drivable or non-re-drivable counting sink.
fn start(backend: &SharedBackend, pipe: &Pipeline, redriable: bool) -> Started {
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
    let (_valid, pending) = seed_pair_with_redriable(&backend, true);
    let pipe = fresh_pipeline();

    let (mut hotlap, mut stream, commits, warning, _) = start(&backend, &pipe, true);
    assert_eq!(commits.load(Ordering::SeqCst), 1, "commit re-driven");
    assert!(warning.is_empty(), "a promotion is not a discard");
    assert_resumed(&backend, &pipe, pending);

    // The promoted body is the later state: it already holds the full recompute,
    // so the resumed stream must not replay the earlier `k=1` prefix.
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
    drain(&mut hotlap, &pipe.sources, &mut stream, 5);
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
}

#[test]
fn pending_commit_is_discarded_and_replayed_from_the_valid_one() {
    let backend = SharedBackend::default();
    let (valid, pending) = seed_pair_with_redriable(&backend, false);
    let pipe = fresh_pipeline();

    let (mut hotlap, mut stream, commits, warning, metrics) = start(&backend, &pipe, false);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(warning.contains("not re-drivable"), "reason: {warning}");
    assert!(
        warning.contains(&format!("replaying from checkpoint {valid}")),
        "destination: {warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
    let valid_key = format!("checkpoint/{pending}/valid");
    assert!(backend.get(valid_key.as_bytes()).unwrap().is_none());
    assert_resumed(&backend, &pipe, valid);

    // The fallback is the earlier valid state, which lacks `k=2`; replay adds it.
    assert_ne!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
    drain(&mut hotlap, &pipe.sources, &mut stream, 5);
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
}

#[test]
fn a_corrupt_pending_body_is_discarded_and_both_sources_replay() {
    let backend = SharedBackend::default();
    let (valid, pending) = seed_pair_with_redriable(&backend, true);
    assert!(sources_bytes(&backend, valid).starts_with(b"HLSR"));
    assert!(sources_bytes(&backend, pending).starts_with(b"HLSR"));
    // Keep the `HLSR` header but truncate the framed payload, so the body is
    // undecodable (corruption), not a foreign or unknown format.
    let mut bytes = sources_bytes(&backend, pending);
    bytes.truncate(12);
    put_sources(&backend, pending, bytes);

    let pipe = fresh_pipeline();
    // Even a re-drivable sink cannot rescue an undecodable body.
    let (mut hotlap, mut stream, commits, warning, metrics) = start(&backend, &pipe, true);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(warning.contains("corrupt or incomplete"), "{warning}");
    assert!(
        warning.contains(&format!("replaying from checkpoint {valid}")),
        "{warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
    assert_resumed(&backend, &pipe, valid);

    drain(&mut hotlap, &pipe.sources, &mut stream, 5);
    assert_eq!(zset_keys(&hotlap.snapshot("j").unwrap()), expected());
}
