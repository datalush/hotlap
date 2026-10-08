//! Recovery decisions around the durable commit-intent marker.
//!
//! `checkpoint/<id>/commit` is written after the body, before `Sink::commit`,
//! and removed after `mark_valid`, so a crash leaves a marker without `valid`.

#[path = "recovery_commit_marker/harness.rs"]
mod harness;
#[path = "common/recovery.rs"]
mod recovery;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::sink::SinkCapabilities;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use harness::{FakeSink, log, matching, seed_pending, seed_valid_one, sink};
use recovery::{ResumableSource, engine_with};

#[test]
fn commit_marker_is_durable_before_commit_and_removed_after_valid() {
    let backend = recovery::SharedBackend::default();
    let commits = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(false));
    let probe = Arc::new(FakeSink {
        capabilities: SinkCapabilities::Transactional,
        commits: Arc::clone(&commits),
        probe: Some((backend.clone(), 1, Arc::clone(&observed))),
    });
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(probe))]);

    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    let id = futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).unwrap();

    assert_eq!(id, 1);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert!(
        *observed.lock().unwrap(),
        "the marker must be durable before Sink::commit"
    );
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}

#[test]
fn recovery_promotes_a_redrivable_interrupted_commit() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = sink(SinkCapabilities::Idempotent);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    let checkpoint = match Recovery::inspect(&checkpointer, &matching()).unwrap() {
        RecoveryDecision::Promote(checkpoint) => checkpoint,
        other => panic!("expected Promote, got {other:?}"),
    };
    assert_eq!(checkpoint.id, 2);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "inspect must not commit");

    futures::executor::block_on(checkpointer.promote(2)).unwrap();

    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "commit must be re-driven"
    );
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert!(matches!(
        Recovery::inspect(&checkpointer, &matching()).unwrap(),
        RecoveryDecision::Resume(c) if c.id == 2
    ));
}

#[test]
fn recovery_discards_a_non_redrivable_interrupted_commit() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = sink(SinkCapabilities::AtLeastOnce);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    match Recovery::inspect(&checkpointer, &matching()).unwrap() {
        RecoveryDecision::Discard {
            pending, fallback, ..
        } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");

    checkpointer.discard_commit(2).unwrap();
    assert!(
        backend.list(b"checkpoint/2/").unwrap().is_empty(),
        "the discarded checkpoint must be removed"
    );
    assert_eq!(
        Recovery::load(&checkpointer, &matching())
            .unwrap()
            .unwrap()
            .id,
        1
    );
}

#[test]
fn recovery_without_a_marker_matches_the_newest_valid() {
    let backend = seed_valid_one();
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);

    match Recovery::inspect(&checkpointer, &matching()).unwrap() {
        RecoveryDecision::Resume(checkpoint) => assert_eq!(checkpoint.id, 1),
        other => panic!("expected Resume, got {other:?}"),
    }
}
