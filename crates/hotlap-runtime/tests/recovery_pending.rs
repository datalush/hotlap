//! Recovery around a pending commit body: tolerating corruption, incomplete
//! bodies and stale markers without aborting startup.

#[path = "recovery_pending/harness.rs"]
mod harness;
#[path = "common/recovery.rs"]
mod recovery;

use std::sync::Mutex;

use hotlap::state::StateBackend;
use hotlap_connectors::sink::SinkCapabilities;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};

use harness::{capability, delete_checkpoint, fallback_id, log, seed_pending, seed_valid_one};
use recovery::{ResumableSource, engine_with, rows, sources};

#[test]
fn a_transactional_pending_is_rejected_not_replayed() {
    let backend = seed_valid_one(vec![capability(
        SinkCapabilities::Transactional,
        "test/recovery-pending/sink/transactional",
        "transactional-output",
    )]);
    seed_pending(&backend, 1, 2);
    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![capability(
            SinkCapabilities::Transactional,
            "test/recovery-pending/sink/transactional",
            "transactional-output",
        )]);

    // A transactional sink is not re-drivable by default, and replaying could
    // duplicate a commit it already confirmed, so recovery must refuse instead.
    match Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log()))).unwrap() {
        RecoveryDecision::Reject { pending, reason } => {
            assert_eq!(pending, 2);
            assert!(reason.contains("transactional"), "reason: {reason}");
        }
        other => panic!("expected Reject, got {other:?}"),
    }
}

#[test]
fn corrupt_pending_body_falls_back_to_a_valid_predecessor() {
    let backend = seed_valid_one(vec![capability(
        SinkCapabilities::AtLeastOnce,
        "test/recovery-pending/sink/at-least-once-fallback",
        "fallback-output",
    )]);
    seed_pending(&backend, 1, 2);
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/2/engine", b"not-a-snapshot".to_vec())
        .unwrap();

    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![capability(
            SinkCapabilities::AtLeastOnce,
            "test/recovery-pending/sink/at-least-once-fallback",
            "fallback-output",
        )]);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    assert!(writer.get(b"checkpoint/2/engine").unwrap().is_none());
    let warning = signal.lock().unwrap().clone().expect("explicit signal");
    assert!(
        warning.contains("checkpoint 1") && warning.contains("corrupt or incomplete"),
        "must name the fallback and the corrupt body: {warning}"
    );
}

#[test]
fn a_commit_marker_without_a_participant_manifest_is_fatal_and_preserved() {
    let backend = seed_valid_one(vec![capability(
        SinkCapabilities::Idempotent,
        "test/recovery-pending/sink/idempotent",
        "idempotent-output",
    )]);
    let mut writer = backend.clone();
    let engine = writer.get(b"checkpoint/1/engine").unwrap().unwrap();
    writer.put(b"checkpoint/2/engine", engine).unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();

    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![capability(
            SinkCapabilities::Idempotent,
            "test/recovery-pending/sink/idempotent",
            "idempotent-output",
        )]);
    let error = Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("missing participant identity must fail closed");
    assert!(
        error.to_string().contains("no participant manifest"),
        "{error}"
    );
    assert!(writer.get(b"checkpoint/2/engine").unwrap().is_some());
    assert_eq!(
        writer.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(writer.get(b"checkpoint/2/valid").unwrap(), None);
}

#[test]
fn a_body_without_a_marker_is_not_a_pending_commit() {
    let backend = seed_valid_one(vec![]);
    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    match Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log()))).unwrap() {
        RecoveryDecision::Resume(checkpoint) => assert_eq!(checkpoint.id, 1),
        other => panic!("expected Resume, got {other:?}"),
    }
}

#[test]
fn both_valid_and_commit_resumes_and_sweeps_the_marker() {
    let backend = seed_valid_one(vec![]);
    let mut writer = backend.clone();
    writer.put(b"checkpoint/1/commit", b"1".to_vec()).unwrap();

    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert_eq!(checkpointer.latest().unwrap(), Some(1));
    assert_eq!(
        writer.get(b"checkpoint/1/commit").unwrap(),
        None,
        "the stale marker must be swept"
    );
    assert!(
        signal.lock().unwrap().is_none(),
        "a resume is not a discard"
    );
}

#[test]
fn discard_commit_is_idempotent() {
    let backend = seed_valid_one(vec![]);
    seed_pending(&backend, 1, 2);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);

    checkpointer.discard_commit(2).unwrap();
    checkpointer.discard_commit(2).unwrap();
    assert!(backend.list(b"checkpoint/2/").unwrap().is_empty());
    assert_eq!(fallback_id(&backend), 1);
}

#[test]
fn discarding_without_a_fallback_starts_clean() {
    let backend = seed_valid_one(vec![capability(
        SinkCapabilities::AtLeastOnce,
        "test/recovery-pending/sink/clean-start",
        "clean-output",
    )]);
    seed_pending(&backend, 1, 2);
    delete_checkpoint(&backend, 1);

    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![capability(
            SinkCapabilities::AtLeastOnce,
            "test/recovery-pending/sink/clean-start",
            "clean-output",
        )]);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert!(rows(&hotlap.snapshot("c").unwrap()).is_empty());
    let warning = signal.lock().unwrap().clone().expect("explicit signal");
    assert!(
        warning.contains("starting clean"),
        "not a replay: {warning}"
    );
}
