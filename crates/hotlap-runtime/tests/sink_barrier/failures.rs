//! Failure paths of the two-phase-commit barrier.
//!
//! A commit-phase failure keeps the marker and never rolls a sink back; a
//! pre-commit failure aborts the prepared sinks after clearing the marker.

use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::sink::SinkCapabilities;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use crate::harness::{Event, FakeSink, MemBackend, engine, events, sources};

#[tokio::test]
async fn capture_storage_failure_preserves_commit_evidence() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    let backend = MemBackend::failing_writes();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(result.is_err(), "a failing write must surface the error");
    // An ambiguous storage write may have persisted part of the body or the
    // marker, so the prepared sinks are left untouched instead of rolled back
    // and the evidence is kept for an uncertain commit.
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Prepare],
        "a storage write failure must not abort the prepared sink"
    );
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(
        reader.latest().unwrap(),
        None,
        "no checkpoint may become valid"
    );
}

#[tokio::test]
async fn prepare_failure_aborts_the_already_prepared_sinks() {
    let log = events();
    let first = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    // The second sink fails prepare; the first, already prepared, is aborted.
    let second_log = Arc::new(Mutex::new(Vec::new()));
    let second = SharedSink::new(Arc::new(FakeSink::failing_prepare(second_log.clone())));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(first),
        SinkSync::sink_only(second),
    ]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(result.is_err());
    assert_eq!(*log.lock().unwrap(), vec![Event::Prepare, Event::Abort]);
    assert_eq!(*second_log.lock().unwrap(), vec![Event::Prepare]);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}

#[tokio::test]
async fn at_least_once_flush_failure_keeps_no_valid_checkpoint() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::failing_commit(
        SinkCapabilities::AtLeastOnce,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(
        result.is_err(),
        "a failed delivery must not publish validity"
    );
    assert_eq!(*log.lock().unwrap(), vec![Event::Commit]);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(
        reader.latest().unwrap(),
        None,
        "an unconfirmed append must not certify the offsets"
    );
}

#[tokio::test]
async fn commit_failure_preserves_the_marker_and_does_not_abort() {
    let log = events();
    let first = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    // The second fails its commit; a participant may already have confirmed, so
    // no sink may be rolled back and the marker and body stay for recovery.
    let second_log = events();
    let second = SharedSink::new(Arc::new(FakeSink::failing_commit(
        SinkCapabilities::Transactional,
        second_log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(first),
        SinkSync::sink_only(second),
    ]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(result.is_err());
    assert_eq!(*log.lock().unwrap(), vec![Event::Prepare, Event::Commit]);
    assert_eq!(
        *second_log.lock().unwrap(),
        vec![Event::Prepare, Event::Commit],
        "a failed commit must not roll the sink back"
    );
    assert!(
        backend.get(b"checkpoint/1/commit").unwrap().is_some(),
        "the commit marker must survive for recovery"
    );
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}

#[tokio::test]
async fn idempotent_flush_failure_preserves_the_prepared_transactional_sink() {
    let transactional_log = events();
    let transactional = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        transactional_log.clone(),
    )));
    let idempotent_log = events();
    let idempotent = SharedSink::new(Arc::new(FakeSink::failing_commit(
        SinkCapabilities::Idempotent,
        idempotent_log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(transactional),
        SinkSync::sink_only(idempotent),
    ]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(result.is_err());
    assert_eq!(
        *transactional_log.lock().unwrap(),
        vec![Event::Prepare],
        "a non-reversible flush failure must not roll the transactional sink back"
    );
    assert_eq!(*idempotent_log.lock().unwrap(), vec![Event::Commit]);
    assert!(backend.get(b"checkpoint/1/commit").unwrap().is_some());
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}
