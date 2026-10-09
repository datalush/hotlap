//! Two-phase-commit sink coordination: happy paths.
//!
//! Failure paths live in `sink_barrier::failures`, so each file stays small.

#[path = "sink_barrier/failures.rs"]
mod failures;
#[path = "sink_barrier/harness.rs"]
pub mod harness;

use std::sync::Arc;

use hotlap_connectors::sink::SinkCapabilities;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use harness::{Event, FakeSink, MemBackend, engine, events, sources};

#[tokio::test]
async fn transactional_sink_prepares_then_commits_before_valid() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let id = checkpointer
        .take(&engine(), &sources())
        .await
        .expect("checkpoint");
    assert_eq!(id, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Prepare, Event::Commit],
        "commit must follow prepare and precede validity"
    );

    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), Some(1));
    assert!(
        reader.read(1).is_ok(),
        "checkpoint must be valid after commit"
    );
}

#[tokio::test]
async fn idempotent_sink_is_flushed_but_not_prepared() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Idempotent,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    checkpointer.take(&engine(), &sources()).await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Commit],
        "idempotent sinks flush on commit without prepare"
    );
}

#[tokio::test]
async fn at_least_once_sink_is_flushed_before_valid() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::AtLeastOnce,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    checkpointer.take(&engine(), &sources()).await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Commit],
        "at-least-once sinks must confirm their flush before the checkpoint is valid"
    );
}
