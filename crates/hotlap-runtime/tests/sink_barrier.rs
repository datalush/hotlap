//! Two-phase-commit sink coordination at the checkpoint barrier.

use std::sync::{Arc, Mutex};

use hotlap_connectors::sink::SinkCapabilities;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

#[path = "sink_barrier/harness.rs"]
mod harness;

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
async fn capture_failure_aborts_and_discards_the_checkpoint() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    let backend = MemBackend::failing();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let result = checkpointer.take(&engine(), &sources()).await;
    assert!(result.is_err(), "a failing write must surface the error");
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Prepare, Event::Abort],
        "a prepared sink must be aborted"
    );
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
async fn at_least_once_sink_is_not_coordinated() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::AtLeastOnce,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    checkpointer.take(&engine(), &sources()).await.unwrap();
    assert!(
        log.lock().unwrap().is_empty(),
        "at-least-once sinks are already visible and must not be coordinated"
    );
}

#[tokio::test]
async fn commit_failure_aborts_the_failed_and_remaining_prepared_sinks() {
    let log = events();
    let first = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    // The second fails its commit; it must be aborted along with the first.
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
        vec![Event::Prepare, Event::Commit, Event::Abort],
        "the sink whose commit failed must still be aborted"
    );
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}

#[tokio::test]
async fn idempotent_flush_failure_aborts_prepared_transactional_sinks() {
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
        vec![Event::Prepare, Event::Abort],
        "an idempotent flush failure must abort the prepared transactional sink"
    );
    assert_eq!(*idempotent_log.lock().unwrap(), vec![Event::Commit]);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}
