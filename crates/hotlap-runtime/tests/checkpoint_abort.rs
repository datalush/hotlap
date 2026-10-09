//! Ordering between the durable commit marker and a sink abort.
//!
//! A commit-phase failure keeps the marker and never rolls a sink back, because
//! a participant may already have confirmed. A capture failure that wrote no
//! durable body only aborts after the marker is durably cleared; when clearing
//! fails the sinks are kept and the failure surfaces as storage.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/fault.rs"]
mod fault;
#[path = "checkpoint_abort/harness.rs"]
mod harness;

use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};

use backend::SharedBackend;
use fault::FaultBackend;
use harness::{Event, healthy_engine, poisoned_engine, sink, sources};

#[tokio::test]
async fn a_commit_failure_preserves_the_marker_and_does_not_abort() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![sink(&events, true)]);

    let result = checkpointer.take(&healthy_engine(), &sources()).await;
    assert!(result.is_err(), "the failed commit must surface");
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Prepare, Event::Commit],
        "a sink whose commit may have confirmed must not be rolled back"
    );
    assert!(
        backend.get(b"checkpoint/1/commit").unwrap().is_some(),
        "the commit marker must survive for recovery"
    );
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
}

#[tokio::test]
async fn a_prepared_capture_failure_clears_before_a_safe_abort() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![sink(&events, false)]);

    let result = checkpointer.take(&poisoned_engine(), &sources()).await;
    assert!(result.is_err(), "a poisoned engine must fail the capture");
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Prepare, Event::Abort],
        "the marker was cleared, so the prepared sink is safely aborted"
    );
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert!(
        checkpointer
            .take(&healthy_engine(), &sources())
            .await
            .is_err(),
        "a failed attempt must block later attempts on the same runtime"
    );
}

#[tokio::test]
async fn a_marker_clear_failure_is_storage_and_keeps_the_prepared_sink() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/commit", false);
    let mut checkpointer =
        Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN).with_sinks(vec![sink(&events, false)]);

    let result = checkpointer.take(&poisoned_engine(), &sources()).await;
    let error = result.expect_err("a failed clear must surface");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Prepare],
        "no abort may run when the marker could not be cleared"
    );
    assert!(
        checkpointer
            .take(&healthy_engine(), &sources())
            .await
            .is_err(),
        "the ambiguous clear must block later attempts"
    );
}
