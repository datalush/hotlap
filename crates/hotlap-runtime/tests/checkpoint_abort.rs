//! Ordering between the durable commit marker and a sink abort.
//!
//! A commit-phase failure keeps the marker and never rolls a sink back, because
//! a participant may already have confirmed, and the state is commit-uncertain.
//! A capture failure that wrote no durable body only aborts after the marker is
//! durably cleared; when clearing fails the sinks are kept and the failure
//! surfaces as storage.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/fault.rs"]
mod fault;
#[path = "checkpoint_abort/harness.rs"]
pub mod harness;

use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};

use backend::SharedBackend;
use fault::FaultBackend;
use harness::{Event, healthy_engine, poisoned_engine, sink, sources};

#[tokio::test]
async fn a_commit_failure_is_commit_uncertain_and_does_not_abort() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![sink(&events, true)]);

    let error = checkpointer
        .take(&healthy_engine(), &sources())
        .await
        .expect_err("the failed commit must surface");

    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
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
async fn a_precommit_capture_abort_is_failed() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![sink(&events, false)]);

    let error = checkpointer
        .take(&poisoned_engine(), &sources())
        .await
        .expect_err("a poisoned engine must fail the capture");

    assert!(
        matches!(error, ConnectorError::Infrastructure(_)),
        "got {error:?}"
    );
    assert_eq!(checkpointer.state(), CheckpointState::Failed);
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
async fn a_clear_failure_after_confirmed_abort_keeps_evidence() {
    let backend = SharedBackend::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/prepare", false);
    let mut checkpointer =
        Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN).with_sinks(vec![sink(&events, false)]);

    let error = checkpointer
        .take(&poisoned_engine(), &sources())
        .await
        .expect_err("a failed clear must surface");

    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
    assert!(backend.get(b"checkpoint/1/prepare").unwrap().is_some());
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Prepare, Event::Abort],
        "a confirmed abort precedes the failed marker clear"
    );
    assert!(
        checkpointer
            .take(&healthy_engine(), &sources())
            .await
            .is_err(),
        "the ambiguous clear must block later attempts"
    );
}
