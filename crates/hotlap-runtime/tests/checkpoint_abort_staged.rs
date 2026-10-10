//! Durable abort evidence with real staged and published payloads.

#[path = "common/backend.rs"]
mod backend;
#[path = "checkpoint_abort_staged/support.rs"]
mod support;

use std::sync::Arc;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use backend::SharedBackend;
use support::{FaultBackend, PersistentTxn, Remote, engine_with_changes, sources, write_changes};

fn coordinated(sink: Arc<PersistentTxn>) -> SinkSync {
    SinkSync::sink_only(SharedSink::new(sink))
}

async fn engine_with_written_changes(sink: &PersistentTxn) -> hotlap::Hotlap {
    let (hotlap, changes) = engine_with_changes();
    write_changes(sink, changes).await;
    hotlap
}

fn recovery(backend: SharedBackend, remote: Arc<Remote>, redriable: bool) -> RecoveryDecision {
    let checkpointer = Checkpointer::new(Box::new(backend), 3).with_sinks(vec![coordinated(
        PersistentTxn::new(remote, false, redriable),
    )]);
    Recovery::inspect(&checkpointer, &sources()).unwrap()
}

#[tokio::test]
async fn capture_storage_failure_aborts_persisted_prepare_before_clearing_marker() {
    let durable = SharedBackend::default();
    let backend = FaultBackend::new(durable.clone(), false);
    let remote = Arc::new(Remote::default());
    let txn = PersistentTxn::new(remote.clone(), false, false);
    let hotlap = engine_with_written_changes(&txn).await;
    let mut checkpointer =
        Checkpointer::new(Box::new(backend), 3).with_sinks(vec![coordinated(txn)]);

    let error = checkpointer
        .take(&hotlap, &sources())
        .await
        .expect_err("the injected engine-body write must fail");

    assert!(matches!(error, ConnectorError::Storage(_)));
    assert!(
        remote.staged().is_empty(),
        "confirmed abort must roll back the payload"
    );
    assert!(remote.committed().is_empty());
    assert_eq!(checkpointer.state(), CheckpointState::Failed);
    assert_eq!(durable.get(b"checkpoint/1/prepare").unwrap(), None);
    assert!(checkpointer.take(&hotlap, &sources()).await.is_err());
}

#[tokio::test]
async fn failed_abort_keeps_prepare_marker_and_rejects_a_fresh_writer() {
    let durable = SharedBackend::default();
    let backend = FaultBackend::new(durable.clone(), false);
    let remote = Arc::new(Remote::default());
    let txn = PersistentTxn::new(remote.clone(), true, true);
    let hotlap = engine_with_written_changes(&txn).await;
    let mut checkpointer =
        Checkpointer::new(Box::new(backend), 3).with_sinks(vec![coordinated(txn)]);

    let error = checkpointer
        .take(&hotlap, &sources())
        .await
        .expect_err("the abort error must be surfaced");

    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert_eq!(
        remote.staged(),
        vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
    );
    assert!(durable.get(b"checkpoint/1/prepare").unwrap().is_some());
    assert!(matches!(
        recovery(durable, remote.clone(), true),
        RecoveryDecision::Reject { pending: 1, .. }
    ));
    assert!(remote.committed().is_empty());
    assert_eq!(remote.commit_calls(), 0);
}

#[tokio::test]
async fn clear_failure_after_confirmed_abort_never_redrives_aborted_payload() {
    let durable = SharedBackend::default();
    let backend = FaultBackend::new(durable.clone(), true);
    let remote = Arc::new(Remote::default());
    let txn = PersistentTxn::new(remote.clone(), false, true);
    let hotlap = engine_with_written_changes(&txn).await;
    let mut checkpointer =
        Checkpointer::new(Box::new(backend), 3).with_sinks(vec![coordinated(txn)]);

    let error = checkpointer
        .take(&hotlap, &sources())
        .await
        .expect_err("the marker clear error must be surfaced");

    assert!(matches!(error, ConnectorError::Storage(_)));
    assert!(
        remote.staged().is_empty(),
        "abort completed before marker cleanup"
    );
    assert!(durable.get(b"checkpoint/1/prepare").unwrap().is_some());
    assert!(matches!(
        recovery(durable, remote.clone(), true),
        RecoveryDecision::Reject { pending: 1, .. }
    ));
    assert!(remote.committed().is_empty());
}

#[tokio::test]
async fn prepare_failure_surfaces_abort_error_and_retains_durable_evidence() {
    let durable = SharedBackend::default();
    let first_remote = Arc::new(Remote::default());
    let failed_remote = Arc::new(Remote::default());
    let first = PersistentTxn::new(first_remote.clone(), true, true);
    let hotlap = engine_with_written_changes(&first).await;
    let second = SinkSync::sink_only(SharedSink::new(PersistentTxn::failing_prepare(
        failed_remote.clone(),
        false,
        false,
    )));
    let mut checkpointer = Checkpointer::new(Box::new(durable.clone()), 3)
        .with_sinks(vec![coordinated(first), second]);

    let error = checkpointer
        .take(&hotlap, &sources())
        .await
        .expect_err("abort failure must override the original prepare error");

    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert_eq!(
        first_remote.staged(),
        vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
    );
    assert!(
        failed_remote.staged().is_empty(),
        "also abort the failing participant"
    );
    assert!(durable.get(b"checkpoint/1/prepare").unwrap().is_some());
    assert!(matches!(
        recovery(durable, first_remote.clone(), true),
        RecoveryDecision::Reject { pending: 1, .. }
    ));
}

#[tokio::test]
async fn stalled_abort_is_bounded_and_keeps_the_prepare_marker() {
    let durable = SharedBackend::default();
    let backend = FaultBackend::new(durable.clone(), false);
    let remote = Arc::new(Remote::default());
    let txn = PersistentTxn::stalled_abort(remote.clone());
    let hotlap = engine_with_written_changes(&txn).await;
    let mut checkpointer =
        Checkpointer::new(Box::new(backend), 3).with_sinks(vec![coordinated(txn)]);

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        checkpointer.take(&hotlap, &sources()),
    )
    .await
    .expect("abort must not block the checkpoint indefinitely");

    assert!(matches!(result, Err(ConnectorError::Infrastructure(_))));
    assert_eq!(
        remote.staged(),
        vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
    );
    assert!(durable.get(b"checkpoint/1/prepare").unwrap().is_some());
}
