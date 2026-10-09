//! A sink whose writer only queues in memory cannot complete an interrupted
//! commit after a crash; recovery must replay and re-deliver through the sink.

#[path = "sink_redrive_recovery/flow.rs"]
mod flow;
#[path = "sink_redrive_recovery/harness.rs"]
mod harness;

use harness::{RemoteStore, SharedBackend};
use hotlap::state::StateBackend;

#[tokio::test]
async fn a_fresh_volatile_writer_cannot_promote_and_replay_redelivers() {
    let remote = RemoteStore::default();
    let backend = SharedBackend::default();
    let delivered = flow::crash_mid_commit(&backend, &remote).await;

    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap().as_deref(),
        Some(b"1".as_slice()),
        "the pending commit marker is durable"
    );
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_none());
    assert_eq!(
        remote.rows(),
        delivered,
        "the pending row was never delivered"
    );
    assert!(
        !remote.rows().contains(&vec![3, 1]),
        "the fresh writer must not already hold the pending row"
    );

    let offsets = flow::replay_after_crash(&backend, &remote).await;
    assert_eq!(offsets, vec![3], "replay must start at the fallback offset");
    assert!(
        remote.rows().contains(&vec![3, 1]),
        "the pending row must be redelivered, got {:?}",
        remote.rows()
    );
}
