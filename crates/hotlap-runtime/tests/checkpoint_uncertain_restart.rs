//! Restart resolution of a checkpoint interrupted after the sinks were touched.
//!
//! A re-drivable pending commit is promoted and its staged weighted changes are
//! delivered exactly once, so the remote bags equal the restored engine views.
//! A transactional sink that cannot be re-driven is rejected with the marker
//! preserved, because replaying could duplicate an already-committed transaction.

#[path = "checkpoint_uncertain/codec.rs"]
pub mod codec;
#[path = "checkpoint_uncertain/harness.rs"]
pub mod harness;
#[path = "checkpoint_uncertain_restart/setup.rs"]
mod setup;
#[path = "common/spy.rs"]
pub mod spy;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;
#[path = "checkpoint_uncertain/txn.rs"]
pub mod txn;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;

use setup::{interrupt, restart};
use support::{COUNT, ROWS, bag};

#[test]
fn a_restart_promotes_and_matches_the_restored_views() {
    let fixture = interrupt();
    let restarted = restart(&fixture, true);
    let mut hotlap = restarted
        .hotlap
        .expect("a re-drivable commit must be promoted");

    assert_eq!(
        fixture.remote_a.snapshot(),
        bag(&hotlap.snapshot(ROWS).unwrap())
    );
    assert_eq!(
        fixture.remote_b.snapshot(),
        bag(&hotlap.snapshot(COUNT).unwrap())
    );
    assert_eq!(
        restarted.spy.offset(),
        Some(4),
        "the applied offset must be resumed"
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/valid")
            .unwrap()
            .is_some()
    );
    assert_eq!(fixture.backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert_eq!(
        restarted.metrics.snapshot().get("checkpoints_discarded"),
        None
    );
    assert!(
        restarted.signal.lock().unwrap().is_none(),
        "a promotion is not a discard"
    );
}

#[test]
fn a_transactional_non_redrivable_pending_is_rejected() {
    let fixture = interrupt();
    let before_a = fixture.remote_a.snapshot();
    let before_b = fixture.remote_b.snapshot();
    let restarted = restart(&fixture, false);

    let error = restarted
        .hotlap
        .err()
        .expect("a transactional pending commit must be rejected");
    assert!(
        matches!(error, ConnectorError::Infrastructure(_)),
        "got {error:?}"
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/commit")
            .unwrap()
            .is_some(),
        "the marker must be preserved for a manual decision"
    );
    assert_eq!(fixture.backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(
        restarted.metrics.snapshot().get("checkpoints_discarded"),
        None,
        "nothing may be silently discarded"
    );
    assert_eq!(fixture.remote_a.snapshot(), before_a, "no replay of view a");
    assert_eq!(fixture.remote_b.snapshot(), before_b, "no replay of view b");
    assert_eq!(
        restarted.spy.resumed(),
        0,
        "rejection must not reopen the sources"
    );
}
