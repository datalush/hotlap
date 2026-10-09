//! Restart resolution of a checkpoint interrupted after the sinks were touched.
//!
//! A re-drivable pending commit is promoted and its staged weighted changes
//! are installed exactly once, so the remote bags equal the restored engine
//! views. A transactional sink that cannot be re-driven, or whose body cannot
//! be decoded, is rejected with the evidence preserved: replaying could
//! duplicate an already-committed transaction.

#[path = "checkpoint_uncertain/codec.rs"]
pub mod codec;
#[path = "checkpoint_uncertain_restart/setup.rs"]
mod setup;
#[path = "common/spy.rs"]
pub mod spy;
#[path = "checkpoint_uncertain/store.rs"]
pub mod store;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;
#[path = "checkpoint_uncertain/txn.rs"]
pub mod txn;

use hotlap::Hotlap;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;

use setup::{Fail, Fixture, interrupt, restart};
use support::{COUNT, ROWS, bag};

/// Both remote bags equal the restored engine views exactly.
fn assert_matches(fixture: &Fixture, hotlap: &mut Hotlap) {
    assert_eq!(
        fixture.store.remote_bag(ROWS),
        bag(&hotlap.snapshot(ROWS).unwrap())
    );
    assert_eq!(
        fixture.store.remote_bag(COUNT),
        bag(&hotlap.snapshot(COUNT).unwrap())
    );
}

#[test]
fn a_restart_promotes_and_matches_the_restored_views() {
    let fixture = interrupt(Fail::Before);
    let restarted = restart(&fixture, true);
    let mut hotlap = restarted.hotlap.expect("a re-drivable commit promotes");
    assert_matches(&fixture, &mut hotlap);
    assert_eq!(restarted.spy.offset(), Some(4));
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
    let fixture = interrupt(Fail::Before);
    let restarted = restart(&fixture, false);
    let error = restarted
        .hotlap
        .err()
        .expect("a transactional pending commit must be rejected");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/commit")
            .unwrap()
            .is_some()
    );
    assert_eq!(fixture.backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(
        restarted.metrics.snapshot().get("checkpoints_discarded"),
        None
    );
    assert_eq!(restarted.spy.resumed(), 0);
}

#[test]
fn a_corrupt_transactional_pending_is_rejected() {
    let fixture = interrupt(Fail::Before);
    // The current-format body becomes incomplete after a participant committed.
    let mut writer = fixture.backend.clone();
    writer.delete(b"checkpoint/2/sources").unwrap();

    let restarted = restart(&fixture, true);
    let error = restarted
        .hotlap
        .err()
        .expect("an undecodable pending over a transactional sink is rejected");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/commit")
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/engine")
            .unwrap()
            .is_some(),
        "the evidence must not be deleted"
    );
    assert_eq!(restarted.spy.resumed(), 0);
}

#[test]
fn a_failed_promote_over_a_transactional_sink_preserves_the_pending() {
    let fixture = interrupt(Fail::Before);
    fixture.store.fail_before(COUNT, 1);
    let restarted = restart(&fixture, true);
    let error = restarted
        .hotlap
        .err()
        .expect("a non-storage promote failure must not discard");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/commit")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        restarted.metrics.snapshot().get("checkpoints_discarded"),
        None
    );
    assert_eq!(restarted.spy.resumed(), 0);

    // With the injected fault consumed, a second restart promotes safely.
    let retry = restart(&fixture, true);
    let mut hotlap = retry.hotlap.expect("the preserved pending must promote");
    assert_matches(&fixture, &mut hotlap);
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/valid")
            .unwrap()
            .is_some()
    );
}

#[test]
fn an_ack_lost_after_atomic_commit_redrives_as_a_noop() {
    let fixture = interrupt(Fail::After);
    let restarted = restart(&fixture, true);
    let mut hotlap = restarted.hotlap.expect("promotion is a no-op redrive");
    // A duplicated commit would double the bags, so exact equality proves the
    // already-installed transaction was not re-applied.
    assert_matches(&fixture, &mut hotlap);
    assert!(
        fixture
            .backend
            .get(b"checkpoint/2/valid")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        restarted.metrics.snapshot().get("checkpoints_discarded"),
        None
    );
}
