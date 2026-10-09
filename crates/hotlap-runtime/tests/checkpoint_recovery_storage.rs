//! Recovery must propagate storage failures without discarding pending commit
//! evidence, cleaning up state or reopening any source.

#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery.rs"]
mod recovery;
#[path = "common/spy.rs"]
mod spy;
#[path = "common/recovery_storage_support.rs"]
mod support;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;

use fault::FaultBackend;
use recovery::{SharedBackend, rows};
use support::{TrackingBackend, seed_pending, seed_valid, spy_sources, start};

#[test]
fn promotion_publication_failure_is_storage_without_discard() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2);
    let track = TrackingBackend::new(backend.clone());
    let faulty = FaultBackend::new(track.clone());
    faulty.fail("put", b"checkpoint/2/valid", false);
    let (sources, spy) = spy_sources();

    let (result, metrics) = start(Box::new(faulty), &sources);
    let error = result
        .err()
        .expect("a storage publication failure must propagate");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(track.deletes(), 0, "nothing may be discarded");
    assert_eq!(spy.resumed(), 0, "no source may be reopened");
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), None);
    assert!(backend.get(b"checkpoint/2/commit").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);

    // A fresh restart over the recovered store promotes and resumes id 2.
    let (result, _) = start(Box::new(backend.clone()), &sources);
    assert!(result.is_ok(), "the recovered store must start");
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
}

#[test]
fn promotion_latest_failure_keeps_the_published_valid() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2);
    let track = TrackingBackend::new(backend.clone());
    let faulty = FaultBackend::new(track.clone());
    faulty.fail("put", b"checkpoint/latest", false);
    let (sources, spy) = spy_sources();

    let (result, _) = start(Box::new(faulty), &sources);
    let error = result.err().expect("a failed pointer write must propagate");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(track.deletes(), 0);
    assert_eq!(spy.resumed(), 0);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());

    let (result, _) = start(Box::new(backend.clone()), &sources);
    let (mut hotlap, _stream) = result.expect("a valid body must resume");
    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
}

#[test]
fn a_store_read_error_through_start_stops_before_sources() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let track = TrackingBackend::new(backend.clone());
    let faulty = FaultBackend::new(track.clone());
    faulty.fail("get", b"checkpoint/1/engine", false);
    let (sources, spy) = spy_sources();

    let (result, _) = start(Box::new(faulty), &sources);
    let error = result.err().expect("a store read error must propagate");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(track.deletes(), 0);
    assert_eq!(spy.resumed(), 0);
}

#[test]
fn a_store_list_error_through_start_deletes_nothing() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2);
    let track = TrackingBackend::new(backend.clone());
    let faulty = FaultBackend::new(track.clone());
    faulty.fail("list", b"checkpoint/", false);
    let (sources, spy) = spy_sources();

    let (result, _) = start(Box::new(faulty), &sources);
    let error = result.err().expect("a listing error must propagate");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(track.deletes(), 0);
    assert_eq!(spy.resumed(), 0);
    assert!(!backend.list(b"checkpoint/2/").unwrap().is_empty());
}
