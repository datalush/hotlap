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

/// The pending body is a 4-record checkpoint; the fallback is a 3-record one.
const PENDING_EVENTS: usize = 4;
const PENDING_OFFSET: i64 = 4;
const PENDING_ROWS: [&[i64]; 3] = [&[1, 2], &[2, 2], &[3, 1]];

fn pending_rows() -> Vec<Vec<i64>> {
    PENDING_ROWS.iter().map(|row| row.to_vec()).collect()
}

#[test]
fn promotion_publication_failure_is_storage_without_discard() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2, PENDING_EVENTS);
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

    // A fresh restart over the recovered store promotes and resumes id 2, which
    // must restore the pending state, not the fallback.
    let (result, _) = start(Box::new(backend.clone()), &sources);
    let (mut hotlap, _stream) = result.expect("the recovered store must start");
    assert_eq!(
        backend.get(b"checkpoint/2/valid").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(rows(&hotlap.snapshot("c").unwrap()), pending_rows());
    assert_eq!(spy.offset(), Some(PENDING_OFFSET));
}

#[test]
fn promotion_latest_failure_keeps_the_published_valid() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2, PENDING_EVENTS);
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
    assert_eq!(rows(&hotlap.snapshot("c").unwrap()), pending_rows());
    assert_eq!(spy.offset(), Some(PENDING_OFFSET));
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
    seed_pending(&backend, 1, 2, PENDING_EVENTS);
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
