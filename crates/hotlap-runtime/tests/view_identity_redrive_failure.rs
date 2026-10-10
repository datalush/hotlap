//! Redrive errors preserve pending evidence when fallback identity is unsafe.

#[path = "view_identity_redrive_failure/fixture.rs"]
mod fixture;
#[path = "common/view_identity_pipeline.rs"]
pub mod support;

use std::sync::atomic::Ordering;

use arrow::array::Int64Array;
use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_runtime::runtime::handle::EngineHandle;

use fixture::{pipeline, redrive_sink, seeded_compatible_registries, seeded_registries};
use support::rows;

#[test]
fn incompatible_fallback_keeps_pending_marker_after_redrive_error() {
    let (backend, dataset) = seeded_registries();
    let (sink, remote) = redrive_sink();
    let (pipeline, spy) = pipeline(&backend, dataset, Some(sink.clone()));

    let result = EngineHandle::start(pipeline);
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert!(backend.get(b"checkpoint/2/commit").unwrap().is_some());
    assert!(backend.get(b"checkpoint/2/engine").unwrap().is_some());
    assert!(remote.lock().unwrap().staged);
    assert!(!remote.lock().unwrap().committed);
    assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
    assert_eq!(spy.resumed(), 0);
}

#[test]
fn public_pipeline_promotes_pending_registry_and_resumes_its_offset() {
    let (backend, dataset) = seeded_registries();
    let (pipeline, spy) = pipeline(&backend, dataset, None);
    let handle = EngineHandle::start(pipeline).expect("promote selected pending checkpoint");

    assert_snapshot(&handle, "a", 1);
    assert_snapshot(&handle, "b", 2);
    assert_eq!(spy.offset(), Some(4));
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    handle.shutdown().unwrap();
}

#[test]
fn compatible_fallback_is_discarded_and_replayed_after_redrive_error() {
    let (backend, dataset) = seeded_compatible_registries();
    let (sink, _) = redrive_sink();
    let (pipeline, spy) = pipeline(&backend, dataset, Some(sink));

    let handle = EngineHandle::start(pipeline).expect("replay from compatible fallback");
    assert_eq!(spy.offset(), Some(3));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), None);
    handle.shutdown().unwrap();
}

fn assert_snapshot(handle: &EngineHandle, view: &str, expected: i64) {
    let snapshot = handle.snapshot(view).unwrap();
    assert_eq!(rows(&snapshot), vec![vec![expected]]);
    assert_eq!(
        snapshot
            .diff
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2]
    );
}
