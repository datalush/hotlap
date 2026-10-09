//! Reservation and marker durability under ambiguous store acknowledgements.
//!
//! A write that persists and then reports failure must not let the same
//! checkpointer retry the id, must not erase the durable commit marker, and a
//! stale-marker cleanup failure must surface as a typed storage error.

#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery.rs"]
mod recovery;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::InputStream;

use fault::FaultBackend;
use recovery::{Dataset, ResumableSource, SharedBackend, engine_with, rows, take};

/// A log with retention keeping every record.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// Drain `n` events into `engine`, returning the pipe's stream.
fn feed(engine: &mut hotlap::Hotlap, pipe: &Pipeline, n: usize) -> InputStream {
    let mut stream = pipe.sources.stream().unwrap();
    recovery::drain(engine, &pipe.sources, &mut stream, n);
    stream
}

#[test]
fn reserve_after_ack_failure_advances_and_persists() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    let mut checkpointer = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let _stream = feed(&mut engine, &pipe, 2);
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 1]]
    );

    // The reservation is durable, then the acknowledgement is lost.
    faulty.fail("put", b"checkpoint/reserved", true);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert_eq!(
        backend.get(b"checkpoint/reserved").unwrap(),
        Some(1u64.to_le_bytes().to_vec())
    );

    // The same checkpointer must not retry id 1, and a fresh one must honour
    // the persisted floor.
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 2);
    let mut fresh = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert_eq!(take(&mut fresh, &engine, &pipe.sources), 3);
}

#[test]
fn a_commit_marker_after_ack_failure_is_preserved() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    let mut checkpointer = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));

    // The body and commit marker persist, but the marker write is reported as
    // failed; the marker is pending-commit evidence and must survive.
    faulty.fail("put", b"checkpoint/1/commit", true);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert_eq!(
        backend.get(b"checkpoint/1/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 2);
}

#[test]
fn a_prune_delete_after_ack_failure_keeps_the_published_id() {
    let backend = SharedBackend::default();
    let mut seeder = Checkpointer::new(Box::new(backend.clone()), 1);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    assert_eq!(take(&mut seeder, &engine, &pipe.sources), 1);

    // Publishing id 2 succeeds; pruning id 1 deletes durably then fails.
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/engine", true);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), 1);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(
        backend.get(b"checkpoint/latest").unwrap(),
        Some(2u64.to_le_bytes().to_vec())
    );
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 3);
}

#[test]
fn a_stale_marker_cleanup_failure_is_storage() {
    let backend = SharedBackend::default();
    let mut seeder = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    take(&mut seeder, &engine, &pipe.sources);
    let mut writer = backend.clone();
    writer.put(b"checkpoint/1/commit", b"1".to_vec()).unwrap();

    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/commit", false);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    let error = checkpointer
        .sweep_stale_commits()
        .expect_err("a cleanup failure must surface");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
}
