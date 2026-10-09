//! A checkpoint that fails after the sinks were touched blocks further attempts.
//!
//! The engine and source offsets are not rolled back with the sinks, so the
//! runtime must stop ingesting until a restart resolves the attempt. The
//! durable body, commit marker and staged sink state are preserved as evidence.

#[path = "common/fault.rs"]
mod fault;
#[path = "checkpoint_uncertain/harness.rs"]
mod harness;
#[path = "checkpoint_uncertain/support.rs"]
mod support;

use std::sync::Arc;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sources::Sources;

use fault::FaultBackend;
use harness::{DurableSink, Event, coord};
use support::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

fn take(
    checkpointer: &mut Checkpointer,
    engine: &hotlap::Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    futures::executor::block_on(checkpointer.take(engine, sources))
}

/// A drained engine plus the pipeline whose sources it captured.
fn ready() -> (hotlap::Hotlap, hotlap_runtime::runtime::pipeline::Pipeline) {
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    (engine, pipe)
}

#[test]
fn a_commit_failure_preserves_the_pending_and_blocks_retries() {
    let backend = SharedBackend::default();
    let stage = SharedBackend::default();
    let remote = SharedBackend::default();
    let first = Arc::new(DurableSink::new("a", stage.clone(), remote.clone(), true));
    let second = Arc::new(DurableSink::failing(
        "b",
        stage.clone(),
        remote.clone(),
        true,
        1,
    ));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![coord(first.clone()), coord(second.clone())]);
    let (mut engine, pipe) = ready();

    assert!(take(&mut checkpointer, &engine, &pipe.sources).is_err());

    // A participant may have confirmed, so nothing is rolled back: the failing
    // commit's staged value survives and no abort was recorded.
    assert_eq!(second.events(), vec![Event::Prepare, Event::Commit]);
    assert_eq!(second.staged().as_deref(), Some(b"b".as_ref()));
    assert_eq!(
        backend.get(b"checkpoint/1/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
    // The first participant's confirmed delivery is visible on the remote.
    assert_eq!(first.delivered().as_deref(), Some(b"a".as_ref()));

    assert!(
        take(&mut checkpointer, &engine, &pipe.sources).is_err(),
        "a commit failure must block further attempts on this runtime"
    );
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
}

#[test]
fn a_capture_failure_after_prepare_blocks_retries() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    // The body is written, but the commit marker write is reported as failed.
    faulty.fail("put", b"checkpoint/1/commit", false);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    let (engine, pipe) = ready();

    assert!(take(&mut checkpointer, &engine, &pipe.sources).is_err());
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert!(
        take(&mut checkpointer, &engine, &pipe.sources).is_err(),
        "a capture failure after prepare must block a retry"
    );
}
