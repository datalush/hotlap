//! Durable marker handling when it is observable: an ambiguous marker write and
//! a clear failure over an already published checkpoint.
//!
//! A commit-marker write with an ambiguous acknowledgement can be promoted; a
//! clear failure over a published checkpoint leaves a redundant marker to sweep.

#[path = "common/fault.rs"]
mod fault;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;

use std::sync::{Arc, Mutex};

use hotlap::Hotlap;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::Sources;

use fault::FaultBackend;
use support::{Dataset, ResumableSource, SharedBackend, engine_for, sources_of};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

fn prepared(source: ResumableSource) -> (Hotlap, Pipeline) {
    engine_for(sources_of(Arc::new(source)))
}

fn take(
    checkpointer: &mut Checkpointer,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    futures::executor::block_on(checkpointer.take(engine, sources))
}

/// Run recovery over `backend` and return whether it started.
fn recover(backend: &SharedBackend) -> Result<(), ConnectorError> {
    let (mut hotlap, pipe) = prepared(ResumableSource::new(log()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))?;
    Ok(())
}

#[test]
fn an_ambiguous_marker_write_is_commit_uncertain() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    // The marker is durable but the write reports failure: an ambiguous ack.
    faulty.fail("put", b"checkpoint/1/commit", true);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    let (engine, pipe) = prepared(ResumableSource::new(log()));

    let error = take(&mut checkpointer, &engine, &pipe.sources).expect_err("must surface");

    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
    assert!(backend.get(b"checkpoint/1/commit").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);

    // The body and commit marker persisted, so restart completes publication.
    recover(&backend).expect("an empty sink set is re-drivable");
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
}

#[test]
fn a_clear_failure_after_publish_keeps_the_valid_checkpoint() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/commit", false);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    let (engine, pipe) = prepared(ResumableSource::new(log()));

    let error = take(&mut checkpointer, &engine, &pipe.sources).expect_err("must surface");

    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert_eq!(
        checkpointer.state(),
        CheckpointState::Ready,
        "a published checkpoint stays usable"
    );
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert!(
        backend.get(b"checkpoint/1/commit").unwrap().is_some(),
        "the failed clear left the marker for a restart to sweep"
    );

    recover(&backend).expect("the valid checkpoint must resume");
    assert!(
        backend.get(b"checkpoint/1/commit").unwrap().is_none(),
        "restart swept the stale marker"
    );
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}
