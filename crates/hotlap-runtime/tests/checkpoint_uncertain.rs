//! A checkpoint that fails after the sinks were touched blocks further attempts.
//!
//! The engine and source offsets are not rolled back with the sinks, so the
//! runtime must stop ingesting until a restart resolves the attempt. A commit
//! failure keeps the marker, the body and the staged sink payload, and the
//! interleaved deliveries match the engine views exactly.

#[path = "checkpoint_uncertain/codec.rs"]
pub mod codec;
#[path = "checkpoint_uncertain/harness.rs"]
pub mod harness;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;
#[path = "checkpoint_uncertain/txn.rs"]
pub mod txn;

use std::sync::Arc;

use futures::StreamExt;
use hotlap::ZSetBatch;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{SourceEvent, Sources};

use harness::{RemoteBag, TxnSink};
use support::{COUNT, Dataset, ROWS, ResumableSource, SharedBackend, bag, engine_with, ingest};

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

/// Push every view's changelog into its own sink.
fn pump(engine: &mut hotlap::Hotlap, sinks: [(&str, &SharedSink); 2]) {
    for (view, sink) in sinks {
        let changes: ZSetBatch = engine.take_changes(view).unwrap();
        if !changes.is_empty() {
            futures::executor::block_on(sink.write_batch(changes)).unwrap();
        }
    }
}

fn next(stream: &mut hotlap_runtime::runtime::sources::InputStream) -> SourceEvent {
    futures::executor::block_on(stream.next())
        .expect("an event")
        .expect("ok")
}

#[test]
fn a_commit_failure_is_commit_uncertain_and_blocks() {
    let backend = SharedBackend::default();
    let store = SharedBackend::default();
    let remote_a = RemoteBag::default();
    let remote_b = RemoteBag::default();
    let sink_a = Arc::new(TxnSink::new(ROWS, store.clone(), remote_a.clone()));
    let sink_b = Arc::new(TxnSink::new(COUNT, store.clone(), remote_b.clone()));
    let shared_a = SharedSink::new(sink_a.clone());
    let shared_b = SharedSink::new(sink_b.clone());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(shared_a.clone()),
            SinkSync::sink_only(shared_b.clone()),
        ]);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();

    // Three events commit as a valid checkpoint; the remote bags match the views.
    for _ in 0..3 {
        let event = next(&mut stream);
        ingest(&mut engine, &pipe.sources, &event);
        pump(&mut engine, [(ROWS, &shared_a), (COUNT, &shared_b)]);
    }
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources).unwrap(), 1);
    assert_eq!(remote_a.snapshot(), bag(&engine.snapshot(ROWS).unwrap()));
    assert_eq!(remote_b.snapshot(), bag(&engine.snapshot(COUNT).unwrap()));

    // The fourth event reaches both sinks, but the second commit fails.
    let event = next(&mut stream);
    ingest(&mut engine, &pipe.sources, &event);
    pump(&mut engine, [(ROWS, &shared_a), (COUNT, &shared_b)]);
    sink_b.fail_next(1);
    let result = take(&mut checkpointer, &engine, &pipe.sources);

    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "the sink error must stay typed: {result:?}"
    );
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
    // The first participant confirmed its weighted changes exactly.
    assert_eq!(remote_a.snapshot(), bag(&engine.snapshot(ROWS).unwrap()));
    assert_ne!(remote_b.snapshot(), bag(&engine.snapshot(COUNT).unwrap()));
    assert!(
        take(&mut checkpointer, &engine, &pipe.sources).is_err(),
        "a commit failure must block further attempts on this runtime"
    );
}
