//! A checkpoint that fails after the sinks were touched blocks further attempts.
//!
//! The engine and source offsets are not rolled back with the sinks, so the
//! runtime must stop ingesting until a restart resolves the attempt. A commit
//! failure keeps the marker, the body and the staged transaction, and the
//! interleaved deliveries match the engine views exactly.

#[path = "checkpoint_uncertain/codec.rs"]
pub mod codec;
#[path = "checkpoint_uncertain/store.rs"]
pub mod store;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;
#[path = "checkpoint_uncertain/txn.rs"]
pub mod txn;

use std::sync::Arc;

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, ZSetBatch};
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputStream, SourceEvent};

use store::Store;
use support::{COUNT, Dataset, ROWS, ResumableSource, SharedBackend, bag, engine_with, ingest};
use txn::TxnSink;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// One running engine wired to the two distinct sinks.
struct Run {
    backend: SharedBackend,
    store: Arc<Store>,
    shared_a: Arc<SharedSink>,
    shared_b: Arc<SharedSink>,
    checkpointer: Checkpointer,
    engine: Hotlap,
    pipe: Pipeline,
    stream: InputStream,
}

impl Run {
    fn new() -> Self {
        let backend = SharedBackend::default();
        let store = Store::new();
        let shared_a = SharedSink::new(Arc::new(TxnSink::new(ROWS, store.clone())));
        let shared_b = SharedSink::new(Arc::new(TxnSink::new(COUNT, store.clone())));
        let checkpointer =
            Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
                SinkSync::sink_only(shared_a.clone()),
                SinkSync::sink_only(shared_b.clone()),
            ]);
        let (engine, pipe) = engine_with(ResumableSource::new(log()));
        let stream = pipe.sources.stream().unwrap();
        Self {
            backend,
            store,
            shared_a,
            shared_b,
            checkpointer,
            engine,
            pipe,
            stream,
        }
    }

    /// Ingest one event and push each view's changelog into its sink.
    fn step(&mut self) {
        let event: SourceEvent = futures::executor::block_on(self.stream.next())
            .expect("an event")
            .expect("ok");
        ingest(&mut self.engine, &self.pipe.sources, &event);
        for (view, sink) in [(ROWS, &self.shared_a), (COUNT, &self.shared_b)] {
            let changes: ZSetBatch = self.engine.take_changes(view).unwrap();
            if !changes.is_empty() {
                futures::executor::block_on(sink.write_batch(changes)).unwrap();
            }
        }
    }

    fn take(&mut self) -> Result<u64, ConnectorError> {
        futures::executor::block_on(self.checkpointer.take(&self.engine, &self.pipe.sources))
    }

    /// Commit three events as checkpoint 1 and check both remote bags.
    fn valid_checkpoint(&mut self) {
        for _ in 0..3 {
            self.step();
        }
        assert_eq!(self.take().unwrap(), 1);
        assert_eq!(
            self.store.remote_bag(ROWS),
            bag(&self.engine.snapshot(ROWS).unwrap())
        );
        assert_eq!(
            self.store.remote_bag(COUNT),
            bag(&self.engine.snapshot(COUNT).unwrap())
        );
    }
}

#[test]
fn a_commit_failure_is_commit_uncertain_and_blocks() {
    let mut run = Run::new();
    run.valid_checkpoint();

    // The fourth event reaches both sinks, but the second commit fails with no
    // effect, so the engine view and its remote bag diverge.
    run.step();
    run.store.fail_before(COUNT, 1);
    let error = run.take().expect_err("the failed commit must surface");

    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert_eq!(run.checkpointer.state(), CheckpointState::CommitUncertain);
    assert_eq!(
        run.backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(run.backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(
        run.store.remote_bag(ROWS),
        bag(&run.engine.snapshot(ROWS).unwrap())
    );
    assert_ne!(
        run.store.remote_bag(COUNT),
        bag(&run.engine.snapshot(COUNT).unwrap())
    );
    assert!(
        run.take().is_err(),
        "a commit failure must block further attempts on this runtime"
    );
}
