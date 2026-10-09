//! Fixtures for the restart tests: build the interrupted commit and restart it.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::{Hotlap, ZSetBatch};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::Source;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputStream, SourceEvent, Sources};

use crate::spy::SpySource;
use crate::store::Store;
use crate::support::{
    COUNT, Dataset, ROWS, ResumableSource, SharedBackend, engine_for, engine_with, ingest,
    sources_of,
};
use crate::txn::TxnSink;

/// Where the second commit fails relative to its atomic install.
pub enum Fail {
    /// Fail before any effect.
    Before,
    /// Install the effect, then report failure (an ACK lost after commit).
    After,
}

/// The durable stores left by an interrupted commit.
pub struct Fixture {
    pub backend: SharedBackend,
    pub store: Arc<Store>,
}

/// The outcome of a restart over a fixture.
pub struct Restarted {
    pub hotlap: Result<Hotlap, ConnectorError>,
    pub spy: Arc<SpySource>,
    pub signal: Mutex<Option<String>>,
    pub metrics: MetricsRegistry,
}

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

fn step(
    engine: &mut Hotlap,
    pipe: &Pipeline,
    a: &SharedSink,
    b: &SharedSink,
    stream: &mut InputStream,
) {
    let event: SourceEvent = futures::executor::block_on(stream.next())
        .expect("an event")
        .expect("ok");
    ingest(engine, &pipe.sources, &event);
    for (view, sink) in [(ROWS, a), (COUNT, b)] {
        let changes: ZSetBatch = engine.take_changes(view).unwrap();
        if !changes.is_empty() {
            futures::executor::block_on(sink.write_batch(changes)).unwrap();
        }
    }
}

/// Commit three events, then fail the second sink's commit for the fourth.
pub fn interrupt(fail: Fail) -> Fixture {
    let backend = SharedBackend::default();
    let store = Store::new();
    let a = SharedSink::new(Arc::new(TxnSink::new(ROWS, store.clone())));
    let b = SharedSink::new(Arc::new(TxnSink::new(COUNT, store.clone())));
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(a.clone()),
            SinkSync::sink_only(b.clone()),
        ]);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    for _ in 0..3 {
        step(&mut engine, &pipe, &a, &b, &mut stream);
    }
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources).unwrap(), 1);
    step(&mut engine, &pipe, &a, &b, &mut stream);
    match fail {
        Fail::Before => store.fail_before(COUNT, 1),
        Fail::After => store.fail_after(COUNT, 1),
    }
    assert!(take(&mut checkpointer, &engine, &pipe.sources).is_err());
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
    Fixture { backend, store }
}

fn take(
    checkpointer: &mut Checkpointer,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    futures::executor::block_on(checkpointer.take(engine, sources))
}

/// Restart over the fixture with fresh sinks sharing its durable store.
pub fn restart(fixture: &Fixture, redriable: bool) -> Restarted {
    let build = |view: &'static str| {
        let sink = TxnSink::new(view, fixture.store.clone());
        if redriable {
            sink
        } else {
            sink.non_redrivable()
        }
    };
    let a = SharedSink::new(Arc::new(build(ROWS)));
    let b = SharedSink::new(Arc::new(build(COUNT)));
    let mut checkpointer = Checkpointer::new(Box::new(fixture.backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(a), SinkSync::sink_only(b)]);
    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let (mut hotlap, pipe): (Hotlap, Pipeline) = engine_for(sources_of(spy.clone()));
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let hotlap = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .map(|_stream| hotlap);
    Restarted {
        hotlap,
        spy,
        signal,
        metrics,
    }
}
