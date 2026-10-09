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
use hotlap_runtime::runtime::sources::{SourceEvent, Sources};

use crate::harness::{RemoteBag, TxnSink};
use crate::spy::SpySource;
use crate::support::{
    COUNT, Dataset, ROWS, ResumableSource, SharedBackend, engine_for, engine_with, ingest,
    sources_of,
};

/// The durable stores and remote bags left by an interrupted commit.
pub struct Fixture {
    pub backend: SharedBackend,
    pub store: SharedBackend,
    pub remote_a: RemoteBag,
    pub remote_b: RemoteBag,
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

fn take(
    checkpointer: &mut Checkpointer,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    futures::executor::block_on(checkpointer.take(engine, sources))
}

fn next(stream: &mut hotlap_runtime::runtime::sources::InputStream) -> SourceEvent {
    futures::executor::block_on(stream.next())
        .expect("an event")
        .expect("ok")
}

fn pump(engine: &mut Hotlap, sinks: [(&str, &SharedSink); 2]) {
    for (view, sink) in sinks {
        let changes: ZSetBatch = engine.take_changes(view).unwrap();
        if !changes.is_empty() {
            futures::executor::block_on(sink.write_batch(changes)).unwrap();
        }
    }
}

/// Commit three events, then fail the second sink's commit for the fourth.
pub fn interrupt() -> Fixture {
    let fixture = Fixture {
        backend: SharedBackend::default(),
        store: SharedBackend::default(),
        remote_a: RemoteBag::default(),
        remote_b: RemoteBag::default(),
    };
    let sink_a = Arc::new(TxnSink::new(
        ROWS,
        fixture.store.clone(),
        fixture.remote_a.clone(),
    ));
    let sink_b = Arc::new(TxnSink::new(
        COUNT,
        fixture.store.clone(),
        fixture.remote_b.clone(),
    ));
    let shared_a = SharedSink::new(sink_a);
    let shared_b = SharedSink::new(sink_b.clone());
    let mut checkpointer = Checkpointer::new(Box::new(fixture.backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![
            SinkSync::sink_only(shared_a.clone()),
            SinkSync::sink_only(shared_b.clone()),
        ]);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    for _ in 0..3 {
        let event = next(&mut stream);
        ingest(&mut engine, &pipe.sources, &event);
        pump(&mut engine, [(ROWS, &shared_a), (COUNT, &shared_b)]);
    }
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources).unwrap(), 1);

    let event = next(&mut stream);
    ingest(&mut engine, &pipe.sources, &event);
    pump(&mut engine, [(ROWS, &shared_a), (COUNT, &shared_b)]);
    sink_b.fail_next(1);
    assert!(take(&mut checkpointer, &engine, &pipe.sources).is_err());
    assert_eq!(checkpointer.state(), CheckpointState::CommitUncertain);
    fixture
}

/// Restart over the fixture with fresh sinks sharing its durable stores.
pub fn restart(fixture: &Fixture, redriable: bool) -> Restarted {
    let build = |view: &'static str, remote: RemoteBag| {
        let sink = TxnSink::new(view, fixture.store.clone(), remote);
        if redriable {
            sink
        } else {
            sink.non_redrivable()
        }
    };
    let shared_a = SharedSink::new(Arc::new(build(ROWS, fixture.remote_a.clone())));
    let shared_b = SharedSink::new(Arc::new(build(COUNT, fixture.remote_b.clone())));
    let mut checkpointer = Checkpointer::new(Box::new(fixture.backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![
            SinkSync::sink_only(shared_a),
            SinkSync::sink_only(shared_b),
        ]);
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
