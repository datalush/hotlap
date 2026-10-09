//! Two-source pipeline and SP8 sink fixtures for cross-source pending recovery.
//!
//! It reuses the recovery suite's resumable source so each input keeps its own
//! applied offset, and adds the join pipeline, a checkpoint seed, an ingestion
//! helper and a sink that observes whether `commit` was re-driven after an
//! interrupted checkpoint.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

use crate::backend::SharedBackend;
use crate::resumable::{Dataset, ResumableSource};

/// Log for source `a`: three batches, so a partial run leaves a replayable tail.
pub fn log_a() -> Dataset {
    Dataset::new(vec![vec![1], vec![1], vec![2]]).with_retention(0)
}

/// Log for source `b`: two batches keyed to match `a`.
pub fn log_b() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]]).with_retention(0)
}

/// Inputs `a` (id 0) and `b` (id 1), each resumable and independent.
pub fn two_sources(a: Dataset, b: Dataset) -> Sources {
    Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: Arc::new(ResumableSource::new(a)),
            watermark: None,
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: Arc::new(ResumableSource::new(b)),
            watermark: None,
        },
    ])
    .unwrap()
}

/// Inner equi-join on `k`, exposed as view `j`.
pub fn join_pipeline(sources: Sources) -> Pipeline {
    Pipeline {
        sources,
        views: vec![(
            "j".into(),
            Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    }
}

/// Register both inputs and the join view against a fresh engine.
pub fn engine_with(pipeline: &Pipeline) -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, pipeline).unwrap();
    hotlap
}

/// Push up to `limit` events into `hotlap`, acking each through its own source.
pub fn drain(hotlap: &mut Hotlap, sources: &Sources, stream: &mut InputStream, limit: usize) {
    let mut pushed = 0;
    while pushed < limit {
        match futures::executor::block_on(stream.next()) {
            Some(Ok(event)) => {
                pipeline::ingest_event(hotlap, sources, &event).unwrap();
                sources
                    .get(event.input)
                    .unwrap()
                    .source
                    .commit(event.batch.split, event.batch.next_offset)
                    .unwrap();
                pushed += 1;
            }
            Some(Err(error)) => panic!("unexpected source error: {error}"),
            None => break,
        }
    }
}

/// Take a checkpoint synchronously and return its id.
pub fn take(checkpointer: &mut Checkpointer, engine: &Hotlap, sources: &Sources) -> u64 {
    futures::executor::block_on(checkpointer.take(engine, sources)).unwrap()
}

/// Drain `n` events into a fresh engine and persist a valid checkpoint.
pub fn seed(backend: &SharedBackend, drained: usize) -> (Pipeline, u64) {
    let pipe = join_pipeline(two_sources(log_a(), log_b()));
    let mut engine = engine_with(&pipe);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, drained);
    let id = take(&mut checkpointer, &engine, &pipe.sources);
    (pipe, id)
}

/// Copy the valid body to `pending` and add the durable commit marker.
pub fn copy_to_pending(backend: &SharedBackend, valid: u64) -> u64 {
    let pending = valid + 1;
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/{valid}/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/{pending}/{part}").as_bytes(), value)
            .unwrap();
    }
    writer
        .put(
            format!("checkpoint/{pending}/commit").as_bytes(),
            b"1".to_vec(),
        )
        .unwrap();
    pending
}

/// A sink that counts commits and declares whether they may be re-driven.
struct CountingSink {
    capabilities: SinkCapabilities,
    redriable: bool,
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A shared counting sink plus its commit counter.
pub fn counting(redriable: bool) -> (SinkSync, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(CountingSink {
        capabilities: SinkCapabilities::Idempotent,
        redriable,
        commits: Arc::clone(&commits),
    });
    (SinkSync::sink_only(SharedSink::new(sink)), commits)
}
