//! Two-source pipeline and sink fixtures for cross-source pending recovery.
//!
//! Seeding writes a *distinct* pending body: a real checkpoint is taken after
//! more events than the valid one, so promote and discard resume different
//! per-source offsets. The pending body is then reduced to the crash window
//! (`commit` marker without `valid`) over the previous valid checkpoint, and its
//! sources payload is kept in the real `HLSR` container.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::SourceBatch;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, SourceEvent, Sources};

use crate::backend::SharedBackend;
use crate::resumable::{Dataset, ResumableSource};

/// Log for source `a`: `k = 1, 1, 2`.
pub fn log_a() -> Dataset {
    Dataset::new(vec![vec![1], vec![1], vec![2]])
        .with_retention(0)
        .with_physical_identity("test/cross-source-pending/a")
}

/// Log for source `b`: `k = 1, 2`.
pub fn log_b() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]])
        .with_retention(0)
        .with_physical_identity("test/cross-source-pending/b")
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

/// A fresh two-source join pipeline, independent of any previous run.
pub fn fresh_pipeline() -> Pipeline {
    join_pipeline(two_sources(log_a(), log_b()))
}

/// Register both inputs and the join view against a fresh engine.
pub fn engine_with(pipeline: &Pipeline) -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, pipeline).unwrap();
    hotlap.tap_view("j").unwrap();
    hotlap
}

/// Push one `k` row into `input`, acking it at its own next offset.
pub fn push(hotlap: &mut Hotlap, sources: &Sources, input: InputId, key: i64) {
    let entry = sources.get(input).unwrap();
    let offset = entry.source.state().offsets.get(&0).copied().unwrap_or(0);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(entry.source.schema(), vec![array]).unwrap();
    let event = SourceEvent {
        input,
        batch: SourceBatch {
            batch,
            base_offset: offset,
            next_offset: offset + 1,
            split: 0,
        },
    };
    pipeline::ingest_event(hotlap, sources, &event).unwrap();
    entry.source.commit(0, offset + 1).unwrap();
}

/// Take a checkpoint synchronously and return its id.
pub fn take(checkpointer: &mut Checkpointer, engine: &Hotlap, sources: &Sources) -> u64 {
    futures::executor::block_on(checkpointer.take(engine, sources)).unwrap()
}

/// Leave `pending` with a `commit` marker, no `valid`, over the valid predecessor.
fn mark_pending(backend: &SharedBackend, valid: u64, pending: u64) {
    let mut writer = backend.clone();
    writer
        .delete(format!("checkpoint/{pending}/valid").as_bytes())
        .unwrap();
    writer
        .put(
            format!("checkpoint/{pending}/commit").as_bytes(),
            b"1".to_vec(),
        )
        .unwrap();
    writer
        .put(b"checkpoint/latest", valid.to_le_bytes().to_vec())
        .unwrap();
}

/// Take a valid checkpoint after `k=1,1`/`1`, then a distinct pending body after
/// `k=2` on both sources. Returns `(valid, pending)` ids.
#[allow(dead_code)] // Shared by integration-test targets with different fixture needs.
pub fn seed_pair(backend: &SharedBackend) -> (u64, u64) {
    seed_pair_with_redriable(backend, false)
}

pub fn seed_pair_with_redriable(backend: &SharedBackend, redriable: bool) -> (u64, u64) {
    let pipe = fresh_pipeline();
    let mut engine = engine_with(&pipe);
    let (sink, _) = counting(redriable);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let sources = &pipe.sources;
    push(&mut engine, sources, InputId(0), 1);
    push(&mut engine, sources, InputId(0), 1);
    push(&mut engine, sources, InputId(1), 1);
    let valid = take(&mut checkpointer, &engine, sources);
    push(&mut engine, sources, InputId(0), 2);
    push(&mut engine, sources, InputId(1), 2);
    let pending = take(&mut checkpointer, &engine, sources);
    mark_pending(backend, valid, pending);
    (valid, pending)
}

/// The stored `sources` body of `id`, in the real `HLSR` container.
pub fn sources_bytes(backend: &SharedBackend, id: u64) -> Vec<u8> {
    let key = format!("checkpoint/{id}/sources");
    backend.clone().get(key.as_bytes()).unwrap().unwrap()
}

/// Replace the stored `sources` body of `id`.
pub fn put_sources(backend: &SharedBackend, id: u64, bytes: Vec<u8>) {
    let key = format!("checkpoint/{id}/sources");
    backend.clone().put(key.as_bytes(), bytes).unwrap();
}

/// A sink that counts commits and declares whether they may be re-driven.
struct CountingSink {
    physical_identity: String,
    capabilities: SinkCapabilities,
    redriable: bool,
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }
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
        physical_identity: "test/cross-source-pending/output".into(),
        capabilities: SinkCapabilities::Idempotent,
        redriable,
        commits: Arc::clone(&commits),
    });
    (
        SinkSync::sink_only_named(SharedSink::new(sink), "output".into(), "j".into()),
        commits,
    )
}
