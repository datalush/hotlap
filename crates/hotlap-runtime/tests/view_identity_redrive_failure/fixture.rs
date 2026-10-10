use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, Plan, ZSetBatch};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use super::support::{Dataset, ResumableSource, SharedBackend, SpySource, filter, sources};

#[derive(Default)]
pub struct DurableRemote {
    pub staged: Vec<(i64, i64)>,
    pub committed: Vec<(i64, i64)>,
    fail_next: bool,
}

pub struct RedriveSink {
    pub remote: Arc<Mutex<DurableRemote>>,
    pub writes: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for RedriveSink {
    fn physical_identity(&self) -> Option<String> {
        Some("test/view-identity-redrive/output".into())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut staged = Vec::new();
        while let Some(change) = changes.next().await {
            let change = change?;
            let keys = change
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("test sink receives Int64 keys");
            let diffs = change
                .diff
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("test sink receives Int64 diffs");
            staged.extend((0..keys.len()).map(|index| (keys.value(index), diffs.value(index))));
        }
        self.remote.lock().unwrap().staged.extend(staged);
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }

    fn commit_redriable(&self) -> bool {
        true
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        let mut remote = self.remote.lock().unwrap();
        if remote.fail_next {
            remote.fail_next = false;
            return Err(ConnectorError::Infrastructure(
                "transient re-drive error".into(),
            ));
        }
        let staged = std::mem::take(&mut remote.staged);
        remote.committed.extend(staged);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

pub fn redrive_sink() -> (Arc<RedriveSink>, Arc<Mutex<DurableRemote>>) {
    redrive_sink_with_failure(true)
}

pub fn ready_redrive_sink() -> (Arc<RedriveSink>, Arc<Mutex<DurableRemote>>) {
    redrive_sink_with_failure(false)
}

fn redrive_sink_with_failure(fail_next: bool) -> (Arc<RedriveSink>, Arc<Mutex<DurableRemote>>) {
    let remote = Arc::new(Mutex::new(DurableRemote {
        fail_next,
        ..DurableRemote::default()
    }));
    let initial_process_sink = RedriveSink {
        remote: Arc::clone(&remote),
        writes: AtomicU32::new(0),
    };
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let change = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap();
    futures::executor::block_on(
        initial_process_sink.write(Box::pin(futures::stream::iter([Ok(change)]))),
    )
    .unwrap();
    (
        Arc::new(RedriveSink {
            remote: Arc::clone(&remote),
            writes: AtomicU32::new(0),
        }),
        remote,
    )
}

pub fn seeded_registries() -> (SharedBackend, Dataset) {
    let backend = SharedBackend::default();
    let dataset = Dataset::new(vec![vec![1], vec![2], vec![1], vec![2], vec![3]]);
    seed_registry(&backend, vec![("a".into(), filter(1))], dataset.clone(), 3);
    let pending = SharedBackend::default();
    seed_registry(
        &pending,
        vec![("a".into(), filter(1)), ("b".into(), filter(2))],
        dataset.clone(),
        4,
    );
    copy_as_pending_commit(&backend, &pending);
    (backend, dataset)
}

pub fn seeded_compatible_registries() -> (SharedBackend, Dataset) {
    let backend = SharedBackend::default();
    let dataset = Dataset::new(vec![vec![1], vec![2], vec![1], vec![2], vec![3]]);
    let views = vec![("a".into(), filter(1)), ("b".into(), filter(2))];
    seed_registry(&backend, views.clone(), dataset.clone(), 3);
    let pending = SharedBackend::default();
    seed_registry(&pending, views, dataset.clone(), 4);
    let fallback = read_checkpoint(&backend);
    let interrupted = read_checkpoint(&pending);
    assert_ne!(fallback.engine, interrupted.engine);
    assert_eq!(
        fallback.sources.entries[0]
            .state
            .offsets
            .values()
            .copied()
            .next(),
        Some(3)
    );
    assert_eq!(
        interrupted.sources.entries[0]
            .state
            .offsets
            .values()
            .copied()
            .next(),
        Some(4)
    );
    copy_as_pending_commit(&backend, &pending);
    (backend, dataset)
}

fn read_checkpoint(backend: &SharedBackend) -> hotlap_runtime::runtime::checkpoint::Checkpoint {
    let sink = RedriveSink {
        remote: Arc::new(Mutex::new(DurableRemote::default())),
        writes: AtomicU32::new(0),
    };
    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(SharedSink::new(Arc::new(sink)), "output".into(), "a".into()),
        ]);
    checkpointer.read(1).unwrap()
}

fn seed_registry(
    backend: &SharedBackend,
    views: Vec<(String, Plan)>,
    dataset: Dataset,
    consumed: usize,
) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(dataset))),
        views,
        sinks: vec![SinkSpec::named(
            "output",
            "a",
            Arc::new(RedriveSink {
                remote: Arc::new(Mutex::new(DurableRemote::default())),
                writes: AtomicU32::new(0),
            }),
        )],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    let mut pushed = 0;
    while pushed < consumed {
        let event = futures::executor::block_on(stream.next())
            .expect("seed stream has enough rows")
            .expect("seed source has no errors");
        pipeline::ingest_event(&mut hotlap, &pipe.sources, &event).unwrap();
        pipe.sources
            .get(event.input)
            .unwrap()
            .source
            .commit(event.batch.split, event.batch.next_offset)
            .unwrap();
        pushed += 1;
    }
    let sink = pipe.sinks[0].sink.clone();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(SharedSink::new(sink), "output".into(), "a".into()),
        ]);
    futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap();
}

fn copy_as_pending_commit(valid: &SharedBackend, pending: &SharedBackend) {
    let mut writer = valid.clone();
    for part in ["engine", "sources", "participants"] {
        let body = pending
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
}

pub fn pipeline(
    backend: &SharedBackend,
    dataset: Dataset,
    sink: Option<Arc<dyn Sink>>,
) -> (Pipeline, Arc<SpySource>) {
    pipeline_retaining(backend, dataset, sink, DEFAULT_RETAIN)
}

pub fn pipeline_retaining(
    backend: &SharedBackend,
    dataset: Dataset,
    sink: Option<Arc<dyn Sink>>,
    retain: usize,
) -> (Pipeline, Arc<SpySource>) {
    let source: Arc<dyn Source> = Arc::new(ResumableSource::new(dataset));
    let spy = Arc::new(SpySource::new(source));
    let as_source: Arc<dyn Source> = spy.clone();
    let sink = sink.unwrap_or_else(|| {
        Arc::new(RedriveSink {
            remote: Arc::new(Mutex::new(DurableRemote::default())),
            writes: AtomicU32::new(0),
        })
    });
    let sinks = vec![SinkSpec::named("output", "a", sink)];
    let pipeline = Pipeline {
        sources: sources(as_source),
        views: vec![("a".into(), filter(1)), ("b".into(), filter(2))],
        sinks,
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain,
        }),
        retention: None,
    };
    (pipeline, spy)
}
