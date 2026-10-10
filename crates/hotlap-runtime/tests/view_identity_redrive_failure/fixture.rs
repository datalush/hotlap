use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};

use super::support::{Dataset, ResumableSource, SharedBackend, SpySource, filter, sources};

#[derive(Default)]
pub struct DurableRemote {
    pub staged: bool,
    pub committed: bool,
    fail_next: bool,
}

pub struct RedriveSink {
    pub remote: Arc<Mutex<DurableRemote>>,
    pub writes: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for RedriveSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
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
        if remote.staged {
            remote.staged = false;
            remote.committed = true;
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

pub fn redrive_sink() -> (Arc<RedriveSink>, Arc<Mutex<DurableRemote>>) {
    let remote = Arc::new(Mutex::new(DurableRemote {
        staged: true,
        fail_next: true,
        ..DurableRemote::default()
    }));
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
    copy_as_pending_commit(&backend, &pending);
    (backend, dataset)
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
        sinks: vec![],
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
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap();
}

fn copy_as_pending_commit(valid: &SharedBackend, pending: &SharedBackend) {
    let mut writer = valid.clone();
    for part in ["engine", "sources"] {
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
    let source: Arc<dyn Source> = Arc::new(ResumableSource::new(dataset));
    let spy = Arc::new(SpySource::new(source));
    let as_source: Arc<dyn Source> = spy.clone();
    let sinks = sink
        .map(|sink| {
            vec![SinkSpec {
                view: "a".into(),
                sink,
            }]
        })
        .unwrap_or_default();
    let pipeline = Pipeline {
        sources: sources(as_source),
        views: vec![("a".into(), filter(1)), ("b".into(), filter(2))],
        sinks,
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };
    (pipeline, spy)
}
