//! Fixtures for the writer-ACK checkpoint tests.

#[path = "../common/backend.rs"]
mod backend;
#[path = "../common/recovery/resumable.rs"]
mod resumable;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub use backend::SharedBackend;
use futures::StreamExt;
use futures::executor::block_on;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline, SinkSpec};
use hotlap_runtime::runtime::sink::SinkPump;
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use resumable::{Dataset, ResumableSource};
use tokio::sync::Notify;

/// Durable output: rows only become visible after a committed flush.
#[derive(Clone, Default)]
pub struct RemoteStore(Arc<Mutex<usize>>);

impl RemoteStore {
    pub fn total(&self) -> usize {
        *self.0.lock().unwrap()
    }

    fn flush(&self, rows: usize) {
        *self.0.lock().unwrap() += rows;
    }
}

/// A sink that stages writes in memory and flushes them on `commit`, which can
/// be gated to model a writer that has not yet acknowledged the flush.
pub struct GatedSink {
    remote: RemoteStore,
    staged: Mutex<usize>,
    gated: AtomicBool,
    fail: bool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl GatedSink {
    pub fn new(remote: RemoteStore, fail: bool) -> (Arc<Self>, Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let sink = Arc::new(Self {
            remote,
            staged: Mutex::new(0),
            gated: AtomicBool::new(false),
            fail,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        (sink, entered, release)
    }
}

#[async_trait::async_trait]
impl Sink for GatedSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            *self.staged.lock().unwrap() += item?.len();
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        if !self.gated.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fail {
            return Err(ConnectorError::Infrastructure("flush failed".into()));
        }
        let staged = std::mem::take(&mut *self.staged.lock().unwrap());
        self.remote.flush(staged);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        *self.staged.lock().unwrap() = 0;
        Ok(())
    }
}

fn group_count() -> (String, Plan) {
    (
        "c".into(),
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
}

fn sources(source: ResumableSource) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(source),
        watermark: None,
    }])
    .unwrap()
}

/// Start an engine over three events with the tapped view feeding `sink`.
pub fn start(sink: Arc<dyn Sink>) -> (Hotlap, Pipeline, SinkPump) {
    let dataset = Dataset::new(vec![vec![1], vec![1], vec![2]]).with_retention(0);
    let pipeline = Pipeline {
        sources: sources(ResumableSource::new(dataset)),
        views: vec![group_count()],
        sinks: vec![SinkSpec {
            view: "c".into(),
            sink,
        }],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let pump = SinkPump::start(&pipeline.sinks);
    (hotlap, pipeline, pump)
}

/// Drive two events through the pump so the sink stages invisible writes.
pub async fn stage_writes(hotlap: &mut Hotlap, pipeline: &Pipeline, pump: &SinkPump) {
    let mut stream = pipeline.sources.stream().unwrap();
    for _ in 0..2 {
        let event = block_on(stream.next()).unwrap().unwrap();
        pipeline::ingest_event(hotlap, &pipeline.sources, &event).unwrap();
        pipeline
            .sources
            .get(event.input)
            .unwrap()
            .source
            .commit(event.batch.split, event.batch.next_offset)
            .unwrap();
    }
    pump.pump(hotlap).await.unwrap();
}

pub fn latest(backend: &SharedBackend) -> Option<u64> {
    Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .latest()
        .unwrap()
}
