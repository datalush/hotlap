//! Fixtures for the recovery storage-fault tests.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::InputId;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

use crate::recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, take};
use crate::spy::SpySource;

/// A store that counts deletes, so a test can prove recovery deleted nothing.
#[derive(Clone, Default)]
pub struct TrackingBackend {
    inner: SharedBackend,
    deletes: Arc<AtomicU32>,
}

impl TrackingBackend {
    pub fn new(inner: SharedBackend) -> Self {
        Self {
            inner,
            deletes: Arc::new(AtomicU32::new(0)),
        }
    }
    pub fn deletes(&self) -> u32 {
        self.deletes.load(Ordering::SeqCst)
    }
}

impl StateBackend for TrackingBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        self.inner.get(key)
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        self.inner.put(key, value)
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        self.inner.scan(prefix)
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.inner.list(prefix)
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key)
    }
}

/// A re-drivable sink that counts commit attempts.
struct CountingSink {
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    fn commit_redriable(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// The fixed log, with retention keeping every record.
pub fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// Sources over `log` wrapped in a resume-counting spy.
pub fn spy_sources() -> (Sources, Arc<SpySource>) {
    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let as_source: Arc<dyn Source> = spy.clone();
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: as_source,
        watermark: None,
    }])
    .unwrap();
    (sources, spy)
}

/// Persist one valid checkpoint over `backend`.
pub fn seed_valid(backend: &SharedBackend) -> u64 {
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources)
}

/// Write a pending body for `pending` with genuinely different engine state and
/// applied offsets than `valid`, plus a commit marker and no `valid`.
///
/// The body comes from a real checkpoint over a fresh engine drained to `events`
/// records, so a recovery that wrongly chose the `valid` fallback would restore a
/// different snapshot and offset. `valid` must already exist.
pub fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64, events: usize) {
    let scratch = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(scratch.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, events);
    take(&mut checkpointer, &engine, &pipe.sources);

    let mut writer = backend.clone();
    let fallback = writer
        .get(format!("checkpoint/{valid}/engine").as_bytes())
        .unwrap()
        .unwrap();
    let pending_engine = scratch.get(b"checkpoint/1/engine").unwrap().unwrap();
    assert_ne!(
        pending_engine, fallback,
        "the pending fixture must differ from the fallback body"
    );
    for part in ["engine", "sources"] {
        let value = scratch
            .get(format!("checkpoint/1/{part}").as_bytes())
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
}

/// Outcome of [`start`]. Named so tests avoid `Debug` bounds on the payload.
pub type Started = Result<(hotlap::Hotlap, InputStream), ConnectorError>;

/// Run [`Recovery::start`] with a re-drivable sink.
pub fn start(
    backend: Box<dyn StateBackend + Send>,
    sources: &Sources,
) -> (Started, MetricsRegistry) {
    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(CountingSink { commits });
    let mut checkpointer = Checkpointer::new(backend, DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(sink))]);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let started = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .map(|stream| (hotlap, stream));
    (started, metrics)
}
