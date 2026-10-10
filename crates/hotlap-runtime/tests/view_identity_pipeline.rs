//! A public `Pipeline` with an incompatible saved view registry must fail
//! before the sink pump starts, so no writer opens and no EOF commit runs.

#[path = "common/view_identity_pipeline.rs"]
mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId, Plan, ZSetBatch};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use support::{
    RecordingSink, ResumableSource, SharedBackend, SpySource, corrupt_pending, filter, log, rows,
    seed, seed_output, seed_two_outputs, sources,
};

#[test]
fn an_incompatible_registry_fails_before_the_pump_starts() {
    let backend = SharedBackend::default();
    seed(&backend);

    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let as_source: Arc<dyn Source> = spy.clone();
    let sink = Arc::new(RecordingSink::default());
    let as_sink: Arc<dyn Sink> = sink.clone();
    let pipeline = Pipeline {
        sources: sources(as_source),
        // Same plan as the saved `a`, but a different name.
        views: vec![("b".into(), filter(1))],
        sinks: vec![SinkSpec {
            view: "b".into(),
            sink: as_sink,
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };

    let result = EngineHandle::start(pipeline);
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "an incompatible registry must be rejected"
    );
    assert_eq!(sink.writes.load(Ordering::SeqCst), 0, "no write may run");
    assert_eq!(
        sink.commits.load(Ordering::SeqCst),
        0,
        "the pump must not run its EOF commit"
    );
    assert_eq!(spy.resumed(), 0, "no source may be reopened");
    assert_eq!(spy.offset(), None, "no source may be reopened");
}

#[test]
fn corrupt_pending_body_uses_the_valid_fallback_during_preflight_and_start() {
    let backend = SharedBackend::default();
    seed_output(&backend);
    corrupt_pending(&backend);

    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let pipe = Pipeline {
        sources: sources(spy.clone()),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };
    let mut preflight = pipe;
    preflight
        .preflight_recovery()
        .expect("preflight must allow the same corrupt-pending fallback as recovery");
    let handle = EngineHandle::start(preflight)
        .expect("public pipeline startup must discard the corrupt pending body");
    assert_eq!(rows(&handle.snapshot("a").unwrap()), vec![vec![1]]);
    assert_eq!(spy.resumed(), 1);
    assert_eq!(spy.offset(), Some(1));
    handle.shutdown().unwrap();
}

#[test]
fn corrupt_transactional_pending_is_rejected_before_pipeline_effects() {
    let backend = SharedBackend::default();
    seed(&backend);
    corrupt_pending(&backend);
    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let sink = Arc::new(RecordingSink::default());
    let pipeline = Pipeline {
        sources: sources(spy.clone()),
        views: vec![("a".into(), filter(1))],
        sinks: vec![SinkSpec {
            view: "a".into(),
            sink: sink.clone(),
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };

    let result = EngineHandle::start(pipeline);

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
    assert_eq!(sink.commits.load(Ordering::SeqCst), 0);
    assert_eq!(spy.resumed(), 0);
    assert_eq!(spy.offset(), None);
    assert!(backend.get(b"checkpoint/2/commit").unwrap().is_some());
}

#[test]
fn corrupt_latest_valid_internal_ipc_falls_back_to_restorable_predecessor() {
    let backend = SharedBackend::default();
    seed_two_outputs(&backend);
    corrupt_valid_output(&backend, 2);
    let mut markers = backend.clone();
    markers.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    markers
        .put(b"checkpoint/2/prepare", b"prepare-v1".to_vec())
        .unwrap();
    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let pipeline = Pipeline {
        sources: sources(spy.clone()),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: 1,
        }),
        retention: None,
    };

    let handle = EngineHandle::start(pipeline).expect("fallback checkpoint remains usable");

    assert_eq!(rows(&handle.snapshot("a").unwrap()), vec![vec![1]]);
    assert_eq!(spy.offset(), Some(1));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        backend.get(b"checkpoint/2/prepare").unwrap(),
        Some(b"prepare-v1".to_vec())
    );
    handle.shutdown().unwrap();
}

#[test]
fn transactional_start_rejects_corrupt_latest_ipc_without_replay_or_effects() {
    let backend = SharedBackend::default();
    let remote = seed_transactional_checkpoints(&backend);
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), 1);
    let first = checkpointer.read(1).unwrap();
    let second = checkpointer.read(2).unwrap();
    assert_ne!(first.engine, second.engine);
    assert_eq!(
        first.sources.entries[0]
            .state
            .offsets
            .values()
            .copied()
            .next(),
        Some(1)
    );
    assert_eq!(
        second.sources.entries[0]
            .state
            .offsets
            .values()
            .copied()
            .next(),
        Some(2)
    );
    corrupt_valid_output(&backend, 2);
    let corrupt_body = backend.get(b"checkpoint/2/engine").unwrap();
    let restarted_sink = Arc::new(DurableTxnSink::new(Arc::clone(&remote)));
    let source: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(source));
    let pipeline = Pipeline {
        sources: sources(spy.clone()),
        views: vec![("a".into(), Plan::Source(InputId(0)))],
        sinks: vec![SinkSpec {
            view: "a".into(),
            sink: restarted_sink.clone(),
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: 1,
        }),
        retention: None,
    };

    let result = EngineHandle::start(pipeline);

    assert!(matches!(result, Err(ConnectorError::Corruption(_))));
    assert_eq!(restarted_sink.writes.load(Ordering::SeqCst), 0);
    assert_eq!(restarted_sink.commits.load(Ordering::SeqCst), 0);
    assert_eq!(spy.resumed(), 0);
    assert_eq!(spy.offset(), None);
    assert_eq!(remote.lock().unwrap().committed, vec![(1, 1), (2, 1)]);
    assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), corrupt_body);
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
}

#[test]
fn transactional_pending_internal_corruption_preserves_evidence() {
    let backend = SharedBackend::default();
    let remote = seed_transactional_checkpoints(&backend);
    corrupt_valid_output(&backend, 2);
    let corrupt_body = backend.get(b"checkpoint/2/engine").unwrap().unwrap();
    let source_body = backend.get(b"checkpoint/2/sources").unwrap();
    let mut writer = backend.clone();
    writer.delete(b"checkpoint/2/valid").unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    let restarted_sink = Arc::new(DurableTxnSink::new(Arc::clone(&remote)));
    let source: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(source));
    let pipeline = Pipeline {
        sources: sources(spy.clone()),
        views: vec![("a".into(), Plan::Source(InputId(0)))],
        sinks: vec![SinkSpec {
            view: "a".into(),
            sink: restarted_sink.clone(),
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: 1,
        }),
        retention: None,
    };

    let result = EngineHandle::start(pipeline);

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(restarted_sink.writes.load(Ordering::SeqCst), 0);
    assert_eq!(restarted_sink.commits.load(Ordering::SeqCst), 0);
    assert_eq!(spy.resumed(), 0);
    assert_eq!(spy.offset(), None);
    assert_eq!(remote.lock().unwrap().committed, vec![(1, 1), (2, 1)]);
    assert_eq!(
        backend.get(b"checkpoint/2/engine").unwrap(),
        Some(corrupt_body)
    );
    assert_eq!(backend.get(b"checkpoint/2/sources").unwrap(), source_body);
    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}

fn corrupt_valid_output(backend: &SharedBackend, id: u64) {
    let key = format!("checkpoint/{id}/engine");
    let bytes = backend.get(key.as_bytes()).unwrap().unwrap();
    let mut snapshot = hotlap_engine::decode_snapshot(&bytes).unwrap();
    snapshot.views[0].output.as_mut().unwrap().ipc = b"not an Arrow IPC stream".to_vec();
    let bytes = hotlap_engine::encode_snapshot(&snapshot).unwrap();
    let mut writer = backend.clone();
    writer.put(key.as_bytes(), bytes).unwrap();
}

#[derive(Default)]
struct DurableRows {
    staged: Vec<(i64, i64)>,
    committed: Vec<(i64, i64)>,
}

struct DurableTxnSink {
    rows: Arc<Mutex<DurableRows>>,
    writes: std::sync::atomic::AtomicU32,
    commits: std::sync::atomic::AtomicU32,
}

impl DurableTxnSink {
    fn new(rows: Arc<Mutex<DurableRows>>) -> Self {
        Self {
            rows,
            writes: std::sync::atomic::AtomicU32::new(0),
            commits: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Sink for DurableTxnSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while let Some(change) = changes.next().await {
            let change = change?;
            let keys = change
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let diffs = change.diff.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut rows = self.rows.lock().unwrap();
            rows.staged
                .extend((0..keys.len()).map(|i| (keys.value(i), diffs.value(i))));
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let mut rows = self.rows.lock().unwrap();
        let staged = std::mem::take(&mut rows.staged);
        rows.committed.extend(staged);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.rows.lock().unwrap().staged.clear();
        Ok(())
    }
}

fn seed_transactional_checkpoints(backend: &SharedBackend) -> Arc<Mutex<DurableRows>> {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), Plan::Source(InputId(0)))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    hotlap.tap_view("a").unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    let remote = Arc::new(Mutex::new(DurableRows::default()));
    let sink = Arc::new(DurableTxnSink::new(Arc::clone(&remote)));
    let shared_sink = SharedSink::new(sink.clone());
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3)
        .with_sinks(vec![SinkSync::sink_only(shared_sink)]);
    for expected_id in 1..=2 {
        let event = futures::executor::block_on(stream.next()).unwrap().unwrap();
        pipeline::ingest_event(&mut hotlap, &pipe.sources, &event).unwrap();
        let changes: ZSetBatch = hotlap.take_changes("a").unwrap();
        futures::executor::block_on(sink.write(Box::pin(futures::stream::iter([Ok(changes)]))))
            .unwrap();
        pipe.sources
            .get(event.input)
            .unwrap()
            .source
            .commit(event.batch.split, event.batch.next_offset)
            .unwrap();
        assert_eq!(
            futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap(),
            expected_id
        );
    }
    remote
}
