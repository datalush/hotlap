//! A public `Pipeline` with an incompatible saved view registry must fail
//! before the sink pump starts, so no writer opens and no EOF commit runs.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery/ops.rs"]
mod ops;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "common/spy.rs"]
mod spy;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{self, Pipeline, SinkSpec};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use fault::FaultBackend;
use ops::{drain, rows, take};
use resumable::{Dataset, ResumableSource};
use spy::SpySource;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]]).with_retention(0)
}

fn filter(value: i64) -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(value),
        },
    }
}

fn sources(source: Arc<dyn Source>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap()
}

/// Seed a checkpoint that binds the name `a` to the handle for `filter(1)`.
fn seed(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap();
}

/// Copy checkpoint 1 to a pending commit whose current-format body is corrupt.
fn corrupt_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    writer
        .put(b"checkpoint/2/engine", b"HLSR\x02".to_vec())
        .unwrap();
}

/// Counts writes and commits so a rejected pipeline can prove no writer ran.
#[derive(Default)]
struct RecordingSink {
    writes: AtomicU32,
    commits: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
}

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
    let source = ResumableSource::new(log());
    let seeded_sources = sources(Arc::new(source));
    let seeded_pipe = Pipeline {
        sources: seeded_sources,
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut seeded_engine = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut seeded_engine, &seeded_pipe).unwrap();
    let mut input = seeded_pipe.sources.stream().unwrap();
    drain(&mut seeded_engine, &seeded_pipe.sources, &mut input, 1);
    let mut writer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert_eq!(take(&mut writer, &seeded_engine, &seeded_pipe.sources), 1);

    for part in ["engine", "sources"] {
        let value = backend
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        let mut state = backend.clone();
        state
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    let mut state = backend.clone();
    state.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    state
        .put(b"checkpoint/2/engine", b"HLSR\x02".to_vec())
        .unwrap();

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
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &preflight).unwrap();
    let mut checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let signal = std::sync::Mutex::new(None);
    let metrics = hotlap_engine::MetricsRegistry::new();
    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &preflight.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .expect("runtime recovery must discard the corrupt pending body");
    assert_eq!(rows(&hotlap.snapshot("a").unwrap()), vec![vec![1]]);
    assert_eq!(spy.resumed(), 1);
    assert_eq!(spy.offset(), Some(1));
}

#[test]
fn preflight_keeps_storage_and_unsupported_errors_fatal() {
    let backend = SharedBackend::default();
    seed(&backend);
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("get", b"checkpoint/1/engine", false);
    let mut pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(faulty),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };
    assert!(matches!(
        pipe.preflight_recovery(),
        Err(ConnectorError::Storage(_))
    ));

    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    let mut engine = writer.get(b"checkpoint/2/engine").unwrap().unwrap();
    engine[4..8].copy_from_slice(&9_u32.to_le_bytes());
    writer.put(b"checkpoint/2/engine", engine).unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    let mut unsupported = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };
    assert!(matches!(
        unsupported.preflight_recovery(),
        Err(ConnectorError::Unsupported(_))
    ));
    assert!(backend.get(b"checkpoint/2/commit").unwrap().is_some());
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
