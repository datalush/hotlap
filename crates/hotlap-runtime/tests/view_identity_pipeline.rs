//! A public `Pipeline` with an incompatible saved view registry must fail
//! before the sink pump starts, so no writer opens and no EOF commit runs.

#[path = "common/view_identity_pipeline.rs"]
mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::Source;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};

use support::{
    RecordingSink, ResumableSource, SharedBackend, SpySource, corrupt_pending, filter, log, rows,
    seed, seed_output, sources,
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
