//! Periodic checkpoint trigger and the "not configured" on-demand reply.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use common::{SharedBackend, pipeline, wait_rows};
use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[test]
fn periodic_trigger_writes_a_checkpoint() {
    let backend = SharedBackend::default();
    let handle = EngineHandle::start(pipeline(
        backend.clone(),
        Duration::from_millis(25),
        DEFAULT_RETAIN,
    ))
    .unwrap();
    assert!(wait_rows(&handle, &[vec![1, 2], vec![2, 2]]));

    let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut latest = None;
    while Instant::now() < deadline {
        latest = reader.latest().unwrap();
        if latest.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let id = latest.expect("periodic trigger never wrote a checkpoint");
    assert!(reader.read(id).unwrap().engine.epoch >= 1);

    handle.shutdown().unwrap();
}

#[test]
fn checkpoint_without_config_is_rejected() {
    let mut without = pipeline(
        SharedBackend::default(),
        Duration::from_secs(1),
        DEFAULT_RETAIN,
    );
    without.checkpoint = None;
    let handle = EngineHandle::start(without).unwrap();
    assert!(handle.checkpoint().is_err());
    handle.shutdown().unwrap();
}

/// A source whose stream immediately yields one error.
struct ReadErrorSource;

impl Source for ReadErrorSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/checkpoint-periodic/read-error-source".into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::empty())
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items = vec![Err(ConnectorError::Infrastructure("read boom".into()))];
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// A checkpointer over a source that fails on the first read.
fn failing_pipeline(backend: SharedBackend, interval: Duration) -> Pipeline {
    Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source: Arc::new(ReadErrorSource),
            watermark: None,
        }])
        .unwrap(),
        views: vec![],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval,
            backend: Box::new(backend),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

#[test]
fn failure_disables_periodic_checkpoints() {
    let backend = SharedBackend::default();
    let handle =
        EngineHandle::start(failing_pipeline(backend.clone(), Duration::from_millis(25))).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.last_error().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(handle.last_error().unwrap().is_some(), "no source failure");

    // Several intervals pass; a disabled ticker must publish nothing.
    std::thread::sleep(Duration::from_millis(200));
    let reader = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    assert_eq!(
        reader.latest().unwrap(),
        None,
        "no checkpoint may publish after a failure"
    );
    let error = handle
        .shutdown()
        .expect_err("source failure must survive shutdown");
    assert!(error.to_string().contains("read boom"), "got {error}");
}
