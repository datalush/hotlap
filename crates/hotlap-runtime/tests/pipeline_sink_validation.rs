//! Pipeline-level sink validation before any writer, stream or tap starts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream::StreamExt;
use hotlap::{AggSpec, InputId, Plan};
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// A source with no splits and an empty stream, enough to start the engine.
struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// A sink that records whether the runtime ever started writing to it.
struct RecordingSink {
    written: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.written.store(true, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A sink that explicitly declares it can apply retractions.
struct RetractionSink {
    written: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Sink for RetractionSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.written.store(true, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn sources() -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap()
}

fn retracting_view() -> (String, Plan) {
    (
        "c".into(),
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
}

fn pipeline(views: Vec<(String, Plan)>, sinks: Vec<SinkSpec>) -> Pipeline {
    Pipeline {
        sources: sources(),
        views,
        sinks,
        checkpoint: None,
        retention: None,
    }
}

fn spec(view: &str, written: &Arc<AtomicBool>) -> SinkSpec {
    SinkSpec {
        view: view.into(),
        sink: Arc::new(RecordingSink {
            written: Arc::clone(written),
        }),
    }
}

#[test]
fn two_sinks_on_one_view_are_rejected_before_writers_start() {
    let written = Arc::new(AtomicBool::new(false));
    let pipeline = pipeline(
        vec![("c".into(), Plan::Source(InputId(0)))],
        vec![spec("c", &written), spec("c", &written)],
    );
    let result = EngineHandle::start(pipeline);
    assert!(
        result.is_err(),
        "fan-out to two sinks of one view is out of scope"
    );
    assert!(!written.load(Ordering::SeqCst), "no writer may start");
}

#[test]
fn a_retracting_plan_is_rejected_for_an_append_only_sink() {
    let written = Arc::new(AtomicBool::new(false));
    let pipeline = pipeline(vec![retracting_view()], vec![spec("c", &written)]);
    let result = EngineHandle::start(pipeline);
    assert!(
        result.is_err(),
        "a conservative sink must not accept a retracting plan"
    );
    assert!(!written.load(Ordering::SeqCst), "no writer may start");
}

#[test]
fn a_declared_retraction_sink_accepts_a_retracting_plan() {
    let written = Arc::new(AtomicBool::new(false));
    let pipeline = pipeline(
        vec![retracting_view()],
        vec![SinkSpec {
            view: "c".into(),
            sink: Arc::new(RetractionSink {
                written: Arc::clone(&written),
            }),
        }],
    );
    let handle = EngineHandle::start(pipeline).expect("a retraction-capable sink is accepted");
    handle.shutdown().unwrap();
}
