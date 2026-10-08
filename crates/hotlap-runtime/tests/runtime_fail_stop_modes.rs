#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/source.rs"]
mod source_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{InputId, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use source_support::{BatchSender, ControlledSource};

use backend::SharedBackend;

enum Outcome {
    Error(String),
    Batch(SourceBatch),
}

struct ScriptSource {
    schema: SchemaRef,
    outcome: Outcome,
}

impl Source for ScriptSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![split()])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let item = match &self.outcome {
            Outcome::Error(message) => Err(ConnectorError::Infrastructure(message.clone())),
            Outcome::Batch(batch) => Ok(batch.clone()),
        };
        Ok(Box::pin(futures::stream::iter(vec![item])))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

fn error_source() -> Arc<ScriptSource> {
    Arc::new(ScriptSource {
        schema: schema(),
        outcome: Outcome::Error("read boom".into()),
    })
}

fn one_shot(batch: SourceBatch) -> Arc<ScriptSource> {
    Arc::new(ScriptSource {
        schema: batch.batch.schema(),
        outcome: Outcome::Batch(batch),
    })
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn split() -> Split {
    Split { id: 0, start: 0 }
}

fn batch_on(key: i64) -> SourceBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(schema(), vec![array]).unwrap();
    SourceBatch {
        batch,
        base_offset: 0,
        next_offset: 1,
        split: 0,
    }
}

fn two_col_batch() -> SourceBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(Int64Array::from(vec![10])),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema, cols).unwrap(),
        base_offset: 0,
        next_offset: 1,
        split: 0,
    }
}

fn sources(a: Arc<dyn Source>, b: Arc<dyn Source>) -> Sources {
    Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: a,
            watermark: None,
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: b,
            watermark: None,
        },
    ])
    .unwrap()
}

fn view_pipeline(sources: Sources, view: Plan, backend: SharedBackend) -> Pipeline {
    Pipeline {
        sources,
        views: vec![("v".into(), view)],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

fn join_view() -> Plan {
    Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    }
}

fn window_view() -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 1,
        size: 10,
    }
}

fn wait_for(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

fn assert_stops(pipeline: Pipeline, b: Arc<ControlledSource>, b_tx: Vec<BatchSender>) {
    let handle = EngineHandle::start(pipeline).unwrap();
    assert!(wait_for(|| handle.last_error().unwrap().is_some()));
    b_tx[0].send(Ok(batch_on(9))).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        b.commits().is_empty(),
        "B must not continue after fail-stop"
    );
    assert!(handle.checkpoint().is_err(), "checkpoint must be rejected");
    handle.shutdown().unwrap();
}

#[test]
fn stream_read_failure_stops_the_runtime() {
    let a = error_source();
    let (b, b_tx) = ControlledSource::new(schema(), vec![split()]);
    let pipeline = view_pipeline(sources(a, b.clone()), join_view(), SharedBackend::default());
    assert_stops(pipeline, b, b_tx);
}

#[test]
fn push_failure_stops_the_runtime() {
    let a = one_shot(two_col_batch());
    let (b, b_tx) = ControlledSource::new(schema(), vec![split()]);
    let pipeline = view_pipeline(
        sources(a, b.clone()),
        window_view(),
        SharedBackend::default(),
    );
    assert_stops(pipeline, b, b_tx);
}
