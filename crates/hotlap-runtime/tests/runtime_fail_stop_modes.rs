//! Fail-stop for stream-read and push failures across two sources.

#[path = "common/backend.rs"]
mod backend;
#[path = "runtime_fail_stop_modes/harness.rs"]
mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::{InputId, Plan};
use hotlap_connectors::source::{Source, SourceBatch, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use harness::{BatchSender, ControlledB, error_source, one_shot, schema};

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

/// A batch the window plan rejects: `_event_time` without a watermark.
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

/// The failure must stop the runtime and leave the other source unacked.
fn assert_stops(pipeline: Pipeline, b: Arc<ControlledB>, b_tx: Vec<BatchSender>) {
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
    let (b, b_tx) = ControlledB::new(schema(), vec![split()]);
    let pipeline = view_pipeline(sources(a, b.clone()), join_view(), SharedBackend::default());
    assert_stops(pipeline, b, b_tx);
}

#[test]
fn push_failure_stops_the_runtime() {
    let a = one_shot(two_col_batch());
    let (b, b_tx) = ControlledB::new(schema(), vec![split()]);
    let pipeline = view_pipeline(
        sources(a, b.clone()),
        window_view(),
        SharedBackend::default(),
    );
    assert_stops(pipeline, b, b_tx);
}
