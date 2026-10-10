//! Fail-stop: a source ack failure stops ingestion and forbids later writes.

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
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use source_support::ControlledSource;

use backend::SharedBackend;

/// A source that yields one batch but always fails to ack it.
struct FailAckSource {
    schema: SchemaRef,
    batch: SourceBatch,
}

impl Source for FailAckSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/runtime-fail-stop/ack-source".into())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![split()])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items: Vec<Result<SourceBatch, ConnectorError>> = vec![Ok(self.batch.clone())];
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn commit(&self, split: SplitId, _offset: Offset) -> Result<(), ConnectorError> {
        Err(ConnectorError::Infrastructure(format!(
            "ack failed for split {split}"
        )))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
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

fn pipeline(sources: Sources, backend: SharedBackend) -> Pipeline {
    Pipeline {
        sources,
        views: vec![(
            "j".into(),
            Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
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

#[test]
fn ack_failure_stops_the_runtime_and_rejects_later_checkpoints() {
    let a = Arc::new(FailAckSource {
        schema: schema(),
        batch: batch_on(1),
    });
    let (b, b_tx) = ControlledSource::new(schema(), vec![split()]);
    let handle = EngineHandle::start(pipeline(
        sources(a.clone(), b.clone()),
        SharedBackend::default(),
    ))
    .unwrap();

    // A's batch is ingested, but its ack fails, so the whole runtime stops.
    assert!(wait_for(|| handle.last_error().unwrap().is_some()));

    // The runtime never polls B again: a later batch produces no ack.
    b_tx[0].send(Ok(batch_on(1))).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        b.commits().is_empty(),
        "no source may continue after a fail-stop"
    );

    // Checkpoint and view-build are rejected; reads and shutdown remain.
    let checkpoint_error = handle.checkpoint().unwrap_err();
    assert!(checkpoint_error.to_string().contains("runtime failure"));
    let build = handle.build_view(
        "v2",
        Plan::Project {
            input: Box::new(Plan::Source(InputId(0))),
            cols: vec![0],
        },
    );
    assert!(build.unwrap_err().to_string().contains("runtime failure"));

    assert!(b.applied().offsets.is_empty());
    let shutdown_error = handle
        .shutdown()
        .expect_err("source failure must survive shutdown");
    assert!(
        shutdown_error
            .to_string()
            .contains("ack failed for split 0"),
        "shutdown should preserve the original source failure: {shutdown_error}"
    );
}

#[test]
fn a_source_that_cannot_open_aborts_startup() {
    let bad = ControlledSource::failing_read(schema());
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "bad".into(),
            source: bad,
            watermark: None,
        }])
        .unwrap(),
        views: vec![],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    assert!(EngineHandle::start(pipeline).is_err());
}
