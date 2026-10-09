//! Shared fixtures for the bounded-shutdown tests.
//!
//! A fixture that matters to a negative assertion signals a blocking test
//! thread through a channel, so a test never depends on a sleep to decide
//! whether the runtime is stuck.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{AggSpec, InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "backend.rs"]
mod backend;
pub use backend::SharedBackend;

/// Fire-and-forget signal from an async fixture to a blocking test thread.
#[derive(Clone)]
pub struct Signal(mpsc::Sender<()>);

impl Signal {
    /// A signal and the receiver a test waits on.
    pub fn new() -> (Self, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        (Self(tx), rx)
    }

    pub(crate) fn fire(&self) {
        let _ = self.0.send(());
    }
}

/// A schema with one `Int64` key column.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

/// One single-row batch for `key`.
pub fn batch(key: i64) -> SourceBatch {
    let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![key]))];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), cols).unwrap(),
        base_offset: 0,
        next_offset: 1,
        split: 0,
    }
}

/// A source replaying one single-row batch per key, then ending.
pub struct FixedSource {
    batches: Vec<SourceBatch>,
}

impl Source for FixedSource {
    fn schema(&self) -> SchemaRef {
        self.batches[0].batch.schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items: Vec<Result<SourceBatch, ConnectorError>> =
            self.batches.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// A finite source of one-row batches, one per key.
pub fn keys(values: &[i64]) -> Arc<dyn Source> {
    let batches = values.iter().copied().map(batch).collect();
    Arc::new(FixedSource { batches })
}

/// A checkpoint config backed by an in-memory store, with a long interval so
/// no periodic checkpoint fires during a test.
pub fn checkpoint() -> CheckpointConfig {
    CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(SharedBackend::default()),
        retain: 3,
    }
}

fn group_count() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

/// Start the engine over `source` tapping one group-count view into `sink`.
pub fn start(
    source: Arc<dyn Source>,
    sink: Arc<dyn Sink>,
    checkpoint: Option<CheckpointConfig>,
) -> EngineHandle {
    EngineHandle::start(Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![("c".into(), group_count())],
        sinks: vec![SinkSpec {
            view: "c".into(),
            sink,
        }],
        checkpoint,
        retention: None,
    })
    .unwrap()
}
