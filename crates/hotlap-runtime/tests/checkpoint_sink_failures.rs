//! A failed sink drain makes the direct checkpointer fail-stop.

#[path = "common/backend.rs"]
mod backend;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer};
use hotlap_runtime::runtime::pipeline::SinkSpec;
use hotlap_runtime::runtime::sink::SinkPump;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;

struct FailingSink(Arc<AtomicBool>);

#[async_trait::async_trait]
impl Sink for FailingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while changes.next().await.is_some() {}
        Err(ConnectorError::Infrastructure("write failed".into()))
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> Arc<Schema> {
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

fn batch() -> hotlap::ZSetBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
    let record = RecordBatch::try_new(schema, vec![values]).unwrap();
    hotlap::ZSetBatch::new(record, Arc::new(Int64Array::from(vec![1, 1, -1]))).unwrap()
}

async fn engine_with_delta(pump: &SinkPump) -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    hotlap.register_input("in").unwrap();
    hotlap
        .create_view(
            "c",
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )
        .unwrap();
    hotlap.tap_view("c").unwrap();
    hotlap.push("in", &batch()).unwrap();
    pump.pump(&mut hotlap).await.unwrap();
    hotlap
}

#[tokio::test]
async fn writer_rollback_on_failed_drain_marks_checkpointer_failed() {
    let aborted = Arc::new(AtomicBool::new(false));
    let pump = SinkPump::start(&[SinkSpec {
        view: "c".into(),
        sink: Arc::new(FailingSink(Arc::clone(&aborted))),
    }]);
    let hotlap = engine_with_delta(&pump).await;
    let mut checkpointer =
        Checkpointer::new(Box::new(SharedBackend::default()), 3).with_sinks(pump.coordinated());
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap();

    assert!(checkpointer.take(&hotlap, &sources).await.is_err());
    assert!(aborted.load(Ordering::SeqCst));
    assert_eq!(checkpointer.state(), CheckpointState::Failed);
    assert!(checkpointer.take(&hotlap, &sources).await.is_err());
}
