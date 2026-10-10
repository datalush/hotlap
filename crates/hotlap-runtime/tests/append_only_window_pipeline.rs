//! Public pipeline coverage for final tumbling-window output.

#[path = "cross_source_support/sql_source.rs"]
mod sql_source;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{InputId, Plan, ZSetBatch};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec, Watermark};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use sql_source::{BatchSender, SqlSource};

struct AppendOnlySink(Arc<Mutex<Vec<ZSetBatch>>>);

#[async_trait::async_trait]
impl Sink for AppendOnlySink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(batch) = changes.next().await {
            self.0.lock().unwrap().push(batch?);
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }

    fn accepts_retractions(&self) -> bool {
        false
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

fn batch(times: &[i64]) -> SourceBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1; times.len()])),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), columns).unwrap(),
        base_offset: 0,
        next_offset: times.len() as i64,
        split: 0,
    }
}

fn wait_for_close(output: &Arc<Mutex<Vec<ZSetBatch>>>) -> Vec<ZSetBatch> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let batches = output.lock().unwrap().clone();
        if !batches.is_empty() || Instant::now() >= deadline {
            return batches;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn row(batch: &ZSetBatch) -> (i64, i64, i64, i64) {
    let columns: Vec<&Int64Array> = batch
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref().unwrap())
        .collect();
    let diff = batch.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (
        columns[0].value(0),
        columns[1].value(0),
        columns[2].value(0),
        diff.value(0),
    )
}

fn pipeline(source: Arc<SqlSource>, sink: Arc<dyn Sink>) -> Pipeline {
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "src".into(),
        source,
        watermark: Some(Watermark { lag: 0 }),
    }])
    .unwrap();
    Pipeline {
        sources,
        views: vec![(
            "window".into(),
            Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                time_col: 1,
                size: 10_000,
            },
        )],
        sinks: vec![SinkSpec {
            view: "window".into(),
            sink,
        }],
        checkpoint: None,
        retention: None,
    }
}

#[test]
fn pipeline_delivers_closed_window_as_one_positive_append() {
    let (source, senders): (Arc<SqlSource>, Vec<BatchSender>) = SqlSource::new(schema(), 1);
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink: Arc<dyn Sink> = Arc::new(AppendOnlySink(output.clone()));
    let handle = EngineHandle::start(pipeline(source.clone(), sink)).unwrap();

    senders[0].send(Ok(batch(&[1_000, 2_000, 12_000]))).unwrap();
    let batches = wait_for_close(&output);
    assert_eq!(batches.len(), 1);
    assert_eq!(row(&batches[0]), (1, 0, 2, 1));
    assert_eq!(source.commits().len(), 1);

    handle.shutdown().unwrap();
}
