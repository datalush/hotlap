use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{ChangeBatch, InputId, Plan, Row, Scalar};

use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};

/// Emits a fixed set of `(k, _event_time)` batches, like the differential test.
struct FakeSource {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

impl Source for FakeSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        SourceState {
            offsets: BTreeMap::new(),
        }
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

/// Collects written change batches; `delay` forces engine backpressure.
#[derive(Clone)]
struct FakeSink {
    batches: Arc<Mutex<Vec<ChangeBatch>>>,
    delay: Option<Duration>,
}

impl FakeSink {
    fn new(delay: Option<Duration>) -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            delay,
        }
    }

    fn consolidated(&self) -> BTreeMap<Row, i64> {
        consolidate(&self.batches.lock().unwrap())
    }

    fn len(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            self.batches.lock().unwrap().push(batch);
        }
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema, cols).unwrap(),
        base_offset: 0,
    }
}

/// A distinct trailing row, so observing it proves earlier batches were processed.
fn sentinel() -> Row {
    Row(vec![Scalar::I64(999), Scalar::I64(1)])
}

fn group_count() -> (String, Plan) {
    (
        "c".into(),
        Plan::GroupCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
        },
    )
}

fn start(batches: Vec<SourceBatch>, sink: FakeSink) -> (EngineHandle, FakeSink) {
    let schema = batches[0].batch.schema();
    let spec = SinkSpec {
        view: "c".into(),
        sink: Arc::new(sink.clone()),
    };
    let handle = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(FakeSource { schema, batches }),
        watermark: None,
        views: vec![group_count()],
        sinks: vec![spec],
    })
    .unwrap();
    (handle, sink)
}

fn consolidate(batches: &[ChangeBatch]) -> BTreeMap<Row, i64> {
    let mut acc: BTreeMap<Row, i64> = BTreeMap::new();
    for batch in batches {
        for (row, diff) in &batch.rows {
            *acc.entry(row.clone()).or_insert(0) += diff;
        }
    }
    acc.retain(|_, diff| *diff != 0);
    acc
}

/// Wait until both the engine and the sink have observed the sentinel row.
fn wait_converged(handle: &EngineHandle, sink: &FakeSink) -> Vec<Row> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = handle.snapshot("c").unwrap_or_default();
        let seen_engine = rows.iter().any(|r| r == &sentinel());
        let seen_sink = sink.consolidated().contains_key(&sentinel());
        if seen_engine && seen_sink {
            return rows;
        }
        assert!(Instant::now() < deadline, "sink/engine never converged");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn sink_consolidated_state_matches_snapshot() {
    let batches = vec![
        batch(&[1, 1, 2], &[10, 10, 10]),
        batch(&[2, 3], &[20, 20]),
        batch(&[999], &[99]),
    ];
    let (handle, sink) = start(batches, FakeSink::new(None));
    let rows = wait_converged(&handle, &sink);
    let expected: BTreeMap<Row, i64> = rows.iter().cloned().map(|r| (r, 1)).collect();
    assert_eq!(sink.consolidated(), expected);
    handle.shutdown().unwrap();
}

#[test]
fn backpressure_loses_no_batch() {
    let mut batches: Vec<SourceBatch> = (0..100).map(|i| batch(&[i as i64], &[i as i64])).collect();
    batches.push(batch(&[999], &[999]));
    let (handle, sink) = start(batches, FakeSink::new(Some(Duration::from_millis(1))));
    wait_converged(&handle, &sink);
    handle.shutdown().unwrap();
    assert_eq!(sink.len(), 101, "batch lost");
}

#[test]
fn shutdown_delivers_the_last_changelog() {
    let batches = vec![
        batch(&[1, 2], &[10, 10]),
        batch(&[3], &[20]),
        batch(&[999], &[99]),
    ];
    let (handle, sink) = start(batches, FakeSink::new(None));
    wait_converged(&handle, &sink);
    handle.shutdown().unwrap();
    assert_eq!(sink.len(), 3, "the sink missed a changelog batch");
}
