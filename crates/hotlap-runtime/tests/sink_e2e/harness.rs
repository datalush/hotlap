//! Source/sink fixtures for the end-to-end sink tests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{AggSpec, InputId, Plan, ZSetBatch};
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// Emits a fixed set of `(k, _event_time)` batches.
pub struct FakeSource {
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

/// Collects written Z-sets; `delay` forces engine backpressure.
#[derive(Clone)]
pub struct FakeSink {
    batches: Arc<Mutex<Vec<ZSetBatch>>>,
    delay: Option<Duration>,
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl FakeSink {
    pub fn new(delay: Option<Duration>) -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            delay,
            committed: Arc::new(AtomicBool::new(false)),
            aborted: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn consolidated(&self) -> BTreeMap<Vec<i64>, i64> {
        consolidate(&self.batches.lock().unwrap())
    }

    pub fn len(&self) -> usize {
        self.batches.lock().unwrap().len()
    }

    pub fn committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub fn aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
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
    fn accepts_retractions(&self) -> bool {
        // The accumulator consolidates negative diffs, so it handles the
        // retractions produced by the group-count view.
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Build a two-column (`k`, `_event_time`) batch.
pub fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
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
        next_offset: keys.len() as i64,
        split: 0,
    }
}

/// A distinct trailing row, so observing it proves earlier batches were processed.
pub fn sentinel() -> Vec<i64> {
    vec![999, 1]
}

/// Read a consolidated Z-set of Int64 columns as plain integer rows.
pub fn zset_rows(z: &ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = z
        .batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    (0..z.len())
        .map(|row| columns.iter().map(|c| c.value(row)).collect())
        .collect()
}

fn consolidate(batches: &[ZSetBatch]) -> BTreeMap<Vec<i64>, i64> {
    let mut acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    for z in batches {
        let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
        for (row, diff) in zset_rows(z).into_iter().zip(diffs.values()) {
            *acc.entry(row).or_insert(0) += diff;
        }
    }
    acc.retain(|_, diff| *diff != 0);
    acc
}

fn group_count() -> (String, Plan) {
    (
        "c".into(),
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
}

/// Start an engine over `batches` tapping `sink`.
pub fn start(batches: Vec<SourceBatch>, sink: FakeSink) -> (EngineHandle, FakeSink) {
    let schema = batches[0].batch.schema();
    let spec = SinkSpec {
        view: "c".into(),
        sink: Arc::new(sink.clone()),
    };
    let handle = EngineHandle::start(Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source: Arc::new(FakeSource { schema, batches }),
            watermark: None,
        }])
        .unwrap(),
        views: vec![group_count()],
        sinks: vec![spec],
        checkpoint: None,
        retention: None,
    })
    .unwrap();
    (handle, sink)
}
