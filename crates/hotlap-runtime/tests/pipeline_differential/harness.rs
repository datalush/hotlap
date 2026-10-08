//! Source fixtures for the pipeline differential tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// A source replaying prebuilt batches on split 0.
pub struct FakeSource {
    pub schema: SchemaRef,
    pub batches: Vec<SourceBatch>,
}

impl Source for FakeSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items: Vec<Result<SourceBatch, _>> = self.batches.iter().cloned().map(Ok).collect();
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

/// A source feeding one batch list per split (0 and 1).
pub struct TwoSplit {
    pub schema: SchemaRef,
    pub a: Vec<SourceBatch>,
    pub b: Vec<SourceBatch>,
}

impl Source for TwoSplit {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }, Split { id: 1, start: 0 }])
    }
    fn read(&self, s: &Split) -> Result<SourceStream, ConnectorError> {
        let src = if s.id == 0 { &self.a } else { &self.b };
        let items: Vec<Result<SourceBatch, _>> = src.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
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

/// Open an empty engine.
pub fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

/// Wrap `source` as the single input of a group-count view `c`.
pub fn group_count(source: Arc<dyn Source>) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![(
            "c".into(),
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    (open(), pipeline)
}

/// Read a consolidated Z-set of Int64 columns as sorted integer rows.
pub fn snap_rows(h: &mut Hotlap, view: &str) -> Vec<Vec<i64>> {
    let z = h.snapshot(view).unwrap();
    let columns: Vec<&Int64Array> = z
        .batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut rows: Vec<Vec<i64>> = (0..z.len())
        .map(|row| columns.iter().map(|c| c.value(row)).collect())
        .collect();
    rows.sort();
    rows
}
