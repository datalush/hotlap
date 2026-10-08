use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{Hotlap, InputId, Plan};
use hotlap_engine::EngineCore;

use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};

struct FakeSource {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

impl Source for FakeSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
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
        next_offset: keys.len() as i64,
        split: 0,
    }
}

/// Read a consolidated Z-set of Int64 columns as sorted integer rows.
fn snap_rows(h: &mut Hotlap, view: &str) -> Vec<Vec<i64>> {
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

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

fn group_count(schema: SchemaRef, batches: Vec<SourceBatch>) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        input: "in".into(),
        source: Box::new(FakeSource { schema, batches }),
        watermark: None,
        views: vec![(
            "c".into(),
            Plan::GroupCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    (open(), pipeline)
}

#[tokio::test]
async fn fake_source_feeds_view() {
    let source = batch(&[1, 1, 2], &[10, 10, 10]);
    let schema = source.batch.schema();
    let (mut hotlap, pipeline) = group_count(schema, vec![source, batch(&[2, 3], &[20, 20])]);
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let mut stream = pipeline::merged_stream(pipeline.source.as_ref()).unwrap();
    while let Some(item) = stream.next().await {
        pipeline::ingest(&mut hotlap, "in", &item.unwrap()).unwrap();
    }
    assert_eq!(
        snap_rows(&mut hotlap, "c"),
        vec![vec![1, 2], vec![2, 2], vec![3, 1]]
    );
}

#[tokio::test]
async fn empty_batch_is_a_noop() {
    let good = batch(&[1], &[10]);
    let empty = SourceBatch {
        batch: RecordBatch::new_empty(good.batch.schema()),
        base_offset: 0,
        next_offset: 0,
        split: 0,
    };
    let (mut hotlap, pipeline) = group_count(good.batch.schema(), vec![good, empty]);
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let mut stream = pipeline::merged_stream(pipeline.source.as_ref()).unwrap();
    while let Some(item) = stream.next().await {
        pipeline::ingest(&mut hotlap, "in", &item.unwrap()).unwrap();
    }
    assert_eq!(snap_rows(&mut hotlap, "c"), vec![vec![1, 1]]);
}

#[tokio::test]
async fn two_splits_merge() {
    struct TwoSplit {
        schema: SchemaRef,
        a: Vec<SourceBatch>,
        b: Vec<SourceBatch>,
    }
    impl Source for TwoSplit {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
            Ok(vec![Split { id: 0, start: 0 }, Split { id: 1, start: 0 }])
        }
        fn read(&self, s: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
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
    let a = batch(&[1, 1], &[10, 10]);
    let b = batch(&[1, 2], &[10, 10]);
    let pipeline = Pipeline {
        input: "in".into(),
        source: Box::new(TwoSplit {
            schema: a.batch.schema(),
            a: vec![a],
            b: vec![b],
        }),
        watermark: None,
        views: vec![(
            "c".into(),
            Plan::GroupCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = open();
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let mut stream = pipeline::merged_stream(pipeline.source.as_ref()).unwrap();
    while let Some(item) = stream.next().await {
        pipeline::ingest(&mut hotlap, "in", &item.unwrap()).unwrap();
    }
    assert_eq!(snap_rows(&mut hotlap, "c"), vec![vec![1, 3], vec![2, 1]]);
}

#[tokio::test]
async fn window_without_watermark_is_rejected_at_push() {
    // No event-time -> the engine must reject a windowed view.
    let source = FakeSource {
        schema: batch(&[0], &[0]).batch.schema(),
        batches: vec![batch(&[1], &[10])],
    };
    let pipeline = Pipeline {
        input: "in".into(),
        source: Box::new(source),
        watermark: None,
        views: vec![(
            "w".into(),
            Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                time_col: 1,
                size: 10,
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = open();
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let mut stream = pipeline::merged_stream(pipeline.source.as_ref()).unwrap();
    let sb = stream.next().await.unwrap().unwrap();
    assert!(pipeline::ingest(&mut hotlap, "in", &sb).is_err());
}
