//! Resumable source fixture for the recovery tests.
//!
//! The source replays a fixed log from a split's `start` offset and records its
//! read position, so it can be reopened at a captured checkpoint offset. A
//! `retained_from` boundary models a broker that has dropped older records, to
//! exercise the insufficient-retention error.

use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

/// A shared, immutable log plus the earliest offset still retained.
#[derive(Clone)]
pub struct Dataset {
    pub batches: Arc<Vec<Vec<i64>>>,
    pub retained_from: usize,
}

impl Dataset {
    pub fn new(batches: Vec<Vec<i64>>) -> Self {
        Self {
            batches: Arc::new(batches),
            retained_from: 0,
        }
    }

    pub fn with_retention(mut self, retained_from: usize) -> Self {
        self.retained_from = retained_from;
        self
    }
}

/// A source whose read position is observable and resumable by offset.
pub struct ResumableSource {
    schema: SchemaRef,
    dataset: Dataset,
    progress: Arc<Mutex<SourceState>>,
}

impl ResumableSource {
    pub fn new(dataset: Dataset) -> Self {
        Self {
            schema: schema(),
            dataset,
            progress: Arc::new(Mutex::new(SourceState::default())),
        }
    }
}

impl Source for ResumableSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let start = self.progress.lock().unwrap().offsets.get(&0).copied();
        Ok(vec![Split {
            id: 0,
            start: start.unwrap_or(0),
        }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let start = split.start.max(0) as usize;
        let retained = self.dataset.retained_from;
        let schema = self.schema.clone();
        let data = Arc::clone(&self.dataset.batches);
        let stream = futures::stream::iter(start..data.len()).then(move |index| {
            let schema = schema.clone();
            let rows = data[index].clone();
            async move {
                if index < retained {
                    return Err(ConnectorError::Unsupported(format!(
                        "offset {index} is older than retention start {retained}"
                    )));
                }
                let array: ArrayRef = Arc::new(Int64Array::from(rows));
                let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                Ok(SourceBatch {
                    batch,
                    base_offset: index as i64,
                    next_offset: index as i64 + 1,
                    split: 0,
                })
            }
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, _split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.progress.lock().unwrap().offsets.insert(0, offset);
        Ok(())
    }

    fn state(&self) -> SourceState {
        self.progress.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        for (id, offset) in &state.offsets {
            if (*offset as usize) < self.dataset.retained_from {
                return Err(ConnectorError::Unsupported(format!(
                    "split {id} offset {offset} is older than retention start {}",
                    self.dataset.retained_from
                )));
            }
        }
        let mut splits = self.splits()?;
        for split in &mut splits {
            if let Some(offset) = state.offsets.get(&split.id) {
                split.start = *offset;
            }
        }
        Ok(splits)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

#[path = "backend.rs"]
mod backend;

pub use backend::SharedBackend;

/// Build a single-entry [`Sources`] set over `source`, named like the pipeline.
pub fn sources(source: ResumableSource) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: std::sync::Arc::new(source),
        watermark: None,
    }])
    .unwrap()
}

/// Build a group-count pipeline over `source`, optionally checkpointing.
pub fn pipeline(source: ResumableSource, checkpoint: Option<CheckpointConfig>) -> Pipeline {
    Pipeline {
        sources: sources(source),
        views: vec![(
            "c".into(),
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )],
        sinks: vec![],
        checkpoint,
        retention: None,
    }
}

/// Build a group-count engine over `source`, with the pipeline kept alive.
pub fn engine_with(source: ResumableSource) -> (Hotlap, Pipeline) {
    let pipeline = pipeline(source, None);
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    (hotlap, pipeline)
}

/// Take a checkpoint synchronously and return its id.
pub fn take(checkpointer: &mut Checkpointer, engine: &Hotlap, sources: &Sources) -> u64 {
    futures::executor::block_on(checkpointer.take(engine, sources)).unwrap()
}

/// Push up to `limit` readable events from `stream` into `hotlap`, acking each
/// one through the source that produced it once it is applied (mirrors runtime).
pub fn drain(hotlap: &mut Hotlap, sources: &Sources, stream: &mut InputStream, limit: usize) {
    let mut pushed = 0;
    while pushed < limit {
        match futures::executor::block_on(stream.next()) {
            Some(Ok(event)) => {
                pipeline::ingest_event(hotlap, sources, &event).unwrap();
                sources
                    .get(event.input)
                    .unwrap()
                    .source
                    .commit(event.batch.split, event.batch.next_offset)
                    .unwrap();
                pushed += 1;
            }
            Some(Err(error)) => panic!("unexpected source error: {error}"),
            None => break,
        }
    }
}

/// Materialize a Z-set as sorted integer rows.
pub fn rows(zset: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut out: Vec<Vec<i64>> = (0..zset.len())
        .map(|row| columns.iter().map(|column| column.value(row)).collect())
        .collect();
    out.sort();
    out
}
