//! Pipeline and engine fixtures for the recovery tests.

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

#[path = "backend.rs"]
mod backend;
#[path = "recovery/resumable.rs"]
mod resumable;

pub use backend::SharedBackend;
pub use resumable::{Dataset, ResumableSource};

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
