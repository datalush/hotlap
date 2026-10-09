//! Minimal fixtures for the uncertain-commit tests: a resumable log source and
//! the engine/pipeline setup that captures and restores it.

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

#[path = "../common/backend.rs"]
mod backend;
#[path = "../common/recovery/resumable.rs"]
mod resumable;

pub use backend::SharedBackend;
pub use resumable::{Dataset, ResumableSource};

/// Build a group-count engine over `source`, keeping the pipeline alive.
pub fn engine_with(source: ResumableSource) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source: std::sync::Arc::new(source),
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
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    (hotlap, pipeline)
}

/// Push up to `limit` readable events into `hotlap`, acking each once applied.
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
