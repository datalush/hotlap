//! Minimal fixtures for the uncertain-commit tests: a resumable log source, two
//! distinct views and the bag view of a Z-set.

use std::collections::BTreeMap;

use arrow::array::Int64Array;
use hotlap::{AggSpec, Hotlap, InputId, Plan, ZSetBatch};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, SourceEvent, Sources};

#[path = "../common/backend.rs"]
mod backend;
#[path = "../common/recovery/resumable.rs"]
mod resumable;

pub use backend::SharedBackend;
pub use resumable::{Dataset, ResumableSource};

/// The raw-row view name, fed to its own sink.
pub const ROWS: &str = "rows";
/// The group-count view name, fed to the other sink.
pub const COUNT: &str = "count";

/// A single-entry source set named like the pipeline, over any source.
pub fn sources_of(source: std::sync::Arc<dyn Source>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap()
}

/// Build an engine over `source` with the two distinct views, keeping the pipeline.
pub fn engine_with(source: ResumableSource) -> (Hotlap, Pipeline) {
    engine_for(sources_of(std::sync::Arc::new(source)))
}

/// Build an engine over an explicit source set with the two distinct views.
pub fn engine_for(sources: Sources) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        sources,
        views: vec![
            (
                ROWS.into(),
                Plan::Project {
                    input: Box::new(Plan::Source(InputId(0))),
                    cols: vec![0],
                },
            ),
            (
                COUNT.into(),
                Plan::GroupAggregate {
                    input: Box::new(Plan::Source(InputId(0))),
                    key: vec![0],
                    aggs: vec![AggSpec::count()],
                },
            ),
        ],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    for name in [ROWS, COUNT] {
        hotlap.tap_view(name).unwrap();
    }
    (hotlap, pipeline)
}

/// Ingest `event` and acknowledge it through the source that produced it.
pub fn ingest(hotlap: &mut Hotlap, sources: &Sources, event: &SourceEvent) {
    pipeline::ingest_event(hotlap, sources, event).unwrap();
    sources
        .get(event.input)
        .unwrap()
        .source
        .commit(event.batch.split, event.batch.next_offset)
        .unwrap();
}

/// The nonzero rows of a Z-set as `row -> multiplicity`.
pub fn bag(zset: &ZSetBatch) -> BTreeMap<Vec<i64>, i64> {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out = BTreeMap::new();
    for index in 0..zset.len() {
        let weight = diffs.value(index);
        if weight != 0 {
            let row: Vec<i64> = columns.iter().map(|column| column.value(index)).collect();
            out.insert(row, weight);
        }
    }
    out
}
