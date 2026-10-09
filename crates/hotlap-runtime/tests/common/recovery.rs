//! Pipeline and engine fixtures for the recovery tests.

use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "backend.rs"]
mod backend;
#[path = "recovery/ops.rs"]
mod ops;
#[path = "recovery/resumable.rs"]
mod resumable;

pub use backend::SharedBackend;
pub use ops::{drain, rows, take};
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
