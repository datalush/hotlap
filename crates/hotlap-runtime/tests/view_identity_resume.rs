//! `Recovery::resume` re-checks the named view identity itself, so a public
//! caller cannot restore state under a renamed declaration.

#[path = "common/recovery.rs"]
mod recovery;
#[path = "common/spy.rs"]
mod spy;

use std::sync::Arc;

use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use recovery::{Dataset, ResumableSource, SharedBackend};
use spy::SpySource;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]]).with_retention(0)
}

fn filter(value: i64) -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(value),
        },
    }
}

fn engine(views: Vec<(String, Plan)>) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source: Arc::new(ResumableSource::new(log())),
            watermark: None,
        }])
        .unwrap(),
        views,
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    (hotlap, pipeline)
}

#[test]
fn resume_rejects_a_renamed_view_before_reopening_sources() {
    let backend = SharedBackend::default();
    let (mut seed_engine, seed_pipe) = engine(vec![("a".into(), filter(1))]);
    let mut stream = seed_pipe.sources.stream().unwrap();
    recovery::drain(&mut seed_engine, &seed_pipe.sources, &mut stream, 2);
    let mut writer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let id = recovery::take(&mut writer, &seed_engine, &seed_pipe.sources);
    let checkpoint = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .read(id)
        .unwrap();

    // The restart declares the same plan under a different name, so only the
    // persisted registry can tell the name was renamed.
    let (mut hotlap, _) = engine(vec![("b".into(), filter(1))]);
    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let as_source: Arc<dyn Source> = spy.clone();
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: as_source,
        watermark: None,
    }])
    .unwrap();

    let result = Recovery::resume(&mut hotlap, &sources, &checkpoint);
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "a renamed view must be rejected"
    );
    assert_eq!(
        spy.resumed(),
        0,
        "the source must not be reopened before the identity check"
    );
}
