//! `Recovery::resume` re-checks the named view identity itself, so a public
//! caller cannot restore state under a renamed declaration.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/recovery/ops.rs"]
mod ops;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "common/spy.rs"]
mod spy;

use std::sync::Arc;

use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpoint, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use ops::{drain, rows, take};
use resumable::{Dataset, ResumableSource};
use spy::SpySource;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]])
        .with_retention(0)
        .with_physical_identity("test/view-identity-resume/log")
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

/// Seed a checkpoint over one view and return the decoded checkpoint.
fn seed(backend: &SharedBackend) -> Checkpoint {
    let (mut engine, pipe) = engine(vec![("a".into(), filter(1))]);
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    let mut writer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let id = take(&mut writer, &engine, &pipe.sources);
    Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .read(id)
        .unwrap()
}

/// A spy source over the fixed log, named like the declared source.
fn spy_sources() -> (Sources, Arc<SpySource>) {
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
    (sources, spy)
}

/// A matching declaration resumes and reopens the source at its captured
/// offset, restoring the named view's output.
#[test]
fn resume_reopens_a_matching_declaration_at_its_offset() {
    let backend = SharedBackend::default();
    let checkpoint = seed(&backend);
    let (mut hotlap, _) = engine(vec![("a".into(), filter(1))]);
    let (sources, spy) = spy_sources();

    let _stream = Recovery::resume(&mut hotlap, &sources, &checkpoint)
        .expect("a matching resume must succeed");
    assert_eq!(spy.resumed(), 1);
    assert_eq!(
        spy.offset(),
        Some(2),
        "the source reopens at the captured offset"
    );
    assert_eq!(rows(&hotlap.snapshot("a").unwrap()), vec![vec![1]]);
}

/// A renamed view must be rejected before the source is reopened.
#[test]
fn resume_rejects_a_renamed_view_before_reopening_sources() {
    let backend = SharedBackend::default();
    let checkpoint = seed(&backend);
    // Same plan under a different name; only the persisted registry can tell.
    let (mut hotlap, _) = engine(vec![("b".into(), filter(1))]);
    let (sources, spy) = spy_sources();

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
