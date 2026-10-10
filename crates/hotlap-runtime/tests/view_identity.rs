//! Durable view identity: a saved view must bind to the same declaration on a
//! restart, or recovery must reject the checkpoint before restoring the engine.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/recovery/ops.rs"]
mod ops;
#[path = "common/recovery/resumable.rs"]
mod resumable;

use std::sync::{Arc, Mutex};

use futures::executor::block_on;
use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::Source;
use hotlap_engine::{EngineCore, MetricsRegistry};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use ops::{drain, rows, take};
use resumable::{Dataset, ResumableSource};

/// The fixed log every attempt reads from; retention keeps all records.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]])
        .with_retention(0)
        .with_physical_identity("test/view-identity/log")
}

/// A filter plan selecting the rows whose first column equals `value`.
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

fn sources(source: Arc<dyn Source>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap()
}

/// Build an engine over the fixed log with the given view declarations, in the
/// order the caller lists them (so the declaration order decides the handles).
fn engine(views: Vec<(String, Plan)>) -> (Hotlap, Pipeline) {
    let pipeline = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views,
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    (hotlap, pipeline)
}

/// Seed one real checkpoint over the two views `a` (k=1) and `b` (k=2).
fn seed(backend: &SharedBackend) -> u64 {
    let (mut engine, pipe) = engine(vec![("a".into(), filter(1)), ("b".into(), filter(2))]);
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    take(&mut checkpointer, &engine, &pipe.sources)
}

/// Restart over the seeded checkpoint with `views` as the declaration order.
fn restart(
    backend: &SharedBackend,
    views: Vec<(String, Plan)>,
) -> (Hotlap, Result<(), ConnectorError>) {
    let (mut engine, pipe) = engine(views);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let result = block_on(Recovery::start(
        &mut engine,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .map(|_stream| ());
    (engine, result)
}

/// Inverting the declaration order swaps the two numeric handles; recovery must
/// reject the checkpoint or rebind each name to its own plan's output.
#[test]
fn inverted_view_declaration_binds_each_name_to_its_own_plan() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (mut engine, result) = restart(
        &backend,
        vec![("b".into(), filter(2)), ("a".into(), filter(1))],
    );
    match result {
        Err(ConnectorError::Unsupported(_)) => {}
        Ok(()) => {
            assert_eq!(rows(&engine.snapshot("a").unwrap()), vec![vec![1]]);
            assert_eq!(rows(&engine.snapshot("b").unwrap()), vec![vec![2]]);
        }
        Err(error) => panic!("unexpected recovery error: {error}"),
    }
}

/// The unchanged declaration order must keep restoring both views.
#[test]
fn preserved_view_declaration_restores_both_views() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (mut engine, result) = restart(
        &backend,
        vec![("a".into(), filter(1)), ("b".into(), filter(2))],
    );
    result.expect("a matching declaration must recover");
    assert_eq!(rows(&engine.snapshot("a").unwrap()), vec![vec![1]]);
    assert_eq!(rows(&engine.snapshot("b").unwrap()), vec![vec![2]]);
}

/// The same name with a changed plan must not bind to the saved view.
#[test]
fn changed_view_plan_is_rejected() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (_, result) = restart(
        &backend,
        vec![("a".into(), filter(7)), ("b".into(), filter(2))],
    );
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "a changed plan must be rejected"
    );
}

/// Dropping the first view shifts every later handle, so `b` would bind to the
/// saved state of `a`; recovery must reject the checkpoint.
#[test]
fn dropping_a_view_is_rejected() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (_, result) = restart(&backend, vec![("b".into(), filter(2))]);
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "a shifted handle must not bind unrelated saved state"
    );
}

/// Dropping the last declared view leaves saved state with no declaration; the
/// namespace must match exactly, so recovery rejects it.
#[test]
fn dropping_the_last_view_is_rejected() {
    let backend = SharedBackend::default();
    let (mut engine, pipe) = engine(vec![
        ("a".into(), filter(1)),
        ("b".into(), filter(2)),
        ("c".into(), filter(1)),
    ]);
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    take(&mut checkpointer, &engine, &pipe.sources);

    let (_, result) = restart(
        &backend,
        vec![("a".into(), filter(1)), ("b".into(), filter(2))],
    );
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "an undeclared saved view must not be silently restored"
    );
}
