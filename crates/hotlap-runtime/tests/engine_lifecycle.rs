use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use hotlap::{AggSpec, InputId, Plan};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "engine_lifecycle/harness.rs"]
mod harness;

use harness::{FailingSource, OneBatchSource, PendingSource, zset_rows};

/// A single input pipeline over `source` with the given views.
fn pipeline(
    source: Arc<dyn hotlap_connectors::source::Source>,
    views: Vec<(String, Plan)>,
) -> Pipeline {
    Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views,
        sinks: vec![],
        checkpoint: None,
        retention: None,
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

#[test]
fn start_snapshot_shutdown() {
    let handle = EngineHandle::start(pipeline(
        Arc::new(PendingSource { schema: schema() }),
        vec![],
    ))
    .unwrap();
    // No view registered: snapshot of an unknown view errors, but the handle must respond.
    assert!(handle.snapshot("nope").is_err());
    // The derived handle can read snapshots too, without owning the engine.
    let snap = handle.snapshot_handle();
    assert!(snap.snapshot("nope").is_err());
    assert!(snap.last_error().unwrap().is_none());
    handle.shutdown().unwrap();
}

#[test]
fn snapshot_handle_reads_a_built_view() {
    let handle = EngineHandle::start(pipeline(
        Arc::new(OneBatchSource { schema: schema() }),
        vec![(
            "c".into(),
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )],
    ))
    .unwrap();
    let snap = handle.snapshot_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !snap.is_built() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(snap.is_built(), "engine never built the dataflow");
    assert_eq!(
        zset_rows(&snap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 1]]
    );
    handle.shutdown().unwrap();
}

#[test]
fn source_error_is_surfaced() {
    let handle = EngineHandle::start(pipeline(
        Arc::new(FailingSource { schema: schema() }),
        vec![],
    ))
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut raised = false;
    while Instant::now() < deadline {
        if handle.last_error().unwrap().is_some() {
            raised = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(raised, "engine did not surface the source error");
    handle.shutdown().unwrap();
}

#[test]
fn start_reports_setup_failure() {
    // A view referencing an unknown input fails during setup.
    let result = EngineHandle::start(pipeline(
        Arc::new(PendingSource { schema: schema() }),
        vec![(
            "c".into(),
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(99))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )],
    ));
    assert!(result.is_err());
}
