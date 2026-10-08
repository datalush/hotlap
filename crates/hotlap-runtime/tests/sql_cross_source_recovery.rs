//! Checkpoint and restart a SQL session joining two resumable sources.
//!
//! The restart declares the sources in the reversed order and resumes both from
//! their captured offsets; a fresh (non-recovered) session could not even open
//! them because the second dataset has dropped the records before the offset.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "cross_source_support/session_recovery.rs"]
mod session_recovery;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array};
use hotlap_connectors::source::Source;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{QueryResult, Session, SessionConfig};

use backend::SharedBackend;
use resumable::Dataset;
use session_recovery::ResumableSessionFactory;

const CREATE_A: &str = "CREATE SOURCE a WITH (connector='inmem') WATERMARK FOR \
     k AS k - INTERVAL '1 s';";
const CREATE_B: &str = "CREATE SOURCE b WITH (connector='inmem') WATERMARK FOR \
     k AS k - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW j AS SELECT a.k FROM a JOIN b ON a.k = b.k;";

fn open(factory: &Arc<ResumableSessionFactory>, backend: &SharedBackend) -> Session {
    let config = SessionConfig::new()
        .with_source_factory(factory.clone())
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        );
    let mut session = Session::open(config).expect("open session");
    session.sql(CREATE_A).expect("create a");
    session.sql(CREATE_B).expect("create b");
    session.sql(VIEW).expect("create view");
    session
}

fn ints(result: QueryResult) -> Vec<i64> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            out.push(column.value(row));
        }
    }
    out.sort_unstable();
    out
}

fn wait_ints(session: &mut Session, want: &[i64]) -> Vec<i64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = ints(session.sql("SELECT k FROM j").expect("view query"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_offsets(factory: &ResumableSessionFactory, a: i64, b: i64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let applied_a = factory.source("a").state().offsets.get(&0) == Some(&a);
        let applied_b = factory.source("b").state().offsets.get(&0) == Some(&b);
        if applied_a && applied_b {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// Run the first session to completion, checkpoint it, and return its factory.
fn first_run(backend: &SharedBackend) -> Arc<ResumableSessionFactory> {
    // `a` has two matching records, `b` one; the view exposes the join row once
    // (the MV snapshot keeps distinct rows, not multiplicities).
    let first = ResumableSessionFactory::new();
    first.declare("a", Dataset::new(vec![vec![1], vec![1]]));
    first.declare("b", Dataset::new(vec![vec![1]]));
    let mut session = open(&first, backend);
    session.sql("START;").expect("first start");
    assert_eq!(wait_ints(&mut session, &[1]), vec![1]);
    assert!(wait_offsets(&first, 2, 1), "both sources must be applied");
    let id = session.checkpoint().expect("checkpoint");
    assert!(id >= 1, "checkpoint must be durable");
    session.shutdown().expect("shutdown");
    first
}

/// Restart with reversed declarations and retention that hides the replayed
/// prefix: only a real resume from the checkpoint can open the sources.
#[test]
fn restart_restores_join_and_seeds_offsets() {
    let backend = SharedBackend::default();
    first_run(&backend);

    let second = ResumableSessionFactory::new();
    second.declare("b", Dataset::new(vec![vec![1]]).with_retention(1));
    second.declare("a", Dataset::new(vec![vec![1], vec![1]]).with_retention(2));
    let mut session = open(&second, &backend);
    session.sql("START;").expect("recovered start");
    assert_eq!(
        wait_ints(&mut session, &[1]),
        vec![1],
        "the restored join must equal the checkpointed one"
    );
    assert!(
        wait_offsets(&second, 2, 1),
        "both sources must be seeded at their captured offsets"
    );
    session.shutdown().expect("shutdown");
}

/// A further restart reads only the record past the offset, so `a` advances to
/// 3 while `b` stays at its captured offset.
#[test]
fn restart_continues_from_captured_offset() {
    let backend = SharedBackend::default();
    first_run(&backend);

    let third = ResumableSessionFactory::new();
    third.declare(
        "a",
        Dataset::new(vec![vec![1], vec![1], vec![1]]).with_retention(2),
    );
    third.declare("b", Dataset::new(vec![vec![1]]).with_retention(1));
    let mut session = open(&third, &backend);
    session
        .sql("START;")
        .expect("recovered start with new batch");
    assert_eq!(wait_ints(&mut session, &[1]), vec![1]);
    assert!(wait_offsets(&third, 3, 1));
    session.shutdown().expect("shutdown");
}
