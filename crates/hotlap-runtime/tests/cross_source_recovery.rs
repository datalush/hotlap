//! Checkpoint and restart a cross-source SQL join against full recomputation.
//!
//! The first run captures `a` offset 2 and `b` offset 1. The restart declares
//! the sources in reversed SQL order, resumes each from its own offset, and the
//! grown result must equal a recompute of exactly the records consumed.

#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/recovery.rs"]
mod recovery;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "cross_source_support/session_recovery.rs"]
mod session_recovery;

use std::sync::Arc;
use std::time::{Duration, Instant};

use hotlap_connectors::source::Source;
use hotlap_runtime::Session;

use backend::SharedBackend;
use recovery::{Order, recompute_keys, session, zset_keys};
use resumable::Dataset;
use session_recovery::ResumableSessionFactory;

fn wait_keys(session: &Session, want: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = zset_keys(&session.snapshot("j").expect("MV snapshot"));
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

/// Run `a=[[1],[1]]`, `b=[[1]]`, checkpoint it and return the factory.
fn first_run(backend: &SharedBackend) -> Arc<ResumableSessionFactory> {
    let factory = ResumableSessionFactory::new();
    factory.declare("a", Dataset::new(vec![vec![1], vec![1]]));
    factory.declare("b", Dataset::new(vec![vec![1]]));
    let mut session = session(factory.clone(), Box::new(backend.clone()), Order::Ab);
    session.sql("START;").expect("first start");

    // a has two matching records and b one: the join carries diff 2.
    assert_eq!(wait_keys(&session, &[(1, 2)]), vec![(1, 2)]);
    assert!(wait_offsets(&factory, 2, 1), "a=2, b=1 must be applied");
    assert!(session.checkpoint().expect("checkpoint") >= 1);
    session.shutdown().expect("shutdown");
    factory
}

#[test]
fn restart_resumes_each_source_and_matches_recomputation() {
    let backend = SharedBackend::default();
    first_run(&backend);

    // Reversed SQL declaration order and retention that hides the replayed
    // prefix: only a real resume from the checkpoint can read both new lots.
    let second = ResumableSessionFactory::new();
    second.declare("b", Dataset::new(vec![vec![1], vec![2]]).with_retention(1));
    second.declare(
        "a",
        Dataset::new(vec![vec![1], vec![1], vec![2]]).with_retention(2),
    );
    let mut session = session(second.clone(), Box::new(backend.clone()), Order::Ba);
    session.sql("START;").expect("recovered start");

    // Every consumed record: a = k1, k1, k2 and b = k1, k2.
    let a_rows = [(1, 1), (1, 1), (2, 1)];
    let b_rows = [(1, 1), (2, 1)];
    let expected = recompute_keys(&a_rows, &b_rows);
    assert_eq!(expected, vec![(1, 2), (2, 1)]);
    assert_eq!(wait_keys(&session, &expected), expected);

    // Independent resume: a advanced 2 -> 3, b advanced 1 -> 2.
    assert!(wait_offsets(&second, 3, 2), "independent offsets a=3, b=2");
    session.shutdown().expect("shutdown");
}
