#[path = "common/session_transactional_fixture.rs"]
mod fixture;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use fixture::{
    TestBackend, config, corrupt_pending, declare, remove_pending, rows, seed, wait_for_output,
};
use hotlap::state::StateBackend;
use hotlap_runtime::{Session, SessionError};
use hotlap_sql::SqlError;

#[test]
fn transactional_pending_retry_restores_and_publishes_a_new_checkpoint() {
    let backend = TestBackend::default();
    seed(&backend);
    corrupt_pending(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let spies = Arc::new(Mutex::new(Vec::new()));
    let mut session = Session::open(config(
        &backend,
        reads.clone(),
        creates.clone(),
        true,
        Arc::clone(&spies),
    ))
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("unsafe corrupt pending state must be rejected"),
        Err(error) => error,
    };
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());

    remove_pending(&backend);
    session
        .sql("START;")
        .expect("same-session retry retains config");
    wait_for_output(&session);
    assert_eq!(rows(&session), vec![vec![1]]);
    let probes = spies.lock().unwrap();
    assert_eq!(probes[0].resumed(), 1);
    assert_eq!(probes[0].offset(), Some(1));
    assert_eq!(session.checkpoint().unwrap(), 2);
    assert_eq!(
        backend.get(b"checkpoint/2/valid").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        backend.get(b"checkpoint/reserved").unwrap(),
        Some(2_u64.to_le_bytes().to_vec())
    );
    drop(probes);
    session.shutdown().expect("shutdown retry");
}

#[test]
fn an_actual_transactional_sink_cannot_contradict_preflight() {
    let backend = TestBackend::default();
    seed(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session = Session::open(config(
        &backend,
        reads.clone(),
        creates.clone(),
        false,
        Arc::new(Mutex::new(Vec::new())),
    ))
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("a transactional sink contradicting preflight must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(creates.load(Ordering::SeqCst), 2);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}
