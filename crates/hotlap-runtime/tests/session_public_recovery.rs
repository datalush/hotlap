//! Public session START recovers the fallback body and its applied offset.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "common/spy.rs"]
mod spy;
#[path = "session_public_recovery/support.rs"]
mod support;
#[path = "common/watermarked_spy.rs"]
mod watermarked_spy;

use std::sync::{Arc, Mutex, mpsc};

use arrow::array::Int64Array;
use backend::SharedBackend;
use hotlap::state::StateBackend;
use hotlap_runtime::Session;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR k AS \
     k - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 1;";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM a;";

fn declare(session: &mut Session) {
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW).expect("create view");
    session.sql(SINK).expect("create non-transactional sink");
}

fn rows(session: &Session) -> Vec<Vec<i64>> {
    let snapshot = session.snapshot("a").unwrap();
    let values = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..snapshot.len())
        .map(|index| vec![values.value(index)])
        .collect()
}

fn add_corrupt_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let body = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    writer
        .put(b"checkpoint/2/engine", b"HLSR\x02".to_vec())
        .unwrap();
}

#[test]
fn public_start_restores_valid_fallback_output_and_offset() {
    let backend = SharedBackend::default();
    let initial_spies = Arc::new(Mutex::new(Vec::new()));
    let (release_source, source_gate) = tokio::sync::oneshot::channel();
    let (first_change, output) = mpsc::channel();
    let mut initial = Session::open(support::config(
        &backend,
        initial_spies,
        Some(Arc::new(Mutex::new(Some(source_gate)))),
        Some(first_change),
    ))
    .unwrap();
    declare(&mut initial);
    initial.sql("START;").unwrap();
    let before_first_push = initial.snapshot("a").unwrap();
    assert!(before_first_push.is_empty());
    assert_eq!(before_first_push.batch.num_columns(), 0);
    let _ = release_source.send(());
    support::wait_for_output(output);
    assert_eq!(rows(&initial), vec![vec![1]]);
    assert_eq!(initial.checkpoint().unwrap(), 1);
    initial.shutdown().unwrap();
    add_corrupt_pending(&backend);

    let restart_spies = Arc::new(Mutex::new(Vec::new()));
    let mut restarted = Session::open(support::config(
        &backend,
        Arc::clone(&restart_spies),
        None,
        None,
    ))
    .unwrap();
    declare(&mut restarted);
    restarted
        .sql("START;")
        .expect("public START recovers valid fallback");

    assert_eq!(rows(&restarted), vec![vec![1]]);
    let spy = Arc::clone(&restart_spies.lock().unwrap()[0]);
    assert_eq!(spy.resumed(), 1);
    assert_eq!(spy.offset(), Some(1));
    restarted.shutdown().unwrap();
}
