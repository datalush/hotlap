//! Cross-source fail-stop at the SQL session boundary.
//!
//! A read failure in one source stops the whole session and forbids later
//! checkpoints, while a later row that *would* change the join is neither read
//! nor published. The SELECT surface lists distinct rows, so the oracle compares
//! the raw MV snapshot (which keeps multiplicities) against a full recompute of
//! the records that were actually acked.
//!
//! The read/push/ack mechanisms at the runtime-pipeline boundary are already
//! covered by `runtime_fail_stop.rs` and `runtime_fail_stop_modes.rs`; this file
//! only adds the embedded-session boundary, with every blocking `Session` call
//! bounded by a deadline so a deadlock fails instead of hanging the suite.

#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/factory.rs"]
mod factory;
#[path = "cross_source_support/sql_source.rs"]
mod sql_source;
#[path = "cross_source_support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig};

use backend::SharedBackend;
use factory::{CrossSourceFactory, SourceSpec};
use support::rows::{
    append_batch, ints, query_rows, recompute, schema_left, schema_right, send, wait_commits,
    zset_tuples,
};

const CREATE_A: &str = "CREATE SOURCE a WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const CREATE_B: &str = "CREATE SOURCE b WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW j AS SELECT a.k, a.lv, b.rv \
     FROM a JOIN b ON a.k = b.k;";

/// Run `case` on a worker and panic if it does not finish in `timeout`, so a
/// blocking `Session` call fails the test instead of hanging the suite.
fn bounded(timeout: Duration, case: impl FnOnce() + Send + 'static) {
    let (done, wait) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(case));
        let _ = done.send(result);
    });
    match wait.recv_timeout(timeout) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(_) => panic!("case did not finish within {timeout:?} (possible deadlock)"),
    }
}

/// A checkpointed session over declared sources `a` and `b` and the join view.
fn open(factory: &Arc<CrossSourceFactory>, backend: SharedBackend) -> Session {
    let config = SessionConfig::new()
        .with_source_factory(factory.clone())
        .with_checkpoint(Duration::from_secs(3600), DEFAULT_RETAIN, Box::new(backend));
    let mut session = Session::open(config).expect("open session");
    session.sql(CREATE_A).expect("create a");
    session.sql(CREATE_B).expect("create b");
    session.sql(VIEW).expect("create view");
    session.sql("START;").expect("start");
    session
}

/// An append batch over `schema` from `(key, value, event_time)` rows.
fn rows(schema: SchemaRef, triples: &[(i64, i64, i64)]) -> SourceBatch {
    let keys: Vec<i64> = triples.iter().map(|row| row.0).collect();
    let values: Vec<i64> = triples.iter().map(|row| row.1).collect();
    let times: Vec<i64> = triples.iter().map(|row| row.2).collect();
    append_batch(schema, vec![ints(&keys), ints(&values), ints(&times)])
}

/// Poll until the session rejects a checkpoint, and return that error.
fn wait_failure(session: &Session) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match session.checkpoint() {
            Err(error) => return error.to_string(),
            Ok(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            Ok(_) => panic!("session did not fail within the deadline"),
        }
    }
}

#[test]
fn read_error_stops_the_session_and_keeps_only_acked_rows() {
    bounded(Duration::from_secs(10), || {
        let factory = CrossSourceFactory::new();
        factory.declare("a", SourceSpec::new(schema_left(), 2));
        factory.declare("b", SourceSpec::new(schema_right(), 2));
        let mut session = open(&factory, SharedBackend::default());

        send(&factory, "a", rows(schema_left(), &[(1, 10, 0)]));
        send(&factory, "b", rows(schema_right(), &[(1, 20, 0)]));
        assert!(
            wait_commits(&factory, "a", 1) && wait_commits(&factory, "b", 1),
            "both sources must ack the first lot"
        );

        factory.senders("a")[0]
            .send(Err(ConnectorError::Infrastructure("read boom".into())))
            .expect("source channel open");
        let error = wait_failure(&session);
        assert!(
            error.contains("runtime failure"),
            "unexpected error: {error}"
        );

        // This later `k=1` row would change the join if it were applied. The
        // snapshot below shows it is not published in the immediate observation.
        // The deterministic serve-loop unit test (`engine/serve/tests.rs`) is
        // what proves the event is never polled; here the session stays
        // responsive to commands after the failure.
        send(&factory, "b", rows(schema_right(), &[(1, 999, 0)]));
        assert_eq!(
            factory.source("b").commits().len(),
            1,
            "no source may continue after a fail-stop"
        );

        let snapshot = session.snapshot("j").expect("MV snapshot");
        assert_eq!(
            zset_tuples(&snapshot),
            recompute(&[(1, 10, 1)], &[(1, 20, 1)]),
            "only the acked lot may be published"
        );
        let select = session.sql("SELECT k, lv, rv FROM j").expect("select");
        assert_eq!(query_rows(select), vec![vec![1, 10, 20]]);
        session.shutdown().expect("shutdown");
    });
}
