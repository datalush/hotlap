//! Session view identity: an incompatible saved view registry must be rejected
//! before any sink writer or source stream opens.

#[path = "common/backend.rs"]
mod backend;
#[path = "sql_session_rejected_start/factories.rs"]
mod factories;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use backend::SharedBackend;
use factories::{CountedFactory, ToggleFactory};
use hotlap::state::StateBackend;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SessionError};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW_A: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 1;";
const VIEW_A_CHANGED: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 7;";
const VIEW_B: &str = "CREATE MATERIALIZED VIEW b AS SELECT k FROM src WHERE k = 2;";
const VIEW_C: &str = "CREATE MATERIALIZED VIEW c AS SELECT k FROM src WHERE k = 1;";
const SINK_A: &str = "CREATE SINK outa WITH (connector='inmem') AS SELECT * FROM a;";
const SINK_B: &str = "CREATE SINK outb WITH (connector='inmem') AS SELECT * FROM b;";
const SINK_C: &str = "CREATE SINK outc WITH (connector='inmem') AS SELECT * FROM c;";

/// A config whose source and sink factories count every open attempt.
fn config(backend: &SharedBackend) -> (SessionConfig, Arc<AtomicU32>, Arc<AtomicU32>) {
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let accepts = Arc::new(AtomicBool::new(true));
    let config = SessionConfig::new()
        .with_source_factory(Arc::new(CountedFactory {
            reads: Arc::clone(&reads),
            starts: Arc::default(),
        }))
        .with_sink_factory(Arc::new(ToggleFactory {
            creates: Arc::clone(&creates),
            accepts,
            writes: Arc::default(),
        }))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        );
    (config, reads, creates)
}

/// Seed a real checkpoint binding `a` then `b` to handles 0 and 1.
fn seed(backend: &SharedBackend) {
    let (config, _reads, _creates) = config(backend);
    let mut session = Session::open(config).expect("open session");
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session.sql("START;").expect("start");
    session.checkpoint().expect("checkpoint");
    session.shutdown().expect("shutdown");
}

/// Declare `views` and `sinks` in order, then `START` and report the effect
/// counters observed at the failure boundary.
fn start_with(backend: &SharedBackend, views: &[&str], sinks: &[&str]) -> (SessionError, u32, u32) {
    let (config, reads, creates) = config(backend);
    let mut session = Session::open(config).expect("open session");
    session.sql(SOURCE).expect("create source");
    for view in views {
        session.sql(view).expect("create view");
    }
    for sink in sinks {
        session.sql(sink).expect("create sink");
    }
    let error = match session.sql("START;") {
        Ok(_) => panic!("a mismatched view registry must be rejected"),
        Err(error) => error,
    };
    (
        error,
        creates.load(Ordering::SeqCst),
        reads.load(Ordering::SeqCst),
    )
}

#[test]
fn a_reordered_view_declaration_is_rejected_before_any_effect() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (error, creates, reads) = start_with(&backend, &[VIEW_B, VIEW_A], &[SINK_A, SINK_B]);
    assert!(
        matches!(error, SessionError::Sql(SqlError::Unsupported(_))),
        "an inverted view order must be rejected explicitly: {error:?}"
    );
    assert_eq!(creates, 0, "no sink writer may open");
    assert_eq!(reads, 0, "no source may open");
}

#[test]
fn a_renamed_view_is_rejected_before_any_effect() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (error, creates, reads) = start_with(&backend, &[VIEW_C, VIEW_B], &[SINK_B, SINK_C]);
    assert!(
        matches!(error, SessionError::Sql(SqlError::Unsupported(_))),
        "an unknown view name must be rejected explicitly: {error:?}"
    );
    assert_eq!(creates, 0, "no sink writer may open");
    assert_eq!(reads, 0, "no source may open");
}

#[test]
fn the_original_view_order_still_starts() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (config, _reads, _creates) = config(&backend);
    let mut session = Session::open(config).expect("open session");
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session
        .sql("START;")
        .expect("a matching declaration must start");
    session.shutdown().expect("shutdown");
}

/// A changed view plan is rejected without effects, the checkpoint survives the
/// rejection for a retry, and a corrected declaration still recovers from it.
#[test]
fn a_rejected_changed_view_keeps_the_checkpoint_for_a_corrected_retry() {
    let backend = SharedBackend::default();
    seed(&backend);
    let (session_config, reads, creates) = config(&backend);
    let mut session = Session::open(session_config).expect("open session");
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A_CHANGED).expect("create changed a");
    session.sql(VIEW_B).expect("create b");
    session.sql(SINK_A).expect("sink a");
    session.sql(SINK_B).expect("sink b");

    for _ in 0..2 {
        let error = match session.sql("START;") {
            Ok(_) => panic!("a changed plan must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    }
    assert_eq!(creates.load(Ordering::SeqCst), 0, "no writer may open");
    assert_eq!(reads.load(Ordering::SeqCst), 0, "no source may open");
    assert!(
        backend.get(b"checkpoint/latest").unwrap().is_some(),
        "the durable checkpoint must survive the rejection"
    );
    drop(session);

    let (retry_config, _reads, _creates) = config(&backend);
    let mut fixed = Session::open(retry_config).expect("open session");
    fixed.sql(SOURCE).expect("create source");
    fixed.sql(VIEW_A).expect("create a");
    fixed.sql(VIEW_B).expect("create b");
    fixed
        .sql("START;")
        .expect("corrected declarations must recover from the retained checkpoint");
    fixed.shutdown().expect("shutdown");
}
