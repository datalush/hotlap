//! A corrupt pending body cannot mask the durable fallback view identities.

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
const SINK_A: &str = "CREATE SINK outa WITH (connector='inmem') AS SELECT * FROM a;";
const SINK_B: &str = "CREATE SINK outb WITH (connector='inmem') AS SELECT * FROM b;";

fn config(
    backend: &SharedBackend,
    reads: Arc<AtomicU32>,
    creates: Arc<AtomicU32>,
) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(CountedFactory {
            reads,
            starts: Arc::default(),
        }))
        .with_sink_factory(Arc::new(ToggleFactory {
            creates,
            accepts: Arc::new(AtomicBool::new(true)),
            writes: Arc::default(),
        }))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

fn seed(backend: &SharedBackend) {
    let mut session = Session::open(config(
        backend,
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    ))
    .unwrap();
    session.sql(SOURCE).unwrap();
    session.sql(VIEW_A).unwrap();
    session.sql(VIEW_B).unwrap();
    session.sql("START;").unwrap();
    session.checkpoint().unwrap();
    session.shutdown().unwrap();
}

fn corrupt_pending(backend: &SharedBackend) {
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
fn changed_valid_fallback_is_rejected_before_any_factory_effect() {
    let backend = SharedBackend::default();
    seed(&backend);
    corrupt_pending(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session = Session::open(config(&backend, reads.clone(), creates.clone())).unwrap();
    session.sql(SOURCE).unwrap();
    session.sql(VIEW_A_CHANGED).unwrap();
    session.sql(VIEW_B).unwrap();
    session.sql(SINK_A).unwrap();
    session.sql(SINK_B).unwrap();

    let error = match session.sql("START;") {
        Ok(_) => panic!("changed fallback plan must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}
