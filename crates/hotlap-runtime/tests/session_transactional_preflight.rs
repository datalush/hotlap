#[path = "common/backend.rs"]
mod backend;
#[path = "sql_session_rejected_start/factories.rs"]
mod factories;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use backend::SharedBackend;
use factories::{CountedFactory, ToggleFactory};
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SessionError, SinkFactory};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW_A: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 1;";
const VIEW_B: &str = "CREATE MATERIALIZED VIEW b AS SELECT k FROM src WHERE k = 2;";
const SINK_A: &str = "CREATE SINK outa WITH (connector='inmem') AS SELECT * FROM a;";
const SINK_B: &str = "CREATE SINK outb WITH (connector='inmem') AS SELECT * FROM b;";

struct TransactionalSink;

#[async_trait::async_trait]
impl Sink for TransactionalSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        use futures::StreamExt;
        while changes.next().await.is_some() {}
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

struct TransactionalFactory {
    creates: Arc<AtomicU32>,
    may_create_transactional: bool,
}

#[async_trait::async_trait]
impl SinkFactory for TransactionalFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(TransactionalSink))
    }

    fn may_create_transactional(&self, _options: &BTreeMap<String, String>) -> bool {
        self.may_create_transactional
    }
}

fn config(
    backend: &SharedBackend,
    reads: Arc<AtomicU32>,
    creates: Arc<AtomicU32>,
    may_create_transactional: bool,
) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(CountedFactory { reads }))
        .with_sink_factory(Arc::new(TransactionalFactory {
            creates,
            may_create_transactional,
        }))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

fn declare(session: &mut Session) {
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session.sql(SINK_A).expect("create sink a");
    session.sql(SINK_B).expect("create sink b");
}

fn seed(backend: &SharedBackend) {
    let seed_config = SessionConfig::new()
        .with_source_factory(Arc::new(CountedFactory {
            reads: Arc::new(AtomicU32::new(0)),
        }))
        .with_sink_factory(Arc::new(ToggleFactory {
            creates: Arc::new(AtomicU32::new(0)),
            accepts: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        );
    let mut session = Session::open(seed_config).expect("open seed session");
    declare(&mut session);
    session.sql("START;").expect("start seed");
    session.checkpoint().expect("checkpoint seed");
    session.shutdown().expect("shutdown seed");
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
fn transactional_pending_is_rejected_before_factory_create_and_retry_keeps_config() {
    let backend = SharedBackend::default();
    seed(&backend);
    corrupt_pending(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session =
        Session::open(config(&backend, reads.clone(), creates.clone(), true)).unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("unsafe corrupt pending state must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        creates.load(Ordering::SeqCst),
        0,
        "factory must not create writers"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0, "source must not be read");
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());

    remove_pending(&backend);
    session
        .sql("START;")
        .expect("same-session retry keeps checkpoint config");
    session.shutdown().expect("shutdown retry");
}

#[test]
fn actual_transactional_sink_cannot_contradict_a_safe_factory_declaration() {
    let backend = SharedBackend::default();
    seed(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session =
        Session::open(config(&backend, reads.clone(), creates.clone(), false)).unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("a transactional sink contradicting preflight must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(creates.load(Ordering::SeqCst), 2);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "sources must stay unopened"
    );
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}

fn remove_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources", "commit"] {
        writer
            .delete(format!("checkpoint/2/{part}").as_bytes())
            .unwrap();
    }
}
