//! Fixtures for transactional session recovery preflight regressions.

#[path = "backend.rs"]
mod backend;
#[path = "session_checkpoint_source.rs"]
mod source_fixture;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::SchemaRef;
use backend::SharedBackend;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SinkFactory};
use hotlap_sql::SqlError;
use source_fixture::{ProbeDataset, ProbeFactory, SourceProbe};

pub use backend::SharedBackend as TestBackend;

pub const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR k AS \
     k - INTERVAL '1 s';";
pub const VIEW_A: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 1;";
pub const VIEW_B: &str = "CREATE MATERIALIZED VIEW b AS SELECT k FROM src WHERE k = 2;";
pub const SINK_A: &str = "CREATE SINK outa WITH (connector='inmem') AS SELECT * FROM a;";
pub const SINK_B: &str = "CREATE SINK outb WITH (connector='inmem') AS SELECT * FROM b;";

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

pub fn config(
    backend: &SharedBackend,
    reads: Arc<AtomicU32>,
    creates: Arc<AtomicU32>,
    may_create_transactional: bool,
    spies: Arc<std::sync::Mutex<Vec<Arc<SourceProbe>>>>,
) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(ProbeFactory {
            dataset: ProbeDataset::new(vec![vec![1]]).with_retention(0),
            spies,
            reads,
        }))
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

pub fn declare(session: &mut Session) {
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session.sql(SINK_A).expect("create sink a");
    session.sql(SINK_B).expect("create sink b");
}

pub fn seed(backend: &SharedBackend) {
    let mut session = Session::open(config(
        backend,
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
        true,
        Arc::new(std::sync::Mutex::new(Vec::new())),
    ))
    .expect("open seed session");
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session.sql("START;").expect("start seed");
    wait_for_output(&session);
    assert_eq!(session.checkpoint().unwrap(), 1);
    session.shutdown().expect("shutdown seed");
}

pub fn corrupt_pending(backend: &SharedBackend) {
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

pub fn remove_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources", "commit"] {
        writer
            .delete(format!("checkpoint/2/{part}").as_bytes())
            .unwrap();
    }
}

pub fn rows(session: &Session) -> Vec<Vec<i64>> {
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

pub fn wait_for_output(session: &Session) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if rows(session) == vec![vec![1]] {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("seed output did not arrive");
}
