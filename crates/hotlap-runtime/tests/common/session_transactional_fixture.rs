//! Fixtures for transactional session recovery preflight regressions.

#[path = "backend.rs"]
mod backend;
#[path = "session_checkpoint_source.rs"]
mod source_fixture;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::SchemaRef;
use backend::SharedBackend;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::runtime::pipeline::SinkDescription;
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

#[derive(Default)]
pub struct SessionSignals {
    pub source_gate: Option<Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>>,
    pub first_change: Option<mpsc::Sender<()>>,
    pub source_ack: Option<mpsc::Sender<()>>,
    pub source_read_ack: Option<mpsc::Sender<()>>,
}

struct TransactionalSink {
    first_change: Mutex<Option<mpsc::Sender<()>>>,
    physical_identity: String,
}

#[async_trait::async_trait]
impl Sink for TransactionalSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        use futures::StreamExt;
        while changes.next().await.is_some() {
            if let Some(signal) = self.first_change.lock().unwrap().take() {
                let _ = signal.send(());
            }
        }
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    fn accepts_retractions(&self) -> bool {
        true
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
    first_change: Option<mpsc::Sender<()>>,
}

#[async_trait::async_trait]
impl SinkFactory for TransactionalFactory {
    async fn describe(
        &self,
        binding_name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(Some(SinkDescription {
            binding_name: binding_name.to_owned(),
            view: view.to_owned(),
            physical_identity: format!("test/session-transactional/{binding_name}"),
            capabilities: SinkCapabilities::Transactional,
            accepts_retractions: true,
            commit_redriable: false,
        }))
    }

    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(TransactionalSink {
            first_change: Mutex::new(self.first_change.clone()),
            physical_identity: format!("test/session-transactional/{_name}"),
        }))
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
    signals: SessionSignals,
) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(ProbeFactory {
            dataset: ProbeDataset::new(vec![vec![1]])
                .with_retention(0)
                .with_physical_identity("test/session-transactional/source"),
            spies,
            reads,
            source_gate: signals.source_gate,
            source_ack: signals.source_ack,
            source_read_ack: signals.source_read_ack,
        }))
        .with_sink_factory(Arc::new(TransactionalFactory {
            creates,
            may_create_transactional,
            first_change: signals.first_change,
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
    let (release_source, source_gate) = tokio::sync::oneshot::channel();
    let (source_ack, output) = mpsc::channel();
    let mut session = Session::open(config(
        backend,
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
        true,
        Arc::new(std::sync::Mutex::new(Vec::new())),
        SessionSignals {
            source_gate: Some(Arc::new(Mutex::new(Some(source_gate)))),
            source_ack: Some(source_ack),
            ..SessionSignals::default()
        },
    ))
    .expect("open seed session");
    session.sql(SOURCE).expect("create source");
    session.sql(VIEW_A).expect("create a");
    session.sql(VIEW_B).expect("create b");
    session.sql(SINK_A).expect("create sink a");
    session.sql(SINK_B).expect("create sink b");
    session.sql("START;").expect("start seed");
    let _ = release_source.send(());
    wait_for_output(output);
    assert_eq!(rows(&session), vec![vec![1]]);
    assert_eq!(session.checkpoint().unwrap(), 1);
    session.shutdown().expect("shutdown seed");
}

pub fn corrupt_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
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

pub fn wait_for_output(output: mpsc::Receiver<()>) {
    output
        .recv_timeout(Duration::from_secs(5))
        .expect("source did not acknowledge an applied batch");
}

pub fn wait_for_source_read(output: mpsc::Receiver<()>) {
    output
        .recv_timeout(Duration::from_secs(5))
        .expect("source read did not start after recovery");
}
