//! Public session START recovers the fallback body and its applied offset.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/recovery/resumable.rs"]
mod resumable;
#[path = "common/spy.rs"]
mod spy;
#[path = "common/watermarked_spy.rs"]
mod watermarked_spy;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::SchemaRef;
use backend::SharedBackend;
use hotlap::state::StateBackend;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SinkFactory, SourceFactory};
use hotlap_sql::error::SqlError;
use resumable::{Dataset, ResumableSource};
use spy::SpySource;
use watermarked_spy::WatermarkedSpy;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR k AS \
     k - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW a AS SELECT k FROM src WHERE k = 1;";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM a;";

struct Factory {
    dataset: Dataset,
    spies: Arc<Mutex<Vec<Arc<SpySource>>>>,
}

struct ReplaySafeFactory;

#[async_trait::async_trait]
impl SinkFactory for ReplaySafeFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        Ok(Arc::new(ReplaySafeSink))
    }

    fn may_create_transactional(
        &self,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        false
    }
}

struct ReplaySafeSink;

#[async_trait::async_trait]
impl Sink for ReplaySafeSink {
    async fn write(
        &self,
        mut changes: ChangeStream,
    ) -> Result<(), hotlap_connectors::ConnectorError> {
        use futures::StreamExt;
        while changes.next().await.is_some() {}
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }
    async fn commit(&self) -> Result<(), hotlap_connectors::ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), hotlap_connectors::ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let spy = SpySource::new(Arc::new(ResumableSource::new(
            self.dataset.clone().with_retention(0),
        )));
        self.spies.lock().unwrap().push(Arc::new(spy.clone()));
        Ok(Box::new(WatermarkedSpy(spy)))
    }
}

fn config(backend: &SharedBackend, spies: Arc<Mutex<Vec<Arc<SpySource>>>>) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(Factory {
            dataset: Dataset::new(vec![vec![1]]).with_retention(0),
            spies,
        }))
        .with_sink_factory(Arc::new(ReplaySafeFactory))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

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

fn wait_for_output(session: &Session) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if rows(session) == vec![vec![1]] {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("source output did not arrive");
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
    let mut initial = Session::open(config(&backend, initial_spies)).unwrap();
    declare(&mut initial);
    initial.sql("START;").unwrap();
    wait_for_output(&initial);
    assert_eq!(initial.checkpoint().unwrap(), 1);
    initial.shutdown().unwrap();
    add_corrupt_pending(&backend);

    let restart_spies = Arc::new(Mutex::new(Vec::new()));
    let mut restarted = Session::open(config(&backend, Arc::clone(&restart_spies))).unwrap();
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
