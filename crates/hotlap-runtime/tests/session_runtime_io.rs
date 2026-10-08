//! The [`Session`] runtime must drive connector tasks and IO.
//!
//! The Fluss source connects over TCP and spawns response-reader tasks while the
//! session is being built; the engine thread later drives `read()` on its own
//! runtime. These fixtures approximate that split without a broker: a task
//! spawned on the session runtime must still run once the build returns, and the
//! runtime must carry an IO driver.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::{Session, SessionConfig, SourceFactory};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), columns).unwrap(),
        base_offset: 0,
        next_offset: keys.len() as i64,
        split: 0,
    }
}

/// Emits its batch only after a task spawned at `create` signals.
struct SpawnSource {
    rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    batches: Vec<SourceBatch>,
}

impl Source for SpawnSource {
    fn schema(&self) -> SchemaRef {
        schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let rx = self.rx.lock().unwrap().take().expect("read once");
        let batches = self.batches.clone();
        let stream = stream::once(async move {
            rx.await.ok();
        })
        .flat_map(move |_| stream::iter(batches.clone().into_iter().map(Ok)));
        Ok(Box::pin(stream))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

struct SpawnFactory {
    batches: Vec<SourceBatch>,
}

#[async_trait::async_trait]
impl SourceFactory for SpawnFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Spawned on the session runtime; the engine thread awaits it later.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = tx.send(());
        });
        Ok(Box::new(SpawnSource {
            rx: Mutex::new(Some(rx)),
            batches: self.batches.clone(),
        }))
    }
}

#[test]
fn connector_task_survives_create() {
    let config = SessionConfig::new().with_source_factory(Arc::new(SpawnFactory {
        batches: vec![batch(&[1, 2], &[1000, 2000])],
    }));
    let mut session = Session::open(config).expect("open");
    session.sql(SOURCE).expect("create source");
    session.sql("START;").expect("start engine");

    // Only the runtime's own worker can drive the task now: no `sql()` call
    // follows, and `metrics()` does not enter the runtime.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ingested = session
            .metrics()
            .entries
            .get("rows_ingested")
            .copied()
            .unwrap_or(0);
        if ingested > 0 {
            assert_eq!(ingested, 2);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task spawned on the session runtime never ran"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    session.shutdown().expect("shutdown");
}

/// A source whose creation is enough to prove the runtime has an IO driver.
struct IoSource;

impl Source for IoSource {
    fn schema(&self) -> SchemaRef {
        schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

struct IoFactory;

#[async_trait::async_trait]
impl SourceFactory for IoFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        // `TcpListener::bind` and `TcpStream::connect` need an IO driver; they
        // panic without one, standing in for `FlussConnection::new`.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(io_error)?;
        let address = listener.local_addr().map_err(io_error)?;
        let accept = tokio::spawn(async move { listener.accept().await.map(|_| ()) });
        let _client = tokio::net::TcpStream::connect(address)
            .await
            .map_err(io_error)?;
        accept.await.map_err(io_error)?.map_err(io_error)?;
        Ok(Box::new(IoSource))
    }
}

fn io_error(error: impl std::fmt::Display) -> SqlError {
    SqlError::Engine(error.to_string())
}

#[test]
fn session_runtime_has_io_driver() {
    let config = SessionConfig::new().with_source_factory(Arc::new(IoFactory));
    let mut session = Session::open(config).expect("open");
    session.sql(SOURCE).expect("create source needs IO");
    session.shutdown().expect("shutdown");
}
