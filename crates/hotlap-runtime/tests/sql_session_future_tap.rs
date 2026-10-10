//! Durable SQL restart keeps sink taps active for future source changes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::runtime::pipeline::SinkDescription;
use hotlap_runtime::runtime::source_checkpoint::decode_sources;
use hotlap_runtime::{Session, SessionConfig, SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

use backend::SharedBackend;

#[path = "common/backend.rs"]
mod backend;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW all_rows AS SELECT k FROM src WHERE k >= 1;";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM all_rows;";
const SOURCE_IDENTITY: &str = "test/sql-future-tap/source-store";
const SINK_IDENTITY: &str = "test/sql-future-tap/output-store";

type Rows = Arc<Mutex<Vec<Vec<i64>>>>;
type RemoteBag = Arc<Mutex<BTreeMap<i64, i64>>>;
type ReadGate = Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>;

struct MemorySourceFactory {
    rows: Rows,
    gate: ReadGate,
    resumed: Arc<Mutex<Vec<i64>>>,
}

struct MemorySource {
    rows: Rows,
    gate: ReadGate,
    resumed: Arc<Mutex<Vec<i64>>>,
    progress: Arc<Mutex<SourceState>>,
    schema: SchemaRef,
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

#[async_trait::async_trait]
impl SourceFactory for MemorySourceFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(MemorySource {
            rows: Arc::clone(&self.rows),
            gate: Arc::clone(&self.gate),
            resumed: Arc::clone(&self.resumed),
            progress: Arc::new(Mutex::new(SourceState::default())),
            schema: source_schema(),
        }))
    }
}

impl Source for MemorySource {
    fn physical_identity(&self) -> Option<String> {
        Some(SOURCE_IDENTITY.into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let state = self.progress.lock().unwrap();
        Ok(vec![Split {
            id: 0,
            start: state.offsets.get(&0).copied().unwrap_or(0),
        }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let start = split.start.max(0) as usize;
        let rows = self.rows.lock().unwrap().clone();
        let schema = Arc::clone(&self.schema);
        let mut gate = self.gate.lock().unwrap().take();
        let stream = futures::stream::iter(start..rows.len()).then(move |index| {
            let gate = if index == start { gate.take() } else { None };
            let values = rows[index].clone();
            let schema = Arc::clone(&schema);
            async move {
                if let Some(gate) = gate {
                    gate.await.map_err(|error| {
                        ConnectorError::Infrastructure(format!("source gate closed: {error}"))
                    })?;
                }
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(values.clone())),
                        Arc::new(Int64Array::from(values)),
                    ],
                )
                .map_err(|error| ConnectorError::Arrow(error.to_string()))?;
                Ok(SourceBatch {
                    batch,
                    base_offset: index as Offset,
                    next_offset: index as Offset + 1,
                    split: 0,
                })
            }
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.progress.lock().unwrap().offsets.insert(split, offset);
        Ok(())
    }

    fn state(&self) -> SourceState {
        self.progress.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        let offset = state.offsets.get(&0).copied().unwrap_or(0);
        self.resumed.lock().unwrap().push(offset);
        *self.progress.lock().unwrap() = state.clone();
        Ok(vec![Split {
            id: 0,
            start: offset,
        }])
    }
}

struct MemorySinkFactory {
    remote: RemoteBag,
    created: Arc<AtomicU32>,
    writes: Arc<AtomicU32>,
    changes: mpsc::Sender<()>,
}

struct MemorySink {
    remote: RemoteBag,
    writes: Arc<AtomicU32>,
    changes: mpsc::Sender<()>,
}

#[async_trait::async_trait]
impl SinkFactory for MemorySinkFactory {
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
            physical_identity: SINK_IDENTITY.into(),
            capabilities: SinkCapabilities::AtLeastOnce,
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
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MemorySink {
            remote: Arc::clone(&self.remote),
            writes: Arc::clone(&self.writes),
            changes: self.changes.clone(),
        }))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        true
    }

    fn may_create_transactional(&self, _options: &BTreeMap<String, String>) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl Sink for MemorySink {
    fn physical_identity(&self) -> Option<String> {
        Some(SINK_IDENTITY.into())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let change = item?;
            let keys = change
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConnectorError::Arrow("expected Int64 key".into()))?;
            let diffs = change
                .diff
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConnectorError::Arrow("expected Int64 diff".into()))?;
            let mut remote = self.remote.lock().unwrap();
            for index in 0..change.len() {
                *remote.entry(keys.value(index)).or_default() += diffs.value(index);
            }
            self.writes.fetch_add(1, Ordering::SeqCst);
            let _ = self.changes.send(());
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
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

struct SessionState {
    backend: SharedBackend,
    rows: Rows,
    resumed: Arc<Mutex<Vec<i64>>>,
    remote: RemoteBag,
    created: Arc<AtomicU32>,
    writes: Arc<AtomicU32>,
    changes: mpsc::Sender<()>,
}

fn config(state: &SessionState, gate: ReadGate) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(MemorySourceFactory {
            rows: Arc::clone(&state.rows),
            gate,
            resumed: Arc::clone(&state.resumed),
        }))
        .with_sink_factory(Arc::new(MemorySinkFactory {
            remote: Arc::clone(&state.remote),
            created: Arc::clone(&state.created),
            writes: Arc::clone(&state.writes),
            changes: state.changes.clone(),
        }))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(state.backend.clone()),
        )
}

fn wait_for_change(changes: &mpsc::Receiver<()>) {
    changes
        .recv_timeout(Duration::from_secs(5))
        .expect("sink did not receive a view change");
}

fn snapshot_bag(session: &Session) -> BTreeMap<i64, i64> {
    let snapshot = session.snapshot("all_rows").unwrap();
    let keys = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = snapshot.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..snapshot.len())
        .map(|index| (keys.value(index), diffs.value(index)))
        .collect()
}

fn source_offset(backend: &SharedBackend, id: u64) -> i64 {
    let bytes = backend
        .get(format!("checkpoint/{id}/sources").as_bytes())
        .unwrap()
        .unwrap();
    decode_sources(&bytes).unwrap().entries[0].state.offsets[&0]
}

#[test]
fn identical_sql_restart_delivers_gated_future_changes_to_a_new_writer() {
    let (change_tx, changes) = mpsc::channel();
    let state = SessionState {
        backend: SharedBackend::default(),
        rows: Arc::new(Mutex::new(vec![vec![1]])),
        remote: Arc::new(Mutex::new(BTreeMap::new())),
        created: Arc::new(AtomicU32::new(0)),
        writes: Arc::new(AtomicU32::new(0)),
        resumed: Arc::new(Mutex::new(Vec::new())),
        changes: change_tx,
    };

    let mut first = Session::open(config(&state, Arc::new(Mutex::new(None)))).unwrap();
    first.sql(SOURCE).unwrap();
    first.sql(VIEW).unwrap();
    first.sql(SINK).unwrap();
    first.sql("START;").unwrap();
    wait_for_change(&changes);
    let checkpoint = first.checkpoint().unwrap();
    assert_eq!(checkpoint, 1);
    assert_eq!(
        *state.remote.lock().unwrap(),
        [(1, 1)].into_iter().collect()
    );
    assert_eq!(source_offset(&state.backend, 1), 1);
    first.shutdown().unwrap();

    state.rows.lock().unwrap().push(vec![2]);
    let (release, gate) = tokio::sync::oneshot::channel();
    let mut restarted = Session::open(config(&state, Arc::new(Mutex::new(Some(gate))))).unwrap();
    restarted.sql(SOURCE).unwrap();
    restarted.sql(VIEW).unwrap();
    restarted.sql(SINK).unwrap();
    restarted.sql("START;").unwrap();

    assert_eq!(*state.resumed.lock().unwrap(), vec![1]);
    let restored = snapshot_bag(&restarted);
    assert_eq!(restored, [(1, 1)].into_iter().collect());
    assert_eq!(
        *state.remote.lock().unwrap(),
        [(1, 1)].into_iter().collect()
    );

    release.send(()).unwrap();
    wait_for_change(&changes);
    assert_eq!(
        *state.remote.lock().unwrap(),
        [(1, 1), (2, 1)].into_iter().collect()
    );
    let delivered = snapshot_bag(&restarted);
    assert_eq!(delivered, [(1, 1), (2, 1)].into_iter().collect());
    let checkpoint = restarted.checkpoint().unwrap();
    assert_eq!(checkpoint, 2);
    assert_eq!(source_offset(&state.backend, 2), 2);
    assert_eq!(state.created.load(Ordering::SeqCst), 2);
    assert!(state.writes.load(Ordering::SeqCst) >= 2);
    restarted.shutdown().unwrap();
}
