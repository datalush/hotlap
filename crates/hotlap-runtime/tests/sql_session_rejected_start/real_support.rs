use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::SinkDescription;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::{Session, SessionConfig, SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

#[path = "../common/backend.rs"]
mod backend;

pub use backend::SharedBackend;

pub const SOURCE_IDENTITY: &str = "test/sql-real-retry/source-store";
pub const SINK_TARGET: &str = "test/sql-real-retry/output-store";
const SOURCE_SQL: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
    _event_time AS _event_time - INTERVAL '1 s';";
const VIEW_SQL: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k FROM src WHERE k >= 1;";
const SINK_SQL: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM mv;";

pub fn declared_sql(session: &mut Session) {
    session.sql(SOURCE_SQL).unwrap();
    session.sql(VIEW_SQL).unwrap();
    session.sql(SINK_SQL).unwrap();
}

#[derive(Default)]
pub struct Stats {
    pub source_reads: AtomicU32,
    pub sink_creates: AtomicU32,
    pub sink_writes: AtomicU32,
    pub resumed: Mutex<Vec<i64>>,
}

pub struct SessionFixture {
    pub backend: SharedBackend,
    source_senders: Arc<Mutex<VecDeque<tokio::sync::mpsc::UnboundedSender<i64>>>>,
    ack_sender: mpsc::Sender<i64>,
    ack_receiver: Mutex<mpsc::Receiver<i64>>,
    pub remote_bag: Arc<Mutex<BTreeMap<i64, i64>>>,
    sink_changes: mpsc::Sender<()>,
    sink_change_receiver: Mutex<mpsc::Receiver<()>>,
}

impl SessionFixture {
    pub fn new() -> Self {
        let (ack_sender, ack_receiver) = mpsc::channel();
        let (sink_changes, sink_change_receiver) = mpsc::channel();
        Self {
            backend: SharedBackend::default(),
            source_senders: Arc::new(Mutex::new(VecDeque::new())),
            ack_sender,
            ack_receiver: Mutex::new(ack_receiver),
            remote_bag: Arc::new(Mutex::new(BTreeMap::new())),
            sink_changes,
            sink_change_receiver: Mutex::new(sink_change_receiver),
        }
    }

    pub fn open(&self, stats: &Arc<Stats>, described: Option<String>, actual: String) -> Session {
        self.open_with_identity(
            stats,
            Arc::new(Mutex::new(described)),
            Arc::new(Mutex::new(actual)),
        )
    }

    pub fn open_with_identity(
        &self,
        stats: &Arc<Stats>,
        described: Arc<Mutex<Option<String>>>,
        actual: Arc<Mutex<String>>,
    ) -> Session {
        let config = SessionConfig::new()
            .with_source_factory(Arc::new(QueueSourceFactory {
                senders: Arc::clone(&self.source_senders),
                acknowledgements: self.ack_sender.clone(),
                stats: Arc::clone(stats),
            }))
            .with_sink_factory(Arc::new(BagSinkFactory {
                remote: Arc::clone(&self.remote_bag),
                changes: self.sink_changes.clone(),
                described,
                actual,
                stats: Arc::clone(stats),
            }))
            .with_checkpoint(
                Duration::from_secs(3600),
                DEFAULT_RETAIN,
                Box::new(self.backend.clone()),
            );
        Session::open(config).unwrap()
    }

    pub fn send_source_value(&self, value: i64) {
        self.source_senders
            .lock()
            .unwrap()
            .back()
            .expect("START created a source channel")
            .send(value)
            .expect("source is still receiving rows");
    }

    pub fn source_ack(&self, expected: i64) {
        let receiver = self.ack_receiver.lock().unwrap();
        loop {
            let offset = receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("source did not acknowledge ingestion");
            if offset == expected {
                return;
            }
            assert!(
                offset < expected,
                "source acknowledged offset {offset} past expected {expected}"
            );
        }
    }

    pub fn wait_for_remote_bag(&self, expected: &BTreeMap<i64, i64>) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while *self.remote_bag.lock().unwrap() != *expected {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "sink remote bag did not reach {expected:?}"
            );
            self.sink_change_receiver
                .lock()
                .unwrap()
                .recv_timeout(remaining)
                .expect("sink did not signal a delivered change");
        }
    }

    pub fn read_checkpoint(&self, id: u64) -> hotlap_runtime::runtime::checkpoint::Checkpoint {
        let sink = SharedSink::new(Arc::new(IdentitySink(SINK_TARGET.to_owned())));
        let reader =
            Checkpointer::new(Box::new(self.backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
                SinkSync::sink_only_named(sink, "out".into(), "mv".into()),
            ]);
        reader.read(id).unwrap()
    }
}

struct QueueSourceFactory {
    senders: Arc<Mutex<VecDeque<tokio::sync::mpsc::UnboundedSender<i64>>>>,
    acknowledgements: mpsc::Sender<i64>,
    stats: Arc<Stats>,
}

struct QueueSource {
    receiver: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<i64>>>,
    acknowledgements: mpsc::Sender<i64>,
    stats: Arc<Stats>,
    state: Mutex<SourceState>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl SourceFactory for QueueSourceFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.senders.lock().unwrap().push_back(sender);
        Ok(Box::new(QueueSource {
            receiver: Mutex::new(Some(receiver)),
            acknowledgements: self.acknowledgements.clone(),
            stats: Arc::clone(&self.stats),
            state: Mutex::new(SourceState::default()),
            schema: Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("_event_time", DataType::Int64, false),
            ])),
        }))
    }
}

impl Source for QueueSource {
    fn physical_identity(&self) -> Option<String> {
        Some(SOURCE_IDENTITY.into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let start = self
            .state
            .lock()
            .unwrap()
            .offsets
            .get(&0)
            .copied()
            .unwrap_or(0);
        Ok(vec![Split { id: 0, start }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.stats.source_reads.fetch_add(1, Ordering::SeqCst);
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("source split is read once");
        let schema = Arc::clone(&self.schema);
        let stream = stream::unfold((receiver, split.start), move |(mut receiver, offset)| {
            let schema = Arc::clone(&schema);
            async move {
                let value = receiver.recv().await?;
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![value])),
                        Arc::new(Int64Array::from(vec![value])),
                    ],
                )
                .map_err(|error| ConnectorError::Arrow(error.to_string()));
                let next = offset + 1;
                let item = batch.map(|batch| SourceBatch {
                    batch,
                    base_offset: offset,
                    next_offset: next,
                    split: 0,
                });
                Some((item, (receiver, next)))
            }
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.state.lock().unwrap().offsets.insert(split, offset);
        self.acknowledgements
            .send(offset)
            .map_err(|error| ConnectorError::Infrastructure(error.to_string()))
    }

    fn state(&self) -> SourceState {
        self.state.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        *self.state.lock().unwrap() = state.clone();
        self.stats
            .resumed
            .lock()
            .unwrap()
            .push(state.offsets.get(&0).copied().unwrap_or(0));
        self.splits()
    }
}

struct BagSinkFactory {
    remote: Arc<Mutex<BTreeMap<i64, i64>>>,
    changes: mpsc::Sender<()>,
    described: Arc<Mutex<Option<String>>>,
    actual: Arc<Mutex<String>>,
    stats: Arc<Stats>,
}

struct BagSink {
    remote: Arc<Mutex<BTreeMap<i64, i64>>>,
    changes: mpsc::Sender<()>,
    identity: String,
    stats: Arc<Stats>,
}

#[async_trait::async_trait]
impl SinkFactory for BagSinkFactory {
    async fn describe(
        &self,
        binding_name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(self
            .described
            .lock()
            .unwrap()
            .as_ref()
            .map(|target| SinkDescription {
                binding_name: binding_name.into(),
                view: view.into(),
                physical_identity: target.clone(),
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
        self.stats.sink_creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(BagSink {
            remote: Arc::clone(&self.remote),
            changes: self.changes.clone(),
            identity: self.actual.lock().unwrap().clone(),
            stats: Arc::clone(&self.stats),
        }))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl Sink for BagSink {
    fn binding_name(&self) -> Option<&str> {
        Some("out")
    }

    fn physical_identity(&self) -> Option<String> {
        Some(self.identity.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(change) = changes.next().await {
            let change = change?;
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
            self.stats.sink_writes.fetch_add(1, Ordering::SeqCst);
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

struct IdentitySink(String);

#[async_trait::async_trait]
impl Sink for IdentitySink {
    fn binding_name(&self) -> Option<&str> {
        Some("out")
    }

    fn physical_identity(&self) -> Option<String> {
        Some(self.0.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while changes.next().await.is_some() {}
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

pub fn session_bag(session: &Session) -> BTreeMap<i64, i64> {
    let snapshot = session.snapshot("mv").unwrap();
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
