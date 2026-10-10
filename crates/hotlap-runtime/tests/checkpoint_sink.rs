use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap::{AggSpec, Hotlap, InputId, Plan, ZSetBatch};
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer};
use hotlap_runtime::runtime::pipeline::SinkSpec;
use hotlap_runtime::runtime::sink::SinkPump;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// Ordered record of the observable events the test asserts on.
#[derive(Clone, Default)]
struct Journal(Arc<Mutex<Vec<&'static str>>>);

impl Journal {
    fn record(&self, entry: &'static str) {
        self.0.lock().unwrap().push(entry);
    }

    fn entries(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().clone()
    }
}

/// A sink that records one `write` per batch it receives.
struct JournalSink {
    journal: Journal,
}

struct FailingSink {
    aborted: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Sink for FailingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while changes.next().await.is_some() {}
        Err(ConnectorError::Infrastructure("write failed".into()))
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for JournalSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
            self.journal.record("write");
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// In-memory backend that records when the `valid` marker is published.
#[derive(Clone, Default)]
struct JournalBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    journal: Journal,
}

impl StateBackend for JournalBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        if key.ends_with(b"/valid") {
            self.journal.record("valid");
        }
        self.map.lock().unwrap().insert(key.to_vec(), value);
        Ok(())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
}

/// A source with no splits and empty state, enough for the barrier.
struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::empty())
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

fn batch() -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 1, 2]))];
    let record = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(vec![1, 1, -1]))).unwrap()
}

/// Build an engine with a tapped group-count view fed by one batch.
async fn engine_with_pending_delta(pump: &SinkPump) -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    hotlap.register_input("in").unwrap();
    hotlap
        .create_view(
            "c",
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )
        .unwrap();
    hotlap.tap_view("c").unwrap();
    hotlap.push("in", &batch()).unwrap();
    pump.pump(&mut hotlap).await.unwrap();
    hotlap
}

#[tokio::test]
async fn checkpoint_drains_queued_sink_deltas_before_valid() {
    let journal = Journal::default();
    let sink = Arc::new(JournalSink {
        journal: journal.clone(),
    });
    let pump = SinkPump::start(&[SinkSpec {
        view: "c".into(),
        sink,
    }]);
    let hotlap = engine_with_pending_delta(&pump).await;

    let backend = JournalBackend {
        journal: journal.clone(),
        ..JournalBackend::default()
    };
    let mut checkpointer = Checkpointer::new(Box::new(backend), 3).with_sinks(pump.coordinated());
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap();
    let id = checkpointer.take(&hotlap, &sources).await.unwrap();
    assert_eq!(id, 1);

    let entries = journal.entries();
    let write = entries
        .iter()
        .position(|entry| *entry == "write")
        .expect("the sink must receive the queued delta");
    let valid = entries
        .iter()
        .position(|entry| *entry == "valid")
        .expect("the checkpoint must be published");
    assert!(
        write < valid,
        "the sink output must be applied before `valid`: {entries:?}"
    );
    assert_eq!(checkpointer.latest().unwrap(), Some(1));
}

#[tokio::test]
async fn failed_sink_drain_marks_checkpointer_inconsistent() {
    let aborted = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(FailingSink {
        aborted: Arc::clone(&aborted),
    });
    let pump = SinkPump::start(&[SinkSpec {
        view: "c".into(),
        sink,
    }]);
    let hotlap = engine_with_pending_delta(&pump).await;
    let mut checkpointer = Checkpointer::new(Box::new(JournalBackend::default()), 3)
        .with_sinks(pump.coordinated());
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap();

    assert!(checkpointer.take(&hotlap, &sources).await.is_err());
    assert!(aborted.load(Ordering::SeqCst), "writer failure must roll back");
    assert_eq!(checkpointer.state(), CheckpointState::Failed);
    assert!(checkpointer.take(&hotlap, &sources).await.is_err());
}
