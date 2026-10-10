//! Persistent staged-payload fixtures for checkpoint abort tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap::{InputId, Plan, ZSetBatch};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError, Sink, SinkCapabilities};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[derive(Default)]
pub struct Remote {
    state: Mutex<RemoteState>,
}

#[derive(Default)]
struct RemoteState {
    staged: Option<PreparedPayload>,
    committed: BTreeMap<Vec<i64>, i64>,
    committed_transactions: std::collections::BTreeSet<u64>,
    next_transaction_id: u64,
    commit_calls: usize,
}

struct PreparedPayload {
    transaction_id: u64,
    changes: Vec<(Vec<i64>, i64)>,
}

impl Remote {
    pub fn staged(&self) -> Vec<(Vec<i64>, i64)> {
        self.state
            .lock()
            .unwrap()
            .staged
            .as_ref()
            .map(|payload| payload.changes.clone())
            .unwrap_or_default()
    }

    pub fn committed(&self) -> Vec<(Vec<i64>, i64)> {
        self.state
            .lock()
            .unwrap()
            .committed
            .iter()
            .map(|(key, weight)| (key.clone(), *weight))
            .collect()
    }

    pub fn commit_calls(&self) -> usize {
        self.state.lock().unwrap().commit_calls
    }
}

pub struct PersistentTxn {
    remote: Arc<Remote>,
    writer_buffer: Mutex<Vec<(Vec<i64>, i64)>>,
    fail_abort: bool,
    fail_prepare: bool,
    stall_abort: bool,
    redriable: bool,
}

impl PersistentTxn {
    pub fn new(remote: Arc<Remote>, fail_abort: bool, redriable: bool) -> Arc<Self> {
        Arc::new(Self {
            remote,
            writer_buffer: Mutex::new(Vec::new()),
            fail_abort,
            fail_prepare: false,
            stall_abort: false,
            redriable,
        })
    }

    pub fn failing_prepare(remote: Arc<Remote>, fail_abort: bool, redriable: bool) -> Arc<Self> {
        Arc::new(Self {
            remote,
            writer_buffer: Mutex::new(Vec::new()),
            fail_abort,
            fail_prepare: true,
            stall_abort: false,
            redriable,
        })
    }

    pub fn stalled_abort(remote: Arc<Remote>) -> Arc<Self> {
        Arc::new(Self {
            remote,
            writer_buffer: Mutex::new(Vec::new()),
            fail_abort: false,
            fail_prepare: false,
            stall_abort: true,
            redriable: false,
        })
    }
}

pub fn engine() -> hotlap::Hotlap {
    hotlap::Hotlap::open_with(Box::new(hotlap_engine::EngineCore::new()))
}

/// Build actual changelog batches from the input engine, including a retraction.
pub fn engine_with_changes() -> (hotlap::Hotlap, Vec<ZSetBatch>) {
    let mut hotlap = engine();
    hotlap.register_input("in").unwrap();
    hotlap.create_view("v", Plan::Source(InputId(0))).unwrap();
    hotlap.tap_view("v").unwrap();
    hotlap.push("in", &batch(&[7, 7, 8], &[1, 1, 1])).unwrap();
    let positive = hotlap.take_changes("v").unwrap();
    hotlap.push("in", &batch(&[8], &[-1])).unwrap();
    let retraction = hotlap.take_changes("v").unwrap();
    (hotlap, vec![positive, retraction])
}

fn batch(keys: &[i64], diffs: &[i64]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let record =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys.to_vec()))]).unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

/// Exercise the sink SPI with the actual `ZSetBatch` changelog from `Hotlap`.
pub async fn write_changes(sink: &PersistentTxn, changes: Vec<ZSetBatch>) {
    let stream = Box::pin(futures::stream::iter(changes.into_iter().map(Ok)));
    sink.write(stream).await.unwrap();
}

struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
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

pub fn sources() -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap()
}

#[derive(Clone, Default)]
pub struct FaultBackend {
    inner: crate::backend::SharedBackend,
    fail_engine: Arc<Mutex<bool>>,
    fail_clear: Arc<Mutex<bool>>,
}

impl FaultBackend {
    pub fn new(inner: crate::backend::SharedBackend, fail_clear: bool) -> Self {
        Self {
            inner,
            fail_engine: Arc::new(Mutex::new(true)),
            fail_clear: Arc::new(Mutex::new(fail_clear)),
        }
    }
}

impl StateBackend for FaultBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        self.inner.get(key)
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        if key == b"checkpoint/1/engine" && std::mem::take(&mut *self.fail_engine.lock().unwrap()) {
            return Err(StateError::Io(std::io::Error::other("capture failed")));
        }
        self.inner.put(key, value)
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        self.inner.scan(prefix)
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.inner.list(prefix)
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if key == b"checkpoint/1/prepare" && *self.fail_clear.lock().unwrap() {
            return Err(StateError::Io(std::io::Error::other("clear failed")));
        }
        self.inner.delete(key)
    }
}

#[async_trait::async_trait]
impl Sink for PersistentTxn {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let change = item?;
            let diffs = change
                .diff
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConnectorError::Unsupported("expected Int64 diff".into()))?;
            let mut buffer = self.writer_buffer.lock().unwrap();
            for row in 0..change.len() {
                let values = change
                    .batch
                    .columns()
                    .iter()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .map(|values| values.value(row))
                            .ok_or_else(|| {
                                ConnectorError::Unsupported("expected Int64 columns".into())
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                buffer.push((values, diffs.value(row)));
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

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        let mut state = self.remote.state.lock().unwrap();
        let changes = std::mem::take(&mut *self.writer_buffer.lock().unwrap());
        if !changes.is_empty() {
            state.next_transaction_id += 1;
            let transaction_id = state.next_transaction_id;
            state.staged = Some(PreparedPayload {
                transaction_id,
                changes,
            });
        }
        if self.fail_prepare {
            return Err(ConnectorError::Infrastructure("prepare failed".into()));
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        let mut state = self.remote.state.lock().unwrap();
        state.commit_calls += 1;
        if let Some(payload) = state.staged.take()
            && state.committed_transactions.insert(payload.transaction_id)
        {
            for (key, weight) in payload.changes {
                let remove = {
                    let total = state.committed.entry(key.clone()).or_default();
                    *total += weight;
                    *total == 0
                };
                if remove {
                    state.committed.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        if self.stall_abort {
            futures::future::pending().await
        }
        if self.fail_abort {
            return Err(ConnectorError::Unsupported("abort denied".into()));
        }
        self.remote.state.lock().unwrap().staged = None;
        Ok(())
    }
}
