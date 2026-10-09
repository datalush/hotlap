//! Sink fixtures specific to the shutdown-failure tests.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;

#[path = "../common/shutdown.rs"]
mod common;
pub use common::*;

/// A checkpoint config backed by an in-memory store, with a long interval so no
/// periodic checkpoint fires during a test.
pub fn checkpoint() -> CheckpointConfig {
    CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(SharedBackend::default()),
        retain: 3,
    }
}

/// A sink that records how many change rows it wrote and when it committed.
pub struct RecordingSink {
    rows: Mutex<usize>,
    committed: AtomicBool,
    written: Signal,
}

impl RecordingSink {
    pub fn new(written: Signal) -> Arc<Self> {
        Arc::new(Self {
            rows: Mutex::new(0),
            committed: AtomicBool::new(false),
            written,
        })
    }

    pub fn rows(&self) -> usize {
        *self.rows.lock().unwrap()
    }

    pub fn committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            *self.rows.lock().unwrap() += item?.len();
        }
        self.written.fire();
        Ok(())
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A sink whose final `commit` fails, so EOF cannot certify delivery.
pub struct FailingCommitSink {
    written: Signal,
}

impl FailingCommitSink {
    pub fn new(written: Signal) -> Arc<Self> {
        Arc::new(Self { written })
    }
}

#[async_trait::async_trait]
impl Sink for FailingCommitSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
        }
        self.written.fire();
        Ok(())
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Err(ConnectorError::Infrastructure("final commit failed".into()))
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A sink whose write task panics.
pub struct PanickingSink;

#[async_trait::async_trait]
impl Sink for PanickingSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        panic!("sink write panicked");
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

/// A sink whose `write` parks the runtime thread synchronously until released.
pub struct BlockingSink {
    entered: Signal,
    gate: Mutex<Option<std_mpsc::Receiver<()>>>,
}

impl BlockingSink {
    /// The sink and the sender that releases its blocked `write`.
    pub fn new(entered: Signal) -> (Arc<Self>, std_mpsc::Sender<()>) {
        let (release, held) = std_mpsc::channel();
        let sink = Arc::new(Self {
            entered,
            gate: Mutex::new(Some(held)),
        });
        (sink, release)
    }
}

#[async_trait::async_trait]
impl Sink for BlockingSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        self.entered.fire();
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            // Blocks the engine's runtime thread outright: no await, no timer.
            let _ = gate.recv();
        }
        Ok(())
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

/// A source that fires `signal` then panics on its first poll.
pub struct PanickingSource {
    schema: arrow::datatypes::SchemaRef,
    signal: Signal,
}

impl PanickingSource {
    pub fn new(signal: Signal) -> Arc<Self> {
        Arc::new(Self {
            schema: schema(),
            signal,
        })
    }
}

struct PanicOnPoll {
    signal: Signal,
}

impl Stream for PanicOnPoll {
    type Item = Result<SourceBatch, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.signal.fire();
        panic!("engine source panicked");
    }
}

impl Source for PanickingSource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(PanicOnPoll {
            signal: self.signal.clone(),
        }))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}
