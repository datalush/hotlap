//! A faithful staged transactional sink over a small persistent store.

use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};
use tokio::sync::oneshot;

use super::Signal;

/// A durable, atomically-published logical store.
///
/// A committed payload replaces the staged one in a single lock, so a publish
/// is all-or-nothing. Nothing is published until `publish` runs.
#[derive(Default)]
pub struct StagedStore {
    state: Mutex<StoreState>,
}

#[derive(Default)]
struct StoreState {
    staged: Option<Vec<i64>>,
    published: Option<Vec<i64>>,
}

impl StagedStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StoreState::default()),
        })
    }

    fn stage(&self, payload: Vec<i64>) {
        self.state.lock().unwrap().staged = Some(payload);
    }

    fn publish(&self) {
        let mut state = self.state.lock().unwrap();
        state.published = state.staged.take();
    }

    fn discard(&self) {
        self.state.lock().unwrap().staged = None;
    }

    /// The payload the store will publish when its commit lands.
    pub fn staged(&self) -> Vec<i64> {
        self.state
            .lock()
            .unwrap()
            .staged
            .clone()
            .unwrap_or_default()
    }

    /// The payload the store has published, if any.
    pub fn published(&self) -> Vec<i64> {
        self.state
            .lock()
            .unwrap()
            .published
            .clone()
            .unwrap_or_default()
    }
}

/// A transactional sink that stages the real written payload in `prepare` and
/// publishes it atomically in `commit`, which is gated.
///
/// It does not declare its commit re-drivable: a fresh instance cannot guarantee
/// it re-publishes the same staged payload, so recovery must reject an
/// interrupted commit rather than promote it.
pub struct StagedSink {
    store: Arc<StagedStore>,
    pending: Mutex<Vec<i64>>,
    entered: Signal,
    written: Signal,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl StagedSink {
    pub fn new(
        store: Arc<StagedStore>,
        entered: Signal,
        written: Signal,
    ) -> (Arc<Self>, oneshot::Sender<()>) {
        let (release, held) = oneshot::channel();
        let sink = Arc::new(Self {
            store,
            pending: Mutex::new(Vec::new()),
            entered,
            written,
            release: Mutex::new(Some(held)),
        });
        (sink, release)
    }
}

#[async_trait::async_trait]
impl Sink for StagedSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            let column = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut pending = self.pending.lock().unwrap();
            for row in 0..column.len() {
                pending.push(column.value(row));
            }
        }
        self.written.fire();
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn prepare(&self) -> Result<(), ConnectorError> {
        let payload = std::mem::take(&mut *self.pending.lock().unwrap());
        self.store.stage(payload);
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        // Only the coordinated attempt holds the gate; it publishes on release.
        let held = self.release.lock().unwrap().take();
        let Some(held) = held else {
            return Ok(());
        };
        self.entered.fire();
        let _ = held.await;
        self.store.publish();
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.store.discard();
        Ok(())
    }
}
