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

    /// A fresh writer over a persisted store, as a restart would build.
    ///
    /// The coordinated attempt died with the previous process, so this
    /// instance holds no gate: it sees the staged payload but cannot publish
    /// it, and a commit call fails instead of claiming delivery.
    pub fn reopen(store: Arc<StagedStore>) -> Arc<Self> {
        let (entered, _) = Signal::new();
        let (written, _) = Signal::new();
        Arc::new(Self {
            store,
            pending: Mutex::new(Vec::new()),
            entered,
            written,
            release: Mutex::new(None),
        })
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
        // Only the coordinated attempt owns the gate. Without it this writer
        // cannot deliver the staged payload, so it fails rather than report a
        // commit that never happened.
        let held = self.release.lock().unwrap().take();
        let Some(held) = held else {
            return Err(ConnectorError::Infrastructure(
                "commit has no coordinated attempt to publish".into(),
            ));
        };
        self.entered.fire();
        held.await.map_err(|_| {
            ConnectorError::Infrastructure("the coordinated attempt ended before commit".into())
        })?;
        self.store.publish();
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.store.discard();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hotlap_connectors::sink::Sink;

    use super::{StagedSink, StagedStore};

    #[tokio::test]
    async fn a_commit_without_a_coordinated_attempt_fails() {
        let store = StagedStore::new();
        store.stage(vec![7, 8]);
        let sink = StagedSink::reopen(store.clone());

        assert!(
            sink.commit().await.is_err(),
            "a writer without the coordinated attempt must not report a commit"
        );
        assert!(
            store.published().is_empty(),
            "a failed commit must not publish"
        );
    }
}
