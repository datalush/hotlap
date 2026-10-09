//! A volatile-queue sink and a persistent remote output store.

#[path = "../common/backend.rs"]
mod backend;
#[path = "../common/recovery/resumable.rs"]
mod resumable;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array};
pub use backend::SharedBackend;
use futures::StreamExt;
use hotlap::ZSetBatch;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
pub use resumable::{Dataset, ResumableSource};
use tokio::sync::Notify;

/// Persistent output shared by every sink instance.
#[derive(Clone, Default)]
pub struct RemoteStore(Arc<Mutex<Vec<Vec<i64>>>>);

impl RemoteStore {
    pub fn deliver(&self, rows: Vec<Vec<i64>>) {
        self.0.lock().unwrap().extend(rows);
    }

    pub fn rows(&self) -> Vec<Vec<i64>> {
        self.0.lock().unwrap().clone()
    }
}

/// A sink whose writer only queues rows in memory; `commit` delivers them to the
/// remote store. A pending `commit` blocks after signalling `entered`.
pub struct VolatileSink {
    remote: RemoteStore,
    queue: Mutex<Vec<Vec<i64>>>,
    pending: AtomicBool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl VolatileSink {
    pub fn new(remote: RemoteStore) -> (Arc<Self>, Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let sink = Arc::new(Self {
            remote,
            queue: Mutex::new(Vec::new()),
            pending: AtomicBool::new(false),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        (sink, entered, release)
    }

    pub fn set_pending(&self, value: bool) {
        self.pending.store(value, Ordering::SeqCst);
    }
}

fn row(zset: &ZSetBatch, index: usize) -> Vec<i64> {
    zset.batch
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(index)
        })
        .collect()
}

#[async_trait::async_trait]
impl Sink for VolatileSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let zset = item?;
            let mut queue = self.queue.lock().unwrap();
            for index in 0..zset.len() {
                queue.push(row(&zset, index));
            }
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    // `commit_redriable` stays at the default: a fresh, empty queue cannot
    // complete the commit a crashed writer accepted.
    async fn commit(&self) -> Result<(), ConnectorError> {
        if self.pending.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let rows = std::mem::take(&mut *self.queue.lock().unwrap());
        self.remote.deliver(rows);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.queue.lock().unwrap().clear();
        Ok(())
    }
}
