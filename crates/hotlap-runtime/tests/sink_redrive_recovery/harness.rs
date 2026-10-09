//! A volatile-queue sink and a keyed, retraction-aware remote output store.

#[path = "../common/backend.rs"]
mod backend;
#[path = "../common/recovery/resumable.rs"]
mod resumable;

use std::collections::BTreeMap;
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

/// The `Int64Array` of a Z-set column.
pub fn int_column(zset: &ZSetBatch, index: usize) -> &Int64Array {
    zset.batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

/// The signed multiplicity column of a Z-set.
pub fn diff_column(zset: &ZSetBatch) -> &Int64Array {
    zset.diff.as_any().downcast_ref::<Int64Array>().unwrap()
}

/// Persistent keyed output shared by every sink instance.
///
/// Delivery applies the changelog as keyed upserts/retractions: a positive diff
/// upserts `key -> value`, a negative diff removes the key iff its current value
/// matches the retracted row. Applying the same changelog twice is idempotent.
#[derive(Clone, Default)]
pub struct RemoteStore(Arc<Mutex<BTreeMap<i64, i64>>>);

impl RemoteStore {
    fn apply(&self, key: i64, value: i64, diff: i64) {
        let mut map = self.0.lock().unwrap();
        if diff > 0 {
            map.insert(key, value);
        } else if diff < 0 && map.get(&key) == Some(&value) {
            map.remove(&key);
        }
    }

    pub fn snapshot(&self) -> BTreeMap<i64, i64> {
        self.0.lock().unwrap().clone()
    }
}

/// A sink whose writer only queues the changelog in memory; `commit` applies it
/// to the remote store. A pending `commit` blocks after signalling `entered`.
pub struct VolatileSink {
    remote: RemoteStore,
    queue: Mutex<Vec<(i64, i64, i64)>>,
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

#[async_trait::async_trait]
impl Sink for VolatileSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let zset = item?;
            let keys = int_column(&zset, 0);
            let values = int_column(&zset, 1);
            let diffs = diff_column(&zset);
            let mut queue = self.queue.lock().unwrap();
            for index in 0..zset.len() {
                queue.push((keys.value(index), values.value(index), diffs.value(index)));
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
        for (key, value, diff) in std::mem::take(&mut *self.queue.lock().unwrap()) {
            self.remote.apply(key, value, diff);
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.queue.lock().unwrap().clear();
        Ok(())
    }
}
