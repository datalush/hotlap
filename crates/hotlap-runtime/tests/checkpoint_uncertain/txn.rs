//! A staged transactional sink whose state lives in a durable store.
//!
//! `write` consumes the real weighted changelog of one view. `prepare` persists
//! the queued payload and the active transaction id in a store shared by every
//! instance; `commit` applies the weighted changes to the remote bag and records
//! the transaction committed, so a repeated or re-driven commit is a no-op.
//! `abort` only clears an uncommitted transaction. A new instance sharing the
//! store and bag observes the prepared or committed state.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::ZSetBatch;
use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};

use crate::codec::{decode, encode};
use crate::harness::RemoteBag;
use crate::support::SharedBackend;

/// A transactional sink staging through a durable store.
pub struct TxnSink {
    view: &'static str,
    store: SharedBackend,
    remote: RemoteBag,
    redriable: bool,
    fail_commit: AtomicU32,
    queue: Mutex<Vec<(Vec<i64>, i64)>>,
}

impl TxnSink {
    /// A re-drivable sink over `store` and `remote`.
    pub fn new(view: &'static str, store: SharedBackend, remote: RemoteBag) -> Self {
        Self {
            view,
            store,
            remote,
            redriable: true,
            fail_commit: AtomicU32::new(0),
            queue: Mutex::new(Vec::new()),
        }
    }

    /// A sink whose `commit` is not declared re-drivable.
    pub fn non_redrivable(mut self) -> Self {
        self.redriable = false;
        self
    }

    /// Make the next `attempts` commits fail before applying the payload.
    pub fn fail_next(&self, attempts: u32) {
        self.fail_commit.store(attempts, Ordering::SeqCst);
    }

    fn active(&self) -> Option<u64> {
        let key = format!("tx/{}/active", self.view);
        self.store
            .get(key.as_bytes())
            .unwrap()
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn allocate(&self) -> u64 {
        let key = format!("tx/{}/next", self.view);
        let next = self
            .store
            .get(key.as_bytes())
            .unwrap()
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .unwrap_or(0);
        self.store
            .clone()
            .put(key.as_bytes(), (next + 1).to_le_bytes().to_vec())
            .unwrap();
        next
    }

    fn key(&self, id: u64, part: &str) -> Vec<u8> {
        format!("tx/{}/{id}/{part}", self.view).into_bytes()
    }
}

#[async_trait::async_trait]
impl Sink for TxnSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            collect(&item?, &self.queue);
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        let id = self.allocate();
        let payload = encode(&std::mem::take(&mut *self.queue.lock().unwrap()));
        let mut store = self.store.clone();
        store.put(&self.key(id, "prepared"), payload).unwrap();
        let active = format!("tx/{}/active", self.view);
        store
            .put(active.as_bytes(), id.to_le_bytes().to_vec())
            .unwrap();
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        let Some(id) = self.active() else {
            return Ok(());
        };
        let mut store = self.store.clone();
        if store.get(&self.key(id, "committed")).unwrap().is_some() {
            return Ok(());
        }
        if self.fail_commit.load(Ordering::SeqCst) > 0 {
            self.fail_commit.fetch_sub(1, Ordering::SeqCst);
            return Err(ConnectorError::Unsupported(format!(
                "commit rejected for {}",
                self.view
            )));
        }
        let payload = store.get(&self.key(id, "prepared")).unwrap().unwrap();
        self.remote.apply(&decode(&payload));
        store
            .put(&self.key(id, "committed"), b"1".to_vec())
            .unwrap();
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        let Some(id) = self.active() else {
            return Ok(());
        };
        if self
            .store
            .get(&self.key(id, "committed"))
            .unwrap()
            .is_some()
        {
            return Err(ConnectorError::Unsupported(
                "cannot abort a committed transaction".into(),
            ));
        }
        let mut store = self.store.clone();
        store.delete(&self.key(id, "prepared")).unwrap();
        let active = format!("tx/{}/active", self.view);
        store.delete(active.as_bytes()).unwrap();
        Ok(())
    }
}

/// Append a batch's weighted rows to `queue`.
fn collect(zset: &ZSetBatch, queue: &Mutex<Vec<(Vec<i64>, i64)>>) {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut queue = queue.lock().unwrap();
    for index in 0..zset.len() {
        let row: Vec<i64> = columns.iter().map(|column| column.value(index)).collect();
        queue.push((row, diffs.value(index)));
    }
}
