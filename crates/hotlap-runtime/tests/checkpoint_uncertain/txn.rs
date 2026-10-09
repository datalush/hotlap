//! A transactional sink whose transaction outcome lives in a shared store.
//!
//! `write` consumes the real weighted changelog of one view. `prepare` stages
//! the queued payload as the next transaction; `commit` atomically installs the
//! transaction in the shared store, so a repeated or re-driven commit is a
//! no-op; `abort` only clears an uncommitted transaction. Every instance shares
//! the store, so a new sink observes the staged or committed state.

use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::ZSetBatch;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};

use crate::store::{Change, Store};

/// A transactional sink staging through a shared store.
pub struct TxnSink {
    view: &'static str,
    store: Arc<Store>,
    redriable: bool,
    queue: Mutex<Vec<Change>>,
}

impl TxnSink {
    /// A re-drivable sink over `store`.
    pub fn new(view: &'static str, store: Arc<Store>) -> Self {
        Self {
            view,
            store,
            redriable: true,
            queue: Mutex::new(Vec::new()),
        }
    }

    /// A sink whose `commit` is not declared re-drivable.
    pub fn non_redrivable(mut self) -> Self {
        self.redriable = false;
        self
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
        let payload = std::mem::take(&mut *self.queue.lock().unwrap());
        self.store.stage(self.view, payload);
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.store.commit(self.view).map(|_| ())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.store.abort(self.view)
    }
}

/// Append a batch's weighted rows to `queue`.
fn collect(zset: &ZSetBatch, queue: &Mutex<Vec<Change>>) {
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
