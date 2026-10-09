//! Sink fixture for the backpressured-shutdown test.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::Source;
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::sink::CHANNEL_CAPACITY;

#[path = "../common/shutdown.rs"]
mod common;
pub use common::*;

/// A source of `CHANNEL_CAPACITY + 2` batches that fires `last` as the engine
/// pulls the final one. It can only reach that batch after filling the channel,
/// so the signal pins the next pump send as blocked.
pub fn fill(last: Signal) -> Arc<dyn Source> {
    let values: Vec<i64> = (0..CHANNEL_CAPACITY as i64 + 2).collect();
    keys_with(&values, Some(last))
}

/// A sink that parks forever on its first write, so the channel fills.
pub struct StallingSink {
    entered: Signal,
    pub writes: AtomicUsize,
}

impl StallingSink {
    pub fn new(entered: Signal) -> Arc<Self> {
        Arc::new(Self {
            entered,
            writes: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Sink for StallingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.entered.fire();
            futures::future::pending::<()>().await;
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
