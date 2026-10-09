//! Sink fixtures for the barrier-stall tests.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};
use tokio::sync::oneshot;

use super::Signal;

async fn drain_batches(mut changes: ChangeStream) -> Result<(), ConnectorError> {
    while let Some(item) = changes.next().await {
        item?;
    }
    Ok(())
}

/// A sink that parks forever on its first write.
pub struct ParkedWriteSink {
    entered: Signal,
}

impl ParkedWriteSink {
    pub fn new(entered: Signal) -> Arc<Self> {
        Arc::new(Self { entered })
    }
}

#[async_trait::async_trait]
impl Sink for ParkedWriteSink {
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError> {
        drain_batches(changes).await?;
        self.entered.fire();
        futures::future::pending().await
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

/// A transactional sink whose `prepare` parks until released, signalling entry.
pub struct GatedPrepareSink {
    entered: Signal,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl GatedPrepareSink {
    pub fn new(entered: Signal) -> (Arc<Self>, oneshot::Sender<()>) {
        let (release, held) = oneshot::channel();
        let sink = Arc::new(Self {
            entered,
            release: Mutex::new(Some(held)),
        });
        (sink, release)
    }
}

#[async_trait::async_trait]
impl Sink for GatedPrepareSink {
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError> {
        drain_batches(changes).await
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.entered.fire();
        let held = self.release.lock().unwrap().take();
        if let Some(held) = held {
            let _ = held.await;
        }
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// An at-least-once sink whose `commit` parks until released, signalling entry.
pub struct GatedCommitSink {
    entered: Signal,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl GatedCommitSink {
    pub fn new(entered: Signal) -> (Arc<Self>, oneshot::Sender<()>) {
        let (release, held) = oneshot::channel();
        let sink = Arc::new(Self {
            entered,
            release: Mutex::new(Some(held)),
        });
        (sink, release)
    }
}

#[async_trait::async_trait]
impl Sink for GatedCommitSink {
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError> {
        drain_batches(changes).await
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.entered.fire();
        let held = self.release.lock().unwrap().take();
        if let Some(held) = held {
            let _ = held.await;
        }
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A transactional sink with a chosen re-drive capability, for restart policy.
pub struct CapabilitySink {
    redriable: bool,
}

impl CapabilitySink {
    /// A sink declaring its commit re-drivable, so recovery may promote.
    pub fn redriable() -> Arc<Self> {
        Arc::new(Self { redriable: true })
    }

    /// A sink that is neither re-drivable nor replay-safe, so recovery rejects.
    pub fn staged() -> Arc<Self> {
        Arc::new(Self { redriable: false })
    }
}

#[async_trait::async_trait]
impl Sink for CapabilitySink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
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
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
