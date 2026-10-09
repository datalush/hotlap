//! Sink fixture for the checkpoint-interruption tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use tokio::sync::oneshot;

#[path = "../common/shutdown.rs"]
mod common;
pub use common::*;

/// A checkpoint config backed by an in-memory store, with a long interval so no
/// periodic checkpoint fires during a test.
pub fn checkpoint() -> CheckpointConfig {
    checkpoint_with(SharedBackend::default(), Duration::from_secs(3600))
}

/// A checkpoint config over `backend` firing every `interval`.
///
/// The backend is cloneable, so a test can keep a handle and inspect the durable
/// evidence the engine retained.
pub fn checkpoint_with(backend: SharedBackend, interval: Duration) -> CheckpointConfig {
    CheckpointConfig {
        interval,
        backend: Box::new(backend),
        retain: 3,
    }
}

/// A sink whose `commit` parks until the test releases it, signalling entry.
pub struct GatedCommitSink {
    entered: Signal,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl GatedCommitSink {
    /// The sink and the sender that releases its parked `commit`.
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
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
        }
        Ok(())
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
