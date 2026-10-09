//! Fixtures for final EOF commit timeout tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use hotlap::{InputId, Plan};
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::Source;
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use tokio::sync::Notify;

#[path = "../common/shutdown.rs"]
mod common;
pub use common::{Signal, keys_with, start};

pub fn checkpoint() -> CheckpointConfig {
    let _start = common::start;
    CheckpointConfig {
        interval: std::time::Duration::from_secs(3600),
        backend: Box::new(common::SharedBackend::default()),
        retain: 3,
    }
}

pub fn start_pair(
    source: Arc<dyn Source>,
    first: Arc<dyn Sink>,
    second: Arc<dyn Sink>,
) -> EngineHandle {
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap();
    let plan = || Plan::Source(InputId(0));
    EngineHandle::start(Pipeline {
        sources,
        views: vec![("first".into(), plan()), ("second".into(), plan())],
        sinks: vec![
            SinkSpec {
                view: "first".into(),
                sink: first,
            },
            SinkSpec {
                view: "second".into(),
                sink: second,
            },
        ],
        checkpoint: None,
        retention: None,
    })
    .unwrap()
}

pub struct StallingCommitSink {
    written: Signal,
    entered: Signal,
    release: Arc<Notify>,
    resumed: Signal,
    dropped: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl StallingCommitSink {
    pub fn new(
        written: Signal,
        entered: Signal,
        release: Arc<Notify>,
        resumed: Signal,
        dropped: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            written,
            entered,
            release,
            resumed,
            dropped,
            aborted,
        })
    }
}

struct CommitDrop(Arc<AtomicBool>);

impl Drop for CommitDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Sink for StallingCommitSink {
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
        let _drop = CommitDrop(Arc::clone(&self.dropped));
        self.entered.fire();
        self.release.notified().await;
        self.resumed.fire();
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ObservedSink {
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl ObservedSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub fn aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Sink for ObservedSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
        }
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
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}
